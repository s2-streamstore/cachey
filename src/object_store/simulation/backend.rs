use std::{collections::HashMap, io, sync::Arc, time::Duration};

use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_runtime_api::client::{
    http::{HttpConnector, HttpConnectorFuture},
    orchestrator::{HttpRequest, HttpResponse},
};
use bytes::Bytes;
use http_body::Frame;
use http_body_util::StreamBody;
use parking_lot::Mutex;
use serde::Serialize;
use tokio::{
    sync::{Semaphore, oneshot},
    time::{Instant, sleep, sleep_until},
};

use super::model::{FaultKind, Scenario, draw, occurs};

pub const WARM_READ: u64 = 1 << 60;

#[derive(Debug, Clone, Serialize)]
pub struct Transport {
    pub read: u64,
    pub replica: usize,
    pub invocation: u32,
    pub attempt: u32,
    pub start_us: u64,
    pub service_start_us: Option<u64>,
    pub client_end_us: Option<u64>,
    pub server_end_us: Option<u64>,
    pub cancelled: bool,
    pub produced_bytes: u64,
    pub delivered_bytes: u64,
    pub fault: Option<FaultKind>,
    pub capacity_rejected: bool,
}

#[derive(Debug)]
pub struct State {
    pub epoch: Instant,
    pub transports: Vec<Transport>,
    invocations: HashMap<(u64, usize, String), (u32, u32)>,
    invocation_counts: HashMap<(u64, usize), u32>,
    /// When each cold object a replica is fetching turns warm.
    warm_at: HashMap<(u64, usize), Instant>,
    outstanding: Vec<usize>,
    pub client_active: usize,
    pub peak_client: usize,
    pub server_active: usize,
    pub peak_server: usize,
    pub queued: usize,
    pub peak_queued: usize,
    pub workers: usize,
}

impl State {
    pub fn new(replicas: usize) -> Self {
        Self {
            epoch: Instant::now(),
            transports: vec![],
            invocations: HashMap::new(),
            invocation_counts: HashMap::new(),
            warm_at: HashMap::new(),
            outstanding: vec![0; replicas],
            client_active: 0,
            peak_client: 0,
            server_active: 0,
            peak_server: 0,
            queued: 0,
            peak_queued: 0,
            workers: 0,
        }
    }

    fn now_us(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }
}

#[derive(Debug, Clone)]
pub struct Backend {
    pub scenario: Arc<Scenario>,
    pub seed: u64,
    pub state: Arc<Mutex<State>>,
    slots: Arc<Vec<Arc<Semaphore>>>,
}

impl Backend {
    pub fn new(scenario: Arc<Scenario>, seed: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new(scenario.replicas.len()))),
            seed,
            slots: Arc::new(
                scenario
                    .replicas
                    .iter()
                    .map(|replica| Arc::new(Semaphore::new(replica.slots as usize)))
                    .collect(),
            ),
            scenario,
        }
    }

    pub async fn drain(&self) {
        // Virtual-time polling also verifies that cancelled workers eventually release capacity.
        tokio::time::timeout(Duration::from_secs(120), async {
            while self.state.lock().workers > 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("simulation server did not drain");
        let state = self.state.lock();
        assert_eq!(
            (state.client_active, state.server_active, state.queued),
            (0, 0, 0)
        );
    }

    pub fn reset(&self) {
        assert_eq!(self.state.lock().workers, 0);
        *self.state.lock() = State::new(self.scenario.replicas.len());
    }

    fn fault(
        &self,
        read: u64,
        replica: usize,
        invocation: u32,
        attempt: u32,
        now_us: u64,
    ) -> Option<(FaultKind, u64)> {
        if read >= WARM_READ {
            return None;
        }
        self.scenario
            .faults
            .iter()
            .enumerate()
            .find_map(|(index, fault)| {
                let time_ms = if fault.clock_at_arrival {
                    self.scenario.arrival_ms(read)
                } else {
                    now_us / 1000
                };
                let (destination, operation, wire_attempt) = if fault.correlated {
                    (0, 0, 0)
                } else {
                    (replica, invocation, attempt)
                };
                (fault.replicas.contains(&replica)
                    && (fault.start_ms..fault.end_ms).contains(&time_ms)
                    && (!fault.first_sdk_attempt_only || attempt == 1)
                    && occurs(
                        draw(
                            self.seed,
                            read,
                            destination,
                            operation,
                            wire_attempt,
                            index as u64,
                        ),
                        fault.probability_ppm,
                    ))
                .then_some((fault.kind, fault.duration_ms))
            })
    }
}

struct ClientGuard {
    state: Arc<Mutex<State>>,
    index: usize,
    cancel: Option<oneshot::Sender<()>>,
    complete: bool,
}

impl ClientGuard {
    fn finish(&mut self) {
        self.complete = true;
    }
    fn delivered(&self, bytes: usize) {
        self.state.lock().transports[self.index].delivered_bytes += bytes as u64;
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let now = state.now_us();
        state.client_active -= 1;
        state.transports[self.index].client_end_us = Some(now);
        state.transports[self.index].cancelled = !self.complete;
        if !self.complete
            && let Some(cancel) = self.cancel.take()
        {
            let _ = cancel.send(());
        }
    }
}

struct ServerGuard {
    state: Arc<Mutex<State>>,
    index: usize,
    replica: usize,
    rejected: bool,
    started: bool,
}

impl ServerGuard {
    fn start(&mut self) {
        let mut state = self.state.lock();
        let now = state.now_us();
        state.queued -= 1;
        state.server_active += 1;
        state.peak_server = state.peak_server.max(state.server_active);
        state.transports[self.index].service_start_us = Some(now);
        self.started = true;
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let now = state.now_us();
        state.transports[self.index].server_end_us = Some(now);
        state.workers -= 1;
        if !self.rejected {
            state.outstanding[self.replica] -= 1;
            if self.started {
                state.server_active -= 1;
            } else {
                state.queued -= 1;
            }
        }
    }
}

/// The response a server commits to when it starts serving a request. The server and the reading
/// client each follow it on their own timers, so the downloader wakes for every response event in
/// its own task. Were events sent from the server task instead, the order in which tasks woken on
/// the same tick run would decide whether a response landing exactly on a learned tail beats the
/// hedge timer.
#[derive(Debug, Clone, Copy)]
struct Response {
    headers_at: Instant,
    error: Option<(u16, &'static str)>,
    body_ms: u64,
    body_bytes: usize,
    chunks: u32,
    fails_mid_body: bool,
}

impl Response {
    fn chunk_at(&self, chunk: u32) -> Instant {
        self.headers_at
            + Duration::from_millis(self.body_ms * u64::from(chunk + 1) / u64::from(self.chunks))
    }

    fn chunk_bytes(&self, chunk: u32) -> usize {
        let offset = |chunk: u32| self.body_bytes * chunk as usize / self.chunks as usize;
        offset(chunk + 1) - offset(chunk)
    }

    fn fails_at(&self, chunk: u32) -> bool {
        self.fails_mid_body && chunk == self.chunks / 2
    }
}

impl HttpConnector for Backend {
    #[allow(clippy::too_many_lines)]
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let uri = request.uri();
        let replica = uri
            .split("/replica-")
            .nth(1)
            .and_then(|value| value.split('/').next())
            .and_then(|value| value.parse::<usize>().ok())
            .expect("simulation replica URI");
        let read = uri
            .split("/read-")
            .nth(1)
            .and_then(|value| value.split('?').next())
            .and_then(|value| value.parse::<u64>().ok())
            .expect("simulation read URI");
        let invocation_id = request
            .headers()
            .get("amz-sdk-invocation-id")
            .expect("SDK invocation ID")
            .to_owned();
        let mut state = self.state.lock();
        let key = (read, replica, invocation_id);
        if !state.invocations.contains_key(&key) {
            let ordinal = state.invocation_counts.entry((read, replica)).or_default();
            *ordinal += 1;
            let ordinal = *ordinal;
            state.invocations.insert(key.clone(), (ordinal, 0));
        }
        let (invocation, attempt) = state.invocations.get_mut(&key).unwrap();
        *attempt += 1;
        let (invocation, attempt) = (*invocation, *attempt);
        let now_us = state.now_us();
        let capacity = &self.scenario.replicas[replica];
        let rejected =
            state.outstanding[replica] >= capacity.slots as usize + capacity.queue as usize;
        let fault = self.fault(read, replica, invocation, attempt, now_us);
        let index = state.transports.len();
        state.transports.push(Transport {
            read,
            replica,
            invocation,
            attempt,
            start_us: now_us,
            service_start_us: None,
            client_end_us: None,
            server_end_us: None,
            cancelled: false,
            produced_bytes: 0,
            delivered_bytes: 0,
            fault: fault.map(|value| value.0),
            capacity_rejected: rejected,
        });
        state.client_active += 1;
        state.peak_client = state.peak_client.max(state.client_active);
        state.workers += 1;
        if !rejected {
            state.outstanding[replica] += 1;
            state.queued += 1;
            let queued = state
                .outstanding
                .iter()
                .zip(&self.scenario.replicas)
                .map(|(outstanding, replica)| outstanding.saturating_sub(replica.slots as usize))
                .sum();
            state.peak_queued = state.peak_queued.max(queued);
        }
        drop(state);
        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();
        let mut client = ClientGuard {
            state: self.state.clone(),
            index,
            cancel: Some(cancel_tx),
            complete: false,
        };
        let body_bytes = self.scenario.read_body_bytes(self.seed, read);
        let backend = self.clone();
        tokio::spawn(async move {
            let mut server = ServerGuard {
                state: backend.state.clone(),
                index,
                replica,
                rejected,
                started: false,
            };
            let work = async {
                if rejected {
                    let response = Response {
                        headers_at: Instant::now() + Duration::from_millis(1),
                        error: Some((503, "SlowDown")),
                        body_ms: 0,
                        body_bytes,
                        chunks: backend.scenario.chunks,
                        fails_mid_body: false,
                    };
                    let _ = response_tx.send(response);
                    sleep_until(response.headers_at).await;
                    return;
                }
                let _permit = backend.slots[replica]
                    .acquire()
                    .await
                    .expect("server semaphore open");
                server.start();
                backend.serve(index, fault, body_bytes, response_tx).await;
            };
            tokio::pin!(work);
            tokio::select! {
                biased;
                () = &mut work => {},
                cancellation = &mut cancel_rx => {
                    if cancellation.is_ok() {
                        tokio::select! {
                            biased;
                            () = &mut work => {},
                            () = sleep(Duration::from_millis(backend.scenario.cancellation_lag_ms)) => {},
                        }
                    } else { work.await; }
                }
            }
        });
        HttpConnectorFuture::new(async move {
            let response = response_rx
                .await
                .expect("server responds while client is alive");
            sleep_until(response.headers_at).await;
            let (status, body) = if let Some((status, code)) = response.error {
                client.finish();
                (
                    status,
                    SdkBody::from(format!("<Error><Code>{code}</Code></Error>")),
                )
            } else {
                let stream = async_stream::stream! {
                    for chunk in 0..response.chunks {
                        sleep_until(response.chunk_at(chunk)).await;
                        if response.fails_at(chunk) {
                            client.finish();
                            yield Err(io::Error::other("simulated body failure"));
                            break;
                        }
                        let bytes = response.chunk_bytes(chunk);
                        client.delivered(bytes);
                        yield Ok(Frame::data(Bytes::from(vec![0x5a; bytes])));
                    }
                    client.finish();
                };
                (206, SdkBody::from_body_1_x(StreamBody::new(stream)))
            };
            let mut http_response = HttpResponse::new(status.try_into().unwrap(), body);
            if response.error.is_none() {
                http_response.headers_mut().insert(
                    "content-range",
                    format!("bytes 0-{}/{body_bytes}", body_bytes - 1),
                );
                http_response
                    .headers_mut()
                    .insert("content-length", body_bytes.to_string());
            }
            Ok(http_response)
        })
    }
}

impl Backend {
    async fn serve(
        &self,
        index: usize,
        fault: Option<(FaultKind, u64)>,
        body_bytes: usize,
        response_tx: oneshot::Sender<Response>,
    ) {
        let transport = self.state.lock().transports[index].clone();
        let replica = &self.scenario.replicas[transport.replica];
        let jitter = |millis: u64, domain| {
            let percent = i128::from(self.scenario.jitter_percent);
            let adjustment = i128::from(
                draw(
                    self.seed,
                    transport.read,
                    transport.replica,
                    transport.invocation,
                    transport.attempt,
                    domain,
                ) % 201,
            ) - 100;
            (i128::from(millis) * (10_000 + percent * adjustment) / 10_000) as u64
        };
        let mut headers_ms = jitter(replica.headers_ms(body_bytes), 1000);
        let mut body_ms = jitter(replica.body_ms(body_bytes), 1001);
        let mut header_fault = false;
        if let Some((kind, duration)) = fault {
            match kind {
                FaultKind::HeaderStall
                | FaultKind::ServiceError
                | FaultKind::Missing
                | FaultKind::Overload => {
                    headers_ms = duration;
                    header_fault = true;
                }
                FaultKind::BodyStall | FaultKind::BodyError | FaultKind::Slow => body_ms = duration,
            }
        }
        let mut headers_at = Instant::now() + Duration::from_millis(headers_ms);
        let age_ms = self.scenario.read_age_ms(self.seed, transport.read);
        if !header_fault && let Some(cold_ms) = replica.cold_headers_ms(age_ms, body_bytes) {
            let warm_at = *self
                .state
                .lock()
                .warm_at
                .entry((transport.read, transport.replica))
                .or_insert_with(|| headers_at + Duration::from_millis(jitter(cold_ms, 1002)));
            headers_at = headers_at.max(warm_at);
        }
        let response = Response {
            headers_at,
            error: match fault.map(|value| value.0) {
                Some(FaultKind::ServiceError) => Some((500, "InternalError")),
                Some(FaultKind::Missing) => Some((404, "NoSuchKey")),
                Some(FaultKind::Overload) => Some((503, "SlowDown")),
                _ => None,
            },
            body_ms,
            body_bytes,
            chunks: self.scenario.chunks,
            fails_mid_body: matches!(fault, Some((FaultKind::BodyError, _))),
        };
        let _ = response_tx.send(response);
        sleep_until(response.headers_at).await;
        if response.error.is_some() {
            return;
        }
        for chunk in 0..response.chunks {
            sleep_until(response.chunk_at(chunk)).await;
            if response.fails_at(chunk) {
                return;
            }
            self.state.lock().transports[index].produced_bytes +=
                response.chunk_bytes(chunk) as u64;
        }
    }
}
