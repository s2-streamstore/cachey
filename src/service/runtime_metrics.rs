use prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType};
use tokio::runtime::{Handle, RuntimeMetrics as TokioMetrics};

#[derive(Clone)]
pub(super) struct RuntimeMetrics {
    runtimes: [(&'static str, TokioMetrics); 2],
}

impl RuntimeMetrics {
    pub(super) fn capture(spawner: &foyer::Spawner) -> Self {
        let foyer = match spawner {
            foyer::Spawner::Runtime(runtime) => runtime.metrics(),
            foyer::Spawner::Handle(handle) => handle.metrics(),
        };
        Self {
            runtimes: [("server", Handle::current().metrics()), ("foyer", foyer)],
        }
    }

    pub(super) fn collect(&self) -> Vec<MetricFamily> {
        [
            (
                "cachey_tokio_workers",
                "Number of runtime worker threads",
                MetricType::GAUGE,
                TokioMetrics::num_workers as fn(&TokioMetrics) -> usize,
            ),
            (
                "cachey_tokio_alive_tasks",
                "Number of live asynchronous tasks in the runtime",
                MetricType::GAUGE,
                TokioMetrics::num_alive_tasks,
            ),
            (
                "cachey_tokio_global_queue_depth",
                "Number of tasks in the runtime global scheduling queue",
                MetricType::GAUGE,
                TokioMetrics::global_queue_depth,
            ),
        ]
        .into_iter()
        .map(|(name, help, kind, observe)| {
            self.family(name, help, kind, |metrics| observe(metrics) as f64)
        })
        .chain([
            self.family(
                "cachey_tokio_worker_busy_seconds_total",
                "Total time runtime worker threads have spent executing asynchronous tasks",
                MetricType::COUNTER,
                |metrics| {
                    (0..metrics.num_workers())
                        .map(|worker| metrics.worker_total_busy_duration(worker).as_secs_f64())
                        .sum()
                },
            ),
            self.family(
                "cachey_tokio_worker_parks_total",
                "Total number of times runtime worker threads have parked",
                MetricType::COUNTER,
                |metrics| {
                    (0..metrics.num_workers())
                        .map(|worker| metrics.worker_park_count(worker) as f64)
                        .sum()
                },
            ),
        ])
        .collect()
    }

    fn family(
        &self,
        name: &str,
        help: &str,
        kind: MetricType,
        observe: impl Fn(&TokioMetrics) -> f64,
    ) -> MetricFamily {
        let mut family = MetricFamily::new();
        family.set_name(name.to_owned());
        family.set_help(help.to_owned());
        family.set_field_type(kind);
        family.set_metric(
            self.runtimes
                .iter()
                .map(|(runtime, metrics)| {
                    let mut label = LabelPair::new();
                    label.set_name("runtime".to_owned());
                    label.set_value((*runtime).to_owned());
                    let mut metric = Metric::new();
                    metric.set_label(vec![label]);
                    let value = observe(metrics);
                    if kind == MetricType::COUNTER {
                        let mut counter = Counter::new();
                        counter.set_value(value);
                        metric.set_counter(counter);
                    } else {
                        let mut gauge = Gauge::new();
                        gauge.set_value(value);
                        metric.set_gauge(gauge);
                    }
                    metric
                })
                .collect(),
        );
        family
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use prometheus::{Encoder, TextEncoder, proto::MetricType};
    use tokio::{runtime::Builder, sync::oneshot};

    use super::RuntimeMetrics;

    fn value(metrics: &RuntimeMetrics, name: &str, runtime: &str) -> f64 {
        let families = metrics.collect();
        let family = families
            .iter()
            .find(|family| family.name() == name)
            .unwrap();
        let metric = family
            .get_metric()
            .iter()
            .find(|metric| metric.get_label()[0].value() == runtime)
            .unwrap();
        match family.get_field_type() {
            MetricType::COUNTER => metric.get_counter().value(),
            MetricType::GAUGE => metric.get_gauge().value(),
            _ => panic!("unexpected runtime metric type"),
        }
    }

    #[test]
    fn metrics_follow_captured_runtimes_from_another_scraper_runtime() {
        let server = Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        let foyer = Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let spawner = foyer::Spawner::Handle(foyer.handle().clone());
        let captured = server.block_on(async { RuntimeMetrics::capture(&spawner) });
        let scraper = Builder::new_current_thread().build().unwrap();
        scraper.block_on(async {
            assert!(
                (value(&captured, "cachey_tokio_workers", "server") - 2.0).abs() < f64::EPSILON
            );
            assert!((value(&captured, "cachey_tokio_workers", "foyer") - 1.0).abs() < f64::EPSILON);
        });

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let task = foyer.spawn(async move {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!((value(&captured, "cachey_tokio_alive_tasks", "foyer") - 1.0).abs() < f64::EPSILON);
        assert!(value(&captured, "cachey_tokio_alive_tasks", "server").abs() < f64::EPSILON);
        let busy = value(&captured, "cachey_tokio_worker_busy_seconds_total", "foyer");
        release_tx.send(()).unwrap();
        foyer.block_on(task).unwrap();
        assert!(value(&captured, "cachey_tokio_worker_busy_seconds_total", "foyer") >= busy);
    }

    #[test]
    fn repeated_scrapes_and_clones_have_one_family_and_two_bounded_labels() {
        let server = Builder::new_current_thread().build().unwrap();
        let foyer: foyer::Spawner = Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap()
            .into();
        let captured = server.block_on(async { RuntimeMetrics::capture(&foyer) });
        let mut previous_names = None;
        for _ in 0..3 {
            let families = captured.clone().collect();
            assert_eq!(families.len(), 5);
            let names: Vec<_> = families
                .iter()
                .map(|family| family.name().to_owned())
                .collect();
            if let Some(previous) = &previous_names {
                assert_eq!(&names, previous);
            }
            previous_names = Some(names);
            for family in &families {
                assert_eq!(family.get_metric().len(), 2);
                let labels: Vec<_> = family
                    .get_metric()
                    .iter()
                    .map(|metric| {
                        assert_eq!(metric.get_label().len(), 1);
                        assert_eq!(metric.get_label()[0].name(), "runtime");
                        metric.get_label()[0].value()
                    })
                    .collect();
                assert_eq!(labels, ["server", "foyer"]);
            }
            let mut encoded = Vec::new();
            TextEncoder::new().encode(&families, &mut encoded).unwrap();
            let text = String::from_utf8(encoded).unwrap();
            assert!(text.contains("# TYPE cachey_tokio_worker_busy_seconds_total counter"));
            assert!(text.contains("cachey_tokio_workers{runtime=\"foyer\"} 1"));
        }
        // A metrics handle must not prevent the owning runtime from shutting down.
        drop(foyer);
        drop(captured);
    }
}
