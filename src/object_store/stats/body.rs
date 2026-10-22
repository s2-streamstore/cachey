use std::{
    collections::BTreeMap,
    hash::{BuildHasher, RandomState},
    time::Duration,
};

use tokio::{runtime::Handle, sync::oneshot, time::Instant};

use super::{LATENCY_DECAY, Quantiles, Sample, decay, histogram_micros};

const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const TIMER_MICROS: f64 = TIMER_RESOLUTION.as_micros() as f64;
const FIT_PRECISION_MICROS: f64 = 0.25;
const INLINE_FIT_SAMPLES: usize = 64;
const SAMPLES_PER_BAND: usize = 1028;

#[derive(Debug, Clone, Copy)]
struct Line {
    fixed: f64,
    per_byte: f64,
}

impl Line {
    fn micros(self, bytes: u64) -> f64 {
        (self.fixed + self.per_byte * bytes as f64).max(TIMER_MICROS)
    }
}

#[derive(Debug, Clone, Copy)]
struct Estimate {
    median: Line,
    p99: Line,
    largest_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BodyModel(Option<Estimate>);

impl BodyModel {
    pub fn predict(&self, bytes: u64) -> Option<Quantiles> {
        let estimate = self.0?;
        if bytes > estimate.largest_bytes
            && estimate.median.per_byte * estimate.largest_bytes as f64 <= TIMER_MICROS
        {
            return None;
        }
        let duration = |micros: f64| {
            Duration::try_from_secs_f64(micros.round() / 1_000_000.0).unwrap_or(Duration::MAX)
        };
        let median = duration(estimate.median.micros(bytes));
        Some(Quantiles {
            median,
            p99: duration(estimate.p99.micros(bytes)).max(median),
        })
    }

    pub fn largest_bytes(&self) -> Option<u64> {
        self.0.map(|estimate| estimate.largest_bytes)
    }

    pub fn seconds_per_byte(&self) -> Option<f64> {
        self.0.map(|estimate| estimate.p99.per_byte / 1_000_000.0)
    }
}

#[derive(Debug, Clone, Copy)]
struct Measurement {
    bytes: u64,
    micros: f64,
    at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct Observation {
    bytes: u64,
    micros: f64,
    weight: f64,
}

fn quantile(mut values: &mut [(f64, f64)], q: f64) -> f64 {
    let mut target = values.iter().map(|(_, weight)| weight).sum::<f64>() * (q - 1e-9);
    while !values.is_empty() {
        let middle = (values.len() - 1) / 2;
        let (below, pivot, above) =
            values.select_nth_unstable_by(middle, |left, right| left.0.total_cmp(&right.0));
        let weight_below: f64 = below.iter().map(|(_, weight)| weight).sum();
        if target <= weight_below && !below.is_empty() {
            values = below;
        } else if target <= weight_below + pivot.1 || above.is_empty() {
            return pivot.0;
        } else {
            target -= weight_below + pivot.1;
            values = above;
        }
    }
    0.0
}

struct Fit {
    line: Line,
    loss: f64,
    gradient: f64,
}

impl Fit {
    fn at(
        observations: &[Observation],
        q: f64,
        per_byte: f64,
        values: &mut Vec<(f64, f64)>,
    ) -> Self {
        values.clear();
        values.extend(observations.iter().map(|sample| {
            (
                sample.micros.max(TIMER_MICROS) - per_byte * sample.bytes as f64,
                sample.weight,
            )
        }));
        let fixed = quantile(values, q).max(0.0);
        let line = Line { fixed, per_byte };
        let mut loss = 0.0;
        let (mut below_weight, mut above_weight, mut tied_weight) = (0.0, 0.0, 0.0);
        let (mut below_bytes, mut above_bytes, mut tied_bytes) = (0.0, 0.0, 0.0);
        for sample in observations {
            let bytes = sample.bytes as f64;
            let residual = sample.micros.max(TIMER_MICROS) - per_byte * bytes;
            loss +=
                sample.weight * (residual - fixed) * if residual >= fixed { q } else { q - 1.0 };
            if residual < fixed {
                below_weight += sample.weight;
                below_bytes += sample.weight * bytes;
            } else if residual > fixed {
                above_weight += sample.weight;
                above_bytes += sample.weight * bytes;
            } else {
                tied_weight += sample.weight;
                tied_bytes += sample.weight * bytes;
            }
        }
        // Balance the intercept's subgradient at tied samples, respecting the zero boundary.
        let tied = if tied_weight > 0.0 {
            ((q * above_weight - (1.0 - q) * below_weight) / tied_weight).clamp(-q, 1.0 - q)
        } else {
            0.0
        };
        let gradient = (1.0 - q) * below_bytes - q * above_bytes + tied * tied_bytes;
        Fit {
            line,
            loss,
            gradient,
        }
    }
}

/// For each slope the optimal intercept is a weighted quantile of residual times.
/// Nonnegative fixed cost bounds the slope by the zero-intercept fit.
/// Supporting lines bracket the minimum; each step discards at least a quarter of the bracket.
fn fit(observations: &[Observation], q: f64, values: &mut Vec<(f64, f64)>) -> Line {
    values.clear();
    values.extend(observations.iter().map(|sample| {
        (
            sample.micros.max(TIMER_MICROS) / sample.bytes as f64,
            sample.weight * sample.bytes as f64,
        )
    }));
    let upper = quantile(values, q);
    if observations
        .iter()
        .all(|sample| sample.bytes == observations[0].bytes)
    {
        return Line {
            fixed: TIMER_MICROS,
            per_byte: (upper - TIMER_MICROS / observations[0].bytes as f64).max(0.0),
        };
    }
    let largest_bytes = observations
        .iter()
        .map(|sample| sample.bytes)
        .max()
        .unwrap_or(1) as f64;
    // Below this loss, every retained sample fits within the histogram's time precision.
    let resolved_loss = FIT_PRECISION_MICROS
        * q.min(1.0 - q)
        * observations
            .iter()
            .map(|sample| sample.weight)
            .fold(f64::INFINITY, f64::min);
    let mut evaluate = |per_byte| Fit::at(observations, q, per_byte, values);
    let mut lower = evaluate(0.0);
    if lower.gradient >= 0.0 {
        return lower.line;
    }
    let mut upper = evaluate(upper);
    if upper.gradient <= 0.0 {
        return upper.line;
    }
    loop {
        let width = upper.line.per_byte - lower.line.per_byte;
        let fraction = (((lower.loss - upper.loss) / width + upper.gradient)
            / (upper.gradient - lower.gradient))
            .clamp(0.25, 0.75);
        let per_byte = lower.line.per_byte + fraction * width;
        let fitted = evaluate(per_byte);
        if fitted.gradient == 0.0
            || fitted.loss <= resolved_loss
            || width * largest_bytes <= FIT_PRECISION_MICROS
            || per_byte <= lower.line.per_byte
            || per_byte >= upper.line.per_byte
        {
            return fitted.line;
        }
        if fitted.gradient > 0.0 {
            upper = fitted;
        } else {
            lower = fitted;
        }
    }
}

fn model(observations: &[Observation]) -> BodyModel {
    let mut values = Vec::with_capacity(observations.len());
    BodyModel(
        observations
            .iter()
            .map(|sample| sample.bytes)
            .max()
            .map(|largest_bytes| Estimate {
                median: fit(observations, 0.5, &mut values),
                p99: fit(observations, 0.99, &mut values),
                largest_bytes,
            }),
    )
}

struct BandSnapshot {
    samples: Vec<Measurement>,
    weight: f64,
    minimum_weight: f64,
}

fn summarize(bands: Vec<BandSnapshot>, now: Instant) -> BodyModel {
    let mut observations = Vec::new();
    for snapshot in bands {
        let weight = |at| {
            decay(LATENCY_DECAY, now.saturating_duration_since(at)).max(snapshot.minimum_weight)
        };
        let retained_weight: f64 = snapshot
            .samples
            .iter()
            .map(|sample| weight(sample.at))
            .sum();
        if retained_weight == 0.0 {
            continue;
        }
        // Restore each stratum's observation mass after reservoir sampling.
        let correction = snapshot.weight / retained_weight;
        observations.extend(snapshot.samples.iter().map(|sample| Observation {
            bytes: sample.bytes,
            micros: sample.micros,
            weight: weight(sample.at) * correction,
        }));
    }
    model(&observations)
}

#[derive(Debug)]
struct Band {
    samples: BTreeMap<(u64, u64), Measurement>,
    capacity: usize,
    count: u64,
    random: RandomState,
    started: Instant,
    discarded_priority: Option<u64>,
    weight: f64,
    updated_at: Instant,
}

impl Band {
    fn new(now: Instant) -> Self {
        let capacity = SAMPLES_PER_BAND;
        #[cfg(test)]
        let capacity = crate::object_store::simulation::histogram_capacity().unwrap_or(capacity);
        Self {
            samples: BTreeMap::new(),
            capacity,
            count: 0,
            random: RandomState::new(),
            started: now,
            discarded_priority: None,
            weight: 0.0,
            updated_at: now,
        }
    }

    fn update(&mut self, now: Instant, sample: &Sample) {
        let uniform = ((self.random.hash_one(self.count) >> 11) + 1) as f64 / (1_u64 << 53) as f64;
        self.retain(now, sample, uniform);
    }

    fn retain(&mut self, now: Instant, sample: &Sample, uniform: f64) {
        self.weight = self.weight_at(now) + 1.0;
        self.updated_at = now;
        self.count += 1;
        // Log priorities avoid rescaling. Positive float bits preserve numeric order;
        // the observation count distinguishes equal priorities.
        let priority = (LATENCY_DECAY * now.duration_since(self.started).as_secs_f64()
            - uniform.ln())
        .to_bits();
        let measurement = Measurement {
            bytes: sample.body_bytes,
            micros: histogram_micros(sample.body) as f64,
            at: now,
        };
        if self.samples.len() < self.capacity {
            self.samples.insert((priority, self.count), measurement);
        } else if let Some(lowest) = self.samples.first_entry() {
            let discarded = if priority > lowest.key().0 {
                let ((discarded, _), _) = lowest.remove_entry();
                self.samples.insert((priority, self.count), measurement);
                discarded
            } else {
                priority
            };
            self.discarded_priority = self.discarded_priority.max(Some(discarded));
        }
    }

    fn snapshot(&self, now: Instant) -> BandSnapshot {
        // Priority sampling assigns max(weight, highest discarded priority), correcting
        // for age-biased inclusion: https://arxiv.org/abs/cs/0509026.
        let minimum_weight = self.discarded_priority.map_or(0.0, |priority| {
            (f64::from_bits(priority)
                - LATENCY_DECAY * now.duration_since(self.started).as_secs_f64())
            .exp()
        });
        BandSnapshot {
            samples: self.samples.values().copied().collect(),
            weight: self.weight_at(now),
            minimum_weight,
        }
    }

    fn weight_at(&self, now: Instant) -> f64 {
        self.weight
            * decay(
                LATENCY_DECAY,
                now.saturating_duration_since(self.updated_at),
            )
    }
}

/// Size-stratified retention keeps rare transfers available to one continuous model.
#[derive(Debug, Default)]
pub(super) struct BodyHistogram {
    bands: BTreeMap<u64, Band>,
    model: BodyModel,
    updates: u64,
    summarized_updates: u64,
    pending: Option<oneshot::Receiver<BodyModel>>,
}

impl BodyHistogram {
    pub fn update(&mut self, now: Instant, sample: &Sample) {
        if sample.body_bytes == 0 {
            return;
        }
        let bytes = sample
            .body_bytes
            .checked_next_power_of_two()
            .unwrap_or(u64::MAX);
        self.bands
            .entry(bytes)
            .or_insert_with(|| Band::new(now))
            .update(now, sample);
        self.updates = self.updates.saturating_add(1);
    }

    #[cfg(test)]
    pub fn retention(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.bands
            .values()
            .map(|band| (band.count, band.samples.len()))
    }

    pub fn summary(&mut self, due: bool) -> BodyModel {
        if let Some(pending) = &mut self.pending {
            match pending.try_recv() {
                Ok(model) => self.model = model,
                Err(oneshot::error::TryRecvError::Empty) => return self.model,
                Err(oneshot::error::TryRecvError::Closed) => {
                    tracing::warn!(
                        "Body model refresh did not complete; retaining previous estimate"
                    );
                    self.summarized_updates = 0;
                }
            }
            self.pending = None;
        }
        if self.updates != self.summarized_updates
            && (due || self.updates >= self.summarized_updates.saturating_mul(2))
        {
            if self.updates > INLINE_FIT_SAMPLES as u64 && Handle::try_current().is_err() {
                return self.model;
            }
            self.summarized_updates = self.updates;
            let now = Instant::now();
            let bands: Vec<_> = self.bands.values().map(|band| band.snapshot(now)).collect();
            let samples = bands.iter().map(|snapshot| snapshot.samples.len()).sum();
            self.refresh(samples, move || summarize(bands, now));
        }
        self.model
    }

    fn refresh(&mut self, samples: usize, fit: impl FnOnce() -> BodyModel + Send + 'static) {
        #[cfg(test)]
        if crate::object_store::simulation::inline_body_fits() {
            self.model = fit();
            return;
        }
        if samples <= INLINE_FIT_SAMPLES {
            self.model = fit();
        } else if let Ok(runtime) = Handle::try_current() {
            let (sender, receiver) = oneshot::channel();
            self.pending = Some(receiver);
            runtime.spawn_blocking(move || {
                if !sender.is_closed() {
                    let _ = sender.send(fit());
                }
            });
        } else {
            self.summarized_updates = 0;
        }
    }
}
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytesize::MIB;
    use proptest::{collection, prop_assert, proptest};
    use tokio::time::{Instant, advance};

    use super::{
        Band, BodyHistogram, BodyModel, INLINE_FIT_SAMPLES, Line, Observation, SAMPLES_PER_BAND,
        TIMER_MICROS, TIMER_RESOLUTION, fit, summarize,
    };
    use crate::object_store::stats::Sample;

    async fn refreshed(histogram: &mut BodyHistogram, due: bool) -> BodyModel {
        histogram.summary(due);
        while histogram.pending.is_some() {
            tokio::task::yield_now().await;
            histogram.summary(false);
        }
        histogram.model
    }

    #[tokio::test(start_paused = true)]
    async fn sampled_and_full_history_apply_the_same_decay() {
        let mut sampled = Band::new(Instant::now());
        let mut full = Band::new(Instant::now());
        full.capacity = 20_000;
        for (count, millis) in [(10_000, 11), (4_000, 41)] {
            let sample = Sample::from_millis(10, MIB, millis);
            for index in 0..count {
                // Evenly spaced draws make the retained proportions reproducible.
                let uniform = (f64::from(index) + 0.5) / f64::from(count);
                for band in [&mut sampled, &mut full] {
                    band.retain(Instant::now(), &sample, uniform);
                }
            }
            advance(Duration::from_secs(15)).await;
        }
        assert_eq!(sampled.samples.len(), SAMPLES_PER_BAND);
        assert_eq!(full.samples.len(), 14_000);
        for band in [&sampled, &full] {
            let quantiles = summarize(vec![band.snapshot(Instant::now())], Instant::now())
                .predict(MIB)
                .unwrap();
            assert_eq!(quantiles.median, Duration::from_millis(11));
            assert_eq!(quantiles.p99, Duration::from_millis(41));
            assert_eq!(quantiles.hedge_delay(), Duration::from_micros(16_500));
        }
        advance(Duration::from_hours(168)).await;
        for band in [&mut sampled, &mut full] {
            band.retain(Instant::now(), &Sample::from_millis(10, MIB, 21), 0.5);
            let quantiles = summarize(vec![band.snapshot(Instant::now())], Instant::now())
                .predict(MIB)
                .unwrap();
            assert_eq!(quantiles.median, Duration::from_millis(21));
            assert_eq!(quantiles.p99, Duration::from_millis(21));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tiny_buffered_reads_do_not_erase_a_measured_transfer() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, 4 * MIB, 41));
        for _ in 0..5000 {
            histogram.update(Instant::now(), &Sample::from_millis(10, 1024, 0));
        }
        let model = refreshed(&mut histogram, true).await;
        assert_eq!(model.predict(1024).unwrap().p99, TIMER_RESOLUTION);
        assert_eq!(
            model.predict(4 * MIB).unwrap().median,
            Duration::from_millis(41)
        );
        assert!(model.predict(16 * MIB).unwrap().p99 <= Duration::from_millis(162));
    }

    #[tokio::test(start_paused = true)]
    async fn body_observations_decay_without_capping_the_tail() {
        let mut histogram = BodyHistogram::default();
        for _ in 0..50 {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        }
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 101));
        let model = histogram.summary(true);
        let quantiles = model.predict(MIB).unwrap();
        assert_eq!(quantiles.p99, Duration::from_millis(101));
        assert_eq!(quantiles.hedge_delay(), Duration::from_micros(16_500));
        advance(Duration::from_secs(300)).await;
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
        assert_eq!(
            histogram.summary(true).predict(MIB).unwrap().p99,
            Duration::from_millis(21)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_bodies_do_not_establish_throughput_for_larger_sizes() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, 3000, 0));
        let model = histogram.summary(false);
        assert_eq!(model.predict(3000).unwrap().p99, TIMER_RESOLUTION);
        assert!(model.predict(3001).is_none());
        histogram.update(Instant::now(), &Sample::from_millis(10, 3000, 1000));
        let model = histogram.summary(true);
        assert!(model.predict(MIB).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_overhead_alone_does_not_establish_larger_transfer_times() {
        let mut histogram = BodyHistogram::default();
        for bytes in [4096, 65_536] {
            for _ in 0..4 {
                histogram.update(Instant::now(), &Sample::from_millis(10, bytes, 20));
            }
        }
        let model = histogram.summary(true);
        assert_eq!(
            model.predict(65_536).unwrap().p99,
            Duration::from_millis(20)
        );
        assert!(model.predict(MIB).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn empty_bodies_and_extreme_measurements_are_bounded() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, 0, 100));
        assert!(histogram.summary(false).predict(0).is_none());
        for bytes in [1, u64::MAX] {
            histogram.update(
                Instant::now(),
                &Sample {
                    first_byte: Duration::ZERO,
                    body_bytes: bytes,
                    body: Duration::MAX,
                },
            );
        }
        assert!(histogram.summary(true).predict(u64::MAX).is_some());
    }
    #[tokio::test(start_paused = true)]
    async fn isolated_large_stalls_borrow_history_and_age_out_with_other_sizes() {
        let mut histogram = BodyHistogram::default();
        for _ in 0..50 {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        }
        histogram.update(Instant::now(), &Sample::from_millis(10, 16 * MIB, 1001));
        let model = histogram.summary(true);
        assert_eq!(
            model.predict(MIB).unwrap().median,
            Duration::from_millis(11)
        );
        assert!(model.predict(MIB).unwrap().hedge_delay() <= Duration::from_micros(16_500));
        // With a nonnegative intercept, extrapolation grows no faster than response size.
        assert!(model.predict(16 * MIB).unwrap().hedge_delay() <= Duration::from_millis(265));
        advance(Duration::from_secs(180)).await;
        for _ in 0..4 {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        }
        assert_eq!(
            histogram.summary(true).predict(MIB).unwrap().p99,
            Duration::from_millis(11)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn diverse_exact_sizes_share_fixed_overhead_from_nine_observations() {
        let mut histogram = BodyHistogram::default();
        for exponent in 10..=18 {
            let bytes = 1 << exponent;
            histogram.update(
                Instant::now(),
                &Sample {
                    first_byte: Duration::from_millis(25),
                    body_bytes: bytes,
                    body: Duration::from_secs_f64(0.020 + 0.010 * bytes as f64 / MIB as f64),
                },
            );
        }
        let model = histogram.summary(true);
        for bytes in [1, 512, 1024, 4096, 12_345, 65_536, 262_144] {
            let expected = 0.020 + 0.010 * bytes as f64 / MIB as f64;
            let quantiles = model.predict(bytes).unwrap();
            assert!(
                (quantiles.median.as_secs_f64() - expected).abs() < 0.000_005,
                "{bytes}: {quantiles:?}"
            );
            assert!(
                (quantiles.p99.as_secs_f64() - expected).abs() < 0.000_005,
                "{bytes}: {quantiles:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_rare_large_stall_does_not_become_the_body_median() {
        let mut histogram = BodyHistogram::default();
        for (bytes, body_micros, count) in [
            (1024, 20_010, 100),
            (65_536, 20_625, 10),
            (16 * MIB, 180_000, 1),
            (16 * MIB, 800_000, 1),
        ] {
            for _ in 0..count {
                histogram.update(
                    Instant::now(),
                    &Sample {
                        first_byte: Duration::from_millis(10),
                        body_bytes: bytes,
                        body: Duration::from_micros(body_micros),
                    },
                );
            }
        }
        let predicted = refreshed(&mut histogram, true)
            .await
            .predict(16 * MIB)
            .unwrap();
        assert!(predicted.median.abs_diff(Duration::from_millis(180)) <= Duration::from_micros(1));
        assert!(predicted.p99.abs_diff(Duration::from_millis(800)) <= Duration::from_micros(1));
        assert!(
            predicted.hedge_delay().abs_diff(Duration::from_millis(270))
                <= Duration::from_micros(2)
        );
    }

    #[test]
    fn pending_fits_serve_cached_predictions_and_coalesce_refreshes() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut histogram = BodyHistogram::default();
            for _ in 0..INLINE_FIT_SAMPLES {
                histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
            }
            histogram.summary(false);
            let (release, blocked) = tokio::sync::oneshot::channel::<()>();
            let (started, waiting) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                let _ = blocked.blocking_recv();
            });
            waiting.await.unwrap();
            for _ in 0..=INLINE_FIT_SAMPLES {
                histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
            }
            assert_eq!(
                histogram.summary(true).predict(MIB).unwrap().p99,
                Duration::from_millis(11)
            );
            for _ in 0..4 {
                histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 41));
            }
            assert_eq!(
                histogram.summary(true).predict(MIB).unwrap().p99,
                Duration::from_millis(11)
            );
            release.send(()).unwrap();
            blocker.await.unwrap();
            assert_eq!(
                refreshed(&mut histogram, false)
                    .await
                    .predict(MIB)
                    .unwrap()
                    .p99,
                Duration::from_millis(21)
            );
            assert_eq!(
                refreshed(&mut histogram, true)
                    .await
                    .predict(MIB)
                    .unwrap()
                    .p99,
                Duration::from_millis(41)
            );
        });
    }

    #[tokio::test]
    async fn failed_fits_can_retry_without_new_observations() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        histogram.summary(false);
        for _ in 0..INLINE_FIT_SAMPLES {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
        }
        let (failed, pending) = tokio::sync::oneshot::channel();
        histogram.pending = Some(pending);
        histogram.summarized_updates = histogram.updates;
        drop(failed);
        assert_eq!(
            refreshed(&mut histogram, false)
                .await
                .predict(MIB)
                .unwrap()
                .p99,
            Duration::from_millis(21)
        );
    }

    #[test]
    fn large_refreshes_outside_a_runtime_keep_the_cached_model() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        histogram.summary(false);
        for _ in 0..INLINE_FIT_SAMPLES {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
        }
        assert_eq!(
            histogram.summary(true).predict(MIB).unwrap().p99,
            Duration::from_millis(11)
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let updated = runtime.block_on(refreshed(&mut histogram, false));
        assert_eq!(updated.predict(MIB).unwrap().p99, Duration::from_millis(21));
    }

    proptest! {
        #[test]
        fn fits_match_exhaustive_quantile_regression(
            samples in collection::vec((1_u64..=16 * MIB, 0_u32..=1_000_000, 1_u8..=20, 0_u8..=10), 2..12),
        ) {
            let observations: Vec<_> = samples.iter().map(|&(bytes, micros, weight, age)| Observation {
                bytes: if age % 2 == 0 { bytes.next_power_of_two() } else { bytes },
                micros: f64::from(micros).max(TIMER_MICROS),
                weight: f64::from(weight) * 2.0_f64.powi(-i32::from(age)),
            }).collect();
            // A piecewise-linear loss attains a minimum where two sample lines intersect,
            // or where one sample line meets a nonnegative coefficient boundary.
            let mut candidates = Vec::new();
            for (index, left) in observations.iter().enumerate() {
                candidates.extend([
                    Line { fixed: left.micros, per_byte: 0.0 },
                    Line { fixed: 0.0, per_byte: left.micros / left.bytes as f64 },
                ]);
                for right in &observations[index + 1..] {
                    if left.bytes != right.bytes {
                        let per_byte = (right.micros - left.micros)
                            / (right.bytes as f64 - left.bytes as f64);
                        let fixed = left.micros - per_byte * left.bytes as f64;
                        if fixed >= 0.0 && per_byte >= 0.0 {
                            candidates.push(Line { fixed, per_byte });
                        }
                    }
                }
            }
            let tolerance: f64 = observations.iter().map(|sample| sample.weight).sum();
            for q in [0.5, 0.99] {
                let loss = |line: Line| observations.iter().map(|sample| {
                    let residual = sample.micros - line.fixed - line.per_byte * sample.bytes as f64;
                    sample.weight * residual * if residual >= 0.0 { q } else { q - 1.0 }
                }).sum::<f64>();
                let optimal = candidates.iter().copied().map(loss).fold(f64::INFINITY, f64::min);
                let fitted = fit(&observations, q, &mut Vec::new());
                prop_assert!(fitted.fixed.is_finite() && fitted.fixed >= 0.0);
                prop_assert!(fitted.per_byte.is_finite() && fitted.per_byte >= 0.0);
                prop_assert!(loss(fitted) <= optimal + tolerance, "q={q}: {fitted:?}, loss={}, optimal={optimal}", loss(fitted));
            }
        }
    }
}
