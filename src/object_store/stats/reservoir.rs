use std::{
    collections::BTreeMap,
    hash::{BuildHasher, RandomState},
};

use tokio::time::Instant;

use super::{LATENCY_DECAY, decay};

const CAPACITY: usize = 1028;

#[derive(Debug, Clone, Copy)]
struct Measurement<T> {
    value: T,
    at: Instant,
}

pub(super) struct Snapshot<T> {
    samples: Vec<Measurement<T>>,
    at: Instant,
    weight: f64,
    minimum_weight: f64,
}

impl<T: Copy> Snapshot<T> {
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn weighted(&self) -> impl Iterator<Item = (T, f64)> + '_ {
        let weight = |at| {
            decay(LATENCY_DECAY, self.at.saturating_duration_since(at)).max(self.minimum_weight)
        };
        let retained_weight: f64 = self.samples.iter().map(|sample| weight(sample.at)).sum();
        // Preserve the reservoir's observation mass when combining size strata.
        let correction = if retained_weight > 0.0 {
            self.weight / retained_weight
        } else {
            0.0
        };
        self.samples.iter().filter_map(move |sample| {
            let weight = weight(sample.at) * correction;
            (weight > 0.0).then_some((sample.value, weight))
        })
    }
}

/// Priority sampling with inclusion-corrected weights and exponential age decay.
#[derive(Debug)]
pub(super) struct Reservoir<T> {
    samples: BTreeMap<(u64, u64), Measurement<T>>,
    capacity: usize,
    count: u64,
    random: RandomState,
    started: Instant,
    discarded_priority: Option<u64>,
    weight: f64,
    updated_at: Instant,
}

impl<T: Copy> Reservoir<T> {
    pub fn new(now: Instant) -> Self {
        let capacity = CAPACITY;
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

    #[cfg(test)]
    pub fn with_capacity(now: Instant, capacity: usize) -> Self {
        Self {
            capacity,
            ..Self::new(now)
        }
    }

    #[cfg(test)]
    pub fn retention(&self) -> (u64, usize) {
        (self.count, self.samples.len())
    }

    pub fn update(&mut self, now: Instant, value: T) {
        let uniform = ((self.random.hash_one(self.count) >> 11) + 1) as f64 / (1_u64 << 53) as f64;
        self.retain(now, value, uniform);
    }

    pub(super) fn retain(&mut self, now: Instant, value: T, uniform: f64) {
        self.weight = self.weight_at(now) + 1.0;
        self.updated_at = now;
        self.count += 1;
        // Log priorities avoid rescaling. Positive float bits preserve numeric order;
        // the observation count distinguishes equal priorities.
        let priority = (LATENCY_DECAY * now.duration_since(self.started).as_secs_f64()
            - uniform.ln())
        .to_bits();
        let measurement = Measurement { value, at: now };
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

    pub fn snapshot(&self, now: Instant) -> Snapshot<T> {
        // Priority sampling assigns max(weight, highest discarded priority), correcting
        // for age-biased inclusion: https://arxiv.org/abs/cs/0509026.
        let minimum_weight = self.discarded_priority.map_or(0.0, |priority| {
            (f64::from_bits(priority)
                - LATENCY_DECAY * now.duration_since(self.started).as_secs_f64())
            .exp()
        });
        Snapshot {
            samples: self.samples.values().copied().collect(),
            at: now,
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

/// Weighted nearest-rank quantile, including the selected value's own weight.
pub(super) fn quantile(mut values: &mut [(f64, f64)], q: f64) -> f64 {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{Instant, advance};

    use super::{CAPACITY, Reservoir, quantile};

    #[tokio::test(start_paused = true)]
    async fn sampling_preserves_decayed_mass_and_recovers_after_long_idle() {
        let mut sampled = Reservoir::new(Instant::now());
        let mut full = Reservoir::with_capacity(Instant::now(), 20_000);
        assert!(sampled.snapshot(Instant::now()).weighted().next().is_none());
        for (count, value) in [(10_000, 11.0), (4_000, 41.0)] {
            for index in 0..count {
                let uniform = (f64::from(index) + 0.5) / f64::from(count);
                for reservoir in [&mut sampled, &mut full] {
                    reservoir.retain(Instant::now(), value, uniform);
                }
            }
            advance(Duration::from_secs(15)).await;
        }
        assert_eq!(sampled.retention(), (14_000, CAPACITY));
        assert_eq!(full.retention(), (14_000, 14_000));
        for reservoir in [&sampled, &full] {
            let mut values: Vec<_> = reservoir.snapshot(Instant::now()).weighted().collect();
            assert!((values.iter().map(|(_, weight)| weight).sum::<f64>() - 4500.0).abs() < 1e-6);
            assert_eq!(quantile(&mut values, 0.5), 11.0);
            assert_eq!(quantile(&mut values, 0.99), 41.0);
        }
        advance(Duration::from_hours(168)).await;
        for reservoir in [&mut sampled, &mut full] {
            assert!(
                reservoir
                    .snapshot(Instant::now())
                    .weighted()
                    .next()
                    .is_none()
            );
            reservoir.retain(Instant::now(), 21.0, 0.5);
            assert_eq!(
                reservoir
                    .snapshot(Instant::now())
                    .weighted()
                    .collect::<Vec<_>>(),
                [(21.0, 1.0)]
            );
        }
    }
    #[tokio::test(start_paused = true)]
    async fn quantiles_take_the_nearest_rank_of_samples_recorded_together() {
        let now = Instant::now();
        let mut samples = Reservoir::new(now);
        for value in [5, 1, 3, 3, 3, 9, 7, 7, 2, 8] {
            samples.update(now, value);
        }
        let mut values: Vec<_> = samples
            .snapshot(now)
            .weighted()
            .map(|(value, weight)| (f64::from(value), weight))
            .collect();
        for (q, expected) in [
            (0.0, 1),
            (0.1, 1),
            (0.15, 2),
            (0.25, 3),
            (0.5, 3),
            (0.55, 5),
            (0.6, 5),
            (0.65, 7),
            (0.8, 7),
            (0.85, 8),
            (0.9, 8),
            (0.95, 9),
            (0.99, 9),
            (1.0, 9),
        ] {
            assert_eq!(quantile(&mut values, q), f64::from(expected), "{q}");
        }
    }
}
