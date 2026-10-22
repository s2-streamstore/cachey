use std::{
    collections::BTreeMap,
    hash::{BuildHasher, RandomState},
};

use super::LATENCY_DECAY;

const CAPACITY: usize = 256;
/// Samples older than this weigh under 0.4% of a new one.
const PRUNE_HALF_LIVES: f64 = 8.0;

#[derive(Debug, Clone, Copy)]
struct Measurement<T> {
    value: T,
    /// Model time in seconds.
    at: f64,
}

/// Priority sampling with inclusion-corrected weights and exponential age decay.
/// Time is the owning model's clock, in seconds.
#[derive(Debug)]
pub(super) struct Reservoir<T> {
    samples: BTreeMap<(u64, u64), Measurement<T>>,
    capacity: usize,
    count: u64,
    pruned: u64,
    random: RandomState,
    discarded_priority: Option<u64>,
}

impl<T: Copy> Default for Reservoir<T> {
    fn default() -> Self {
        let capacity = CAPACITY;
        #[cfg(test)]
        let capacity = crate::object_store::simulation::histogram_capacity().unwrap_or(capacity);
        Self {
            samples: BTreeMap::new(),
            capacity,
            count: 0,
            pruned: 0,
            random: RandomState::new(),
            discarded_priority: None,
        }
    }
}

impl<T: Copy> Reservoir<T> {
    #[cfg(test)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            ..Self::default()
        }
    }

    /// Unpruned observations and retained samples; these differ once sampling evicts.
    #[cfg(test)]
    pub fn retention(&self) -> (u64, usize) {
        (self.count - self.pruned, self.samples.len())
    }

    pub fn update(&mut self, at: f64, value: T) {
        let uniform = ((self.random.hash_one(self.count) >> 11) + 1) as f64 / (1_u64 << 53) as f64;
        self.retain(at, value, uniform);
    }

    pub(super) fn retain(&mut self, at: f64, value: T, uniform: f64) {
        self.count += 1;
        // Log priorities avoid rescaling. Positive float bits preserve numeric order;
        // the observation count distinguishes equal priorities.
        let priority = (LATENCY_DECAY * at - uniform.ln()).to_bits();
        let measurement = Measurement { value, at };
        if self.samples.len() < self.capacity {
            // Space freed by pruning admits only priorities a full reservoir would keep,
            // so the threshold remains each sample's inclusion bound.
            if self
                .discarded_priority
                .is_none_or(|threshold| priority > threshold)
            {
                self.samples.insert((priority, self.count), measurement);
            }
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

    /// Decayed weights at `at`, after dropping samples whose weight has become negligible.
    pub fn weighted(&mut self, at: f64) -> impl Iterator<Item = (T, f64)> + '_ {
        let horizon = at - PRUNE_HALF_LIVES * std::f64::consts::LN_2 / LATENCY_DECAY;
        let retained = self.samples.len();
        self.samples.retain(|_, sample| sample.at >= horizon);
        self.pruned += (retained - self.samples.len()) as u64;
        if self.samples.is_empty() {
            self.discarded_priority = None;
        }
        // Priority sampling estimates each retained weight as the larger of its own and
        // the highest discarded priority: https://arxiv.org/abs/cs/0509026.
        let minimum_weight = self.discarded_priority.map_or(0.0, |priority| {
            (f64::from_bits(priority) - LATENCY_DECAY * at).exp()
        });
        self.samples.values().map(move |sample| {
            let weight = (-LATENCY_DECAY * (at - sample.at)).exp();
            (sample.value, weight.max(minimum_weight))
        })
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
    use super::{CAPACITY, LATENCY_DECAY, Reservoir, quantile};

    #[test]
    fn sampling_estimates_decayed_mass_and_prunes_negligible_weight() {
        let mut sampled = Reservoir::default();
        let mut full = Reservoir::with_capacity(20_000);
        assert!(sampled.weighted(0.0).next().is_none());
        let mut at = 0.0;
        for (count, value) in [(10_000, 11.0), (4_000, 41.0)] {
            for index in 0..count {
                let uniform = (f64::from(index) + 0.5) / f64::from(count);
                for reservoir in [&mut sampled, &mut full] {
                    reservoir.retain(at, value, uniform);
                }
            }
            at += 15.0;
        }
        assert_eq!(sampled.retention(), (14_000, CAPACITY));
        assert_eq!(full.retention(), (14_000, 14_000));
        for (reservoir, tolerance) in [(&mut sampled, 0.05), (&mut full, 1e-9)] {
            let mut values: Vec<_> = reservoir.weighted(at).collect();
            let mass: f64 = values.iter().map(|(_, weight)| weight).sum();
            assert!((mass / 4500.0 - 1.0).abs() < tolerance, "{mass}");
            assert_eq!(quantile(&mut values, 0.5), 11.0);
            assert_eq!(quantile(&mut values, 0.99), 41.0);
        }
        at += 8.0 * 15.0;
        for reservoir in [&mut sampled, &mut full] {
            assert!(reservoir.weighted(at).next().is_none());
            assert_eq!(reservoir.retention().1, 0);
            reservoir.retain(at, 21.0, 0.5);
            assert_eq!(reservoir.weighted(at).next(), Some((21.0, 1.0)));
        }
    }

    /// Once pruning frees space, new samples keep unbiased weights whether or not older
    /// samples selected under the sampling threshold remain.
    #[test]
    fn refilled_samples_keep_unbiased_weights() {
        let half_life = std::f64::consts::LN_2 / LATENCY_DECAY;
        for survivor in [false, true] {
            let mut reservoir = Reservoir::default();
            let count = 1000 * CAPACITY;
            for index in 0..count {
                let uniform = (index as f64 + 0.5) / count as f64;
                reservoir.retain(0.0, 1, uniform);
            }
            if survivor {
                reservoir.retain(2.0 * half_life, 1, 1e-9);
            }
            let at = 8.5 * half_life;
            assert_eq!(reservoir.weighted(at).count(), usize::from(survivor));
            for index in 0..100 {
                reservoir.retain(at, 2, (f64::from(index) + 0.5) / 100.0);
            }
            let refilled: Vec<_> = reservoir
                .weighted(at)
                .filter_map(|(value, weight)| (value == 2).then_some(weight))
                .collect();
            // An emptied reservoir samples afresh.
            if !survivor {
                assert_eq!(refilled.len(), 100);
            }
            let mass: f64 = refilled.iter().sum();
            assert!((mass / 100.0 - 1.0).abs() < 0.05, "{survivor}: {mass}");
        }
    }

    #[test]
    fn quantiles_take_the_nearest_rank_of_samples_recorded_together() {
        let mut samples = Reservoir::default();
        for value in [5, 1, 3, 3, 3, 9, 7, 7, 2, 8] {
            samples.update(0.0, value);
        }
        let mut values: Vec<_> = samples
            .weighted(0.0)
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
            // 0.7 × 10 rounds above 7 in floating point.
            (0.7, 7),
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
