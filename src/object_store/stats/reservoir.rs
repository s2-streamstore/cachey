use std::{
    collections::BTreeMap,
    hash::{BuildHasher, RandomState},
};

use super::LATENCY_DECAY;

/// Samples older than this weigh under 0.4% of a new one.
const PRUNE_HALF_LIVES: f64 = 8.0;

#[derive(Debug, Clone, Copy)]
struct Timestamped<T> {
    value: T,
    /// Model time in seconds.
    at: f64,
}

/// Priority sampling with inclusion-corrected weights and exponential age decay.
/// Time is the owning model's clock, in seconds.
#[derive(Debug)]
pub(super) struct Reservoir<T> {
    samples: BTreeMap<(u64, u64), Timestamped<T>>,
    capacity: usize,
    count: u64,
    pruned: u64,
    random: RandomState,
    discarded_priority: Option<u64>,
}

impl<T: Copy> Reservoir<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            samples: BTreeMap::new(),
            capacity,
            count: 0,
            pruned: 0,
            random: RandomState::new(),
            discarded_priority: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Shrinking evicts the lowest priorities, as sampling into the smaller capacity would.
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        while self.samples.len() > capacity {
            let Some(((priority, _), _)) = self.samples.pop_first() else {
                break;
            };
            self.discarded_priority = self.discarded_priority.max(Some(priority));
        }
    }

    /// Unpruned observations and retained samples; these differ once sampling evicts.
    #[cfg(test)]
    pub fn retention(&self) -> (u64, usize) {
        (self.count - self.pruned, self.samples.len())
    }

    pub fn update(&mut self, at: f64, value: T) {
        let uniform = ((self.random.hash_one(self.count) >> 11) + 1) as f64 / (1_u64 << 53) as f64;
        self.insert(at, value, uniform);
    }

    fn insert(&mut self, at: f64, value: T, uniform: f64) {
        self.count += 1;
        // Log priorities avoid rescaling. Positive float bits preserve numeric order;
        // the observation count distinguishes equal priorities.
        let priority = (LATENCY_DECAY * at - uniform.ln()).to_bits();
        let sample = Timestamped { value, at };
        if self.samples.len() < self.capacity {
            // Space freed by pruning admits only priorities a full reservoir would keep,
            // so the threshold remains each sample's inclusion bound.
            if self
                .discarded_priority
                .is_none_or(|threshold| priority > threshold)
            {
                self.samples.insert((priority, self.count), sample);
            }
        } else if let Some(lowest) = self.samples.first_entry() {
            let discarded = if priority > lowest.key().0 {
                let ((discarded, _), _) = lowest.remove_entry();
                self.samples.insert((priority, self.count), sample);
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

#[cfg(test)]
mod tests {
    use super::{LATENCY_DECAY, Reservoir};
    use crate::object_store::stats::model::quantile;

    const CAPACITY: usize = 256;

    #[test]
    fn sampling_estimates_decayed_mass_and_prunes_negligible_weight() {
        let mut sampled = Reservoir::new(CAPACITY);
        let mut full = Reservoir::new(20_000);
        assert!(sampled.weighted(0.0).next().is_none());
        let mut at = 0.0;
        for (count, value) in [(10_000, 11.0), (4_000, 41.0)] {
            for index in 0..count {
                let uniform = (f64::from(index) + 0.5) / f64::from(count);
                for reservoir in [&mut sampled, &mut full] {
                    reservoir.insert(at, value, uniform);
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
            reservoir.insert(at, 21.0, 0.5);
            assert_eq!(reservoir.weighted(at).next(), Some((21.0, 1.0)));
        }
    }

    /// Once pruning frees space, new samples keep unbiased weights whether or not older
    /// samples selected under the sampling threshold remain.
    #[test]
    fn refilled_samples_keep_unbiased_weights() {
        let half_life = std::f64::consts::LN_2 / LATENCY_DECAY;
        for survivor in [false, true] {
            let mut reservoir = Reservoir::new(CAPACITY);
            let count = 1000 * CAPACITY;
            for index in 0..count {
                let uniform = (index as f64 + 0.5) / count as f64;
                reservoir.insert(0.0, 1, uniform);
            }
            if survivor {
                reservoir.insert(2.0 * half_life, 1, 1e-9);
            }
            let at = 8.5 * half_life;
            assert_eq!(reservoir.weighted(at).count(), usize::from(survivor));
            for index in 0..100 {
                reservoir.insert(at, 2, (f64::from(index) + 0.5) / 100.0);
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
}
