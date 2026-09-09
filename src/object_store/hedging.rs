use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;

use crate::types::BucketName;

const HEDGE_COST: u32 = 100;
const MAX_BUCKET_HEDGES: u16 = 2;

#[derive(Debug)]
struct BucketBudget {
    credits: u32,
    active: u16,
}

impl Default for BucketBudget {
    fn default() -> Self {
        Self {
            credits: HEDGE_COST,
            active: 0,
        }
    }
}

#[derive(Debug)]
struct BudgetState {
    credits: u32,
    active: u16,
    buckets: HashMap<BucketName, BucketBudget>,
}

#[derive(Debug, Clone)]
pub(super) struct HedgeBudget {
    state: Arc<Mutex<BudgetState>>,
    max_concurrent: u16,
    success_credit: u8,
}

impl HedgeBudget {
    pub fn new(max_concurrent: u16, success_credit: u8) -> Self {
        Self {
            state: Arc::new(Mutex::new(BudgetState {
                credits: HEDGE_COST,
                active: 0,
                buckets: HashMap::new(),
            })),
            max_concurrent,
            success_credit,
        }
    }

    pub fn observe_success(&self, bucket: &BucketName) {
        if self.max_concurrent == 0 || self.success_credit == 0 {
            return;
        }
        let mut state = self.state.lock();
        let credit = u32::from(self.success_credit);
        state.credits = (state.credits + credit).min(u32::from(self.max_concurrent) * HEDGE_COST);
        if let Some(budget) = state.buckets.get_mut(bucket) {
            budget.credits =
                (budget.credits + credit).min(u32::from(MAX_BUCKET_HEDGES) * HEDGE_COST);
        } else {
            state.buckets.insert(
                bucket.clone(),
                BucketBudget {
                    credits: HEDGE_COST + credit,
                    active: 0,
                },
            );
        }
    }

    pub fn try_acquire(&self, bucket: &BucketName) -> Option<HedgePermit> {
        if self.max_concurrent == 0 || self.success_credit == 0 {
            return None;
        }
        let mut state = self.state.lock();
        if state.active >= self.max_concurrent || state.credits < HEDGE_COST {
            return None;
        }
        let budget = state.buckets.entry(bucket.clone()).or_default();
        if budget.active >= MAX_BUCKET_HEDGES || budget.credits < HEDGE_COST {
            return None;
        }
        budget.active += 1;
        budget.credits -= HEDGE_COST;
        state.active += 1;
        state.credits -= HEDGE_COST;
        Some(HedgePermit {
            state: self.state.clone(),
            bucket: bucket.clone(),
        })
    }
}

pub(super) struct HedgePermit {
    state: Arc<Mutex<BudgetState>>,
    bucket: BucketName,
}

impl Drop for HedgePermit {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.active -= 1;
        if let Some(budget) = state.buckets.get_mut(&self.bucket) {
            budget.active -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HedgeBudget;
    use crate::types::BucketName;

    #[test]
    fn successful_fetches_fund_hedges_and_drops_only_release_concurrency() {
        let budget = HedgeBudget::new(16, 5);
        let bucket = BucketName::new("bucket").unwrap();
        drop(budget.try_acquire(&bucket).unwrap());
        for _ in 0..19 {
            budget.observe_success(&bucket);
        }
        assert!(budget.try_acquire(&bucket).is_none());
        budget.observe_success(&bucket);
        drop(budget.try_acquire(&bucket).unwrap());
        assert!(budget.try_acquire(&bucket).is_none());
    }

    #[test]
    fn startup_allowance_is_shared_across_buckets_and_clones() {
        let budget = HedgeBudget::new(16, 5);
        let first = BucketName::new("first").unwrap();
        let second = BucketName::new("second").unwrap();
        drop(budget.try_acquire(&first).unwrap());
        assert!(budget.clone().try_acquire(&second).is_none());
        for _ in 0..20 {
            budget.observe_success(&first);
        }
        drop(budget.clone().try_acquire(&second).unwrap());
        assert!(budget.try_acquire(&first).is_none());
    }

    #[test]
    fn concurrency_caps_apply_even_when_more_successes_replenish_credits() {
        for (global_limit, buckets) in [
            (1, ["first", "second", "third"]),
            (16, ["bucket", "bucket", "bucket"]),
        ] {
            let budget = HedgeBudget::new(global_limit, 100);
            let buckets = buckets.map(|name| BucketName::new(name).unwrap());
            for bucket in &buckets {
                budget.observe_success(bucket);
            }
            let first = budget.try_acquire(&buckets[0]).unwrap();
            let second = if global_limit > 1 {
                Some(budget.try_acquire(&buckets[1]).unwrap())
            } else {
                None
            };
            for bucket in &buckets {
                budget.observe_success(bucket);
            }
            assert!(budget.try_acquire(&buckets[2]).is_none());
            drop(first);
            assert!(budget.try_acquire(&buckets[2]).is_some());
            drop(second);
        }
    }

    #[test]
    fn zero_limits_disable_speculation() {
        let bucket = BucketName::new("bucket").unwrap();
        for budget in [HedgeBudget::new(0, 5), HedgeBudget::new(16, 0)] {
            budget.observe_success(&bucket);
            assert!(budget.try_acquire(&bucket).is_none());
        }
    }
}
