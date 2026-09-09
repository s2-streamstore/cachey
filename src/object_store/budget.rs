use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;

use crate::types::BucketName;

const ATTEMPT_COST: u32 = 100;
const MAX_RETRY_CREDITS: u32 = 16 * ATTEMPT_COST;
const MAX_BUCKET_HEDGES: u16 = 2;

#[derive(Debug)]
struct BudgetState {
    hedge_credits: u32,
    retry_credits: u32,
    active_hedges: u16,
    active_by_bucket: HashMap<BucketName, u16>,
}

#[derive(Debug, Clone)]
pub(super) struct AttemptBudget {
    state: Arc<Mutex<BudgetState>>,
    max_hedges: u16,
    hedge_credit: u8,
}

impl AttemptBudget {
    pub fn new(max_hedges: u16, hedge_credit: u8) -> Self {
        Self {
            state: Arc::new(Mutex::new(BudgetState {
                hedge_credits: ATTEMPT_COST,
                retry_credits: ATTEMPT_COST,
                active_hedges: 0,
                active_by_bucket: HashMap::new(),
            })),
            max_hedges,
            hedge_credit,
        }
    }

    pub fn observe_success(&self) {
        let mut state = self.state.lock();
        state.hedge_credits = (state.hedge_credits + u32::from(self.hedge_credit))
            .min(u32::from(self.max_hedges) * ATTEMPT_COST);
        state.retry_credits = (state.retry_credits + 10).min(MAX_RETRY_CREDITS);
    }

    pub fn try_hedge(&self, bucket: &BucketName) -> Option<HedgePermit> {
        if self.max_hedges == 0 || self.hedge_credit == 0 {
            return None;
        }
        let mut state = self.state.lock();
        if state.active_hedges >= self.max_hedges || state.hedge_credits < ATTEMPT_COST {
            return None;
        }
        let budget = state.active_by_bucket.entry(bucket.clone()).or_default();
        if *budget >= MAX_BUCKET_HEDGES {
            return None;
        }
        *budget += 1;
        state.active_hedges += 1;
        state.hedge_credits -= ATTEMPT_COST;
        Some(HedgePermit {
            state: self.state.clone(),
            bucket: bucket.clone(),
        })
    }

    pub fn try_retry(&self) -> bool {
        let mut state = self.state.lock();
        if state.retry_credits < ATTEMPT_COST {
            return false;
        }
        state.retry_credits -= ATTEMPT_COST;
        true
    }
}

pub(super) struct HedgePermit {
    state: Arc<Mutex<BudgetState>>,
    bucket: BucketName,
}

impl Drop for HedgePermit {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.active_hedges -= 1;
        if let Some(budget) = state.active_by_bucket.get_mut(&self.bucket) {
            *budget -= 1;
            if *budget == 0 {
                state.active_by_bucket.remove(&self.bucket);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AttemptBudget;
    use crate::types::BucketName;

    #[test]
    fn startup_and_earned_hedges_are_shared_across_buckets_and_clones() {
        let budget = AttemptBudget::new(16, 5);
        let first = BucketName::new("first").unwrap();
        let second = BucketName::new("second").unwrap();
        drop(budget.try_hedge(&first).unwrap());
        assert!(budget.clone().try_hedge(&second).is_none());
        for _ in 0..19 {
            budget.observe_success();
        }
        assert!(budget.try_hedge(&first).is_none());
        budget.observe_success();
        drop(budget.clone().try_hedge(&second).unwrap());
        assert!(budget.try_hedge(&first).is_none());
    }

    #[test]
    fn concurrency_caps_apply_even_when_more_successes_replenish_credits() {
        for (global_limit, buckets) in [
            (1, ["first", "second", "third"]),
            (16, ["bucket", "bucket", "bucket"]),
        ] {
            let budget = AttemptBudget::new(global_limit, 100);
            let buckets = buckets.map(|name| BucketName::new(name).unwrap());
            for _ in &buckets {
                budget.observe_success();
            }
            let first = budget.try_hedge(&buckets[0]).unwrap();
            let second = if global_limit > 1 {
                Some(budget.try_hedge(&buckets[1]).unwrap())
            } else {
                None
            };
            for _ in &buckets {
                budget.observe_success();
            }
            assert!(budget.try_hedge(&buckets[2]).is_none());
            drop(first);
            assert!(budget.try_hedge(&buckets[2]).is_some());
            drop(second);
        }
    }

    #[test]
    fn zero_hedge_limits_preserve_overload_retries() {
        let bucket = BucketName::new("bucket").unwrap();
        for budget in [AttemptBudget::new(0, 5), AttemptBudget::new(16, 0)] {
            assert!(budget.try_retry());
            assert!(!budget.try_retry());
            for _ in 0..10 {
                budget.observe_success();
            }
            assert!(budget.try_hedge(&bucket).is_none());
            assert!(budget.try_retry());
            assert!(!budget.try_retry());
        }
    }

    #[test]
    fn hedges_and_overload_retries_keep_separate_allowances() {
        let budget = AttemptBudget::new(16, 5);
        let bucket = BucketName::new("bucket").unwrap();
        drop(budget.try_hedge(&bucket).unwrap());
        assert!(budget.clone().try_retry());
        assert!(!budget.try_retry());
        for _ in 0..10 {
            budget.observe_success();
        }
        assert!(budget.try_hedge(&bucket).is_none());
        assert!(budget.try_retry());
        assert!(!budget.try_retry());
        for _ in 0..10 {
            budget.observe_success();
        }
        drop(budget.try_hedge(&bucket).unwrap());
        assert!(budget.try_retry());
        assert!(budget.try_hedge(&bucket).is_none());
        assert!(!budget.try_retry());
    }
}
