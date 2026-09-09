use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;

use crate::types::BucketName;

const ATTEMPT_COST: u32 = 100;
const MAX_HEDGES: u16 = 16;
const MAX_CREDITS: u32 = MAX_HEDGES as u32 * ATTEMPT_COST;
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
    hedge_credit: u8,
}

impl AttemptBudget {
    pub fn new(hedge_credit: u8) -> Self {
        Self {
            state: Arc::new(Mutex::new(BudgetState {
                hedge_credits: ATTEMPT_COST,
                retry_credits: ATTEMPT_COST,
                active_hedges: 0,
                active_by_bucket: HashMap::new(),
            })),
            hedge_credit,
        }
    }

    pub fn observe_success(&self) {
        let mut state = self.state.lock();
        state.hedge_credits = (state.hedge_credits + u32::from(self.hedge_credit)).min(MAX_CREDITS);
        state.retry_credits = (state.retry_credits + 10).min(MAX_CREDITS);
    }

    pub fn try_hedge(&self, bucket: &BucketName) -> Option<HedgePermit> {
        if self.hedge_credit == 0 {
            return None;
        }
        let mut state = self.state.lock();
        if state.active_hedges >= MAX_HEDGES || state.hedge_credits < ATTEMPT_COST {
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
    fn concurrency_caps_apply_even_when_more_successes_replenish_credits() {
        for names in [
            (0..17).map(|index| format!("bucket-{index}")).collect(),
            vec!["bucket".to_owned(); 3],
        ] {
            let budget = AttemptBudget::new(100);
            let buckets: Vec<_> = names
                .into_iter()
                .map(|name| BucketName::new(name).unwrap())
                .collect();
            let (next, initial) = buckets.split_last().unwrap();
            let mut permits = Vec::new();
            for bucket in initial {
                budget.observe_success();
                permits.push(budget.try_hedge(bucket).unwrap());
            }
            budget.observe_success();
            assert!(budget.try_hedge(next).is_none());
            drop(permits.pop());
            assert!(budget.try_hedge(next).is_some());
        }
    }

    #[test]
    fn disabling_hedges_preserves_overload_retries() {
        let bucket = BucketName::new("bucket").unwrap();
        let budget = AttemptBudget::new(0);
        assert!(budget.try_retry());
        assert!(!budget.try_retry());
        for _ in 0..10 {
            budget.observe_success();
        }
        assert!(budget.try_hedge(&bucket).is_none());
        assert!(budget.try_retry());
        assert!(!budget.try_retry());
    }

    #[test]
    fn clones_share_credits_but_hedges_and_retries_keep_separate_allowances() {
        let budget = AttemptBudget::new(5);
        let first = BucketName::new("first").unwrap();
        let second = BucketName::new("second").unwrap();
        drop(budget.try_hedge(&first).unwrap());
        assert!(budget.clone().try_hedge(&second).is_none());
        assert!(budget.clone().try_retry());
        assert!(!budget.try_retry());
        for _ in 0..10 {
            budget.observe_success();
        }
        assert!(budget.try_hedge(&first).is_none());
        assert!(budget.try_retry());
        assert!(!budget.try_retry());
        for _ in 0..9 {
            budget.observe_success();
        }
        assert!(budget.try_hedge(&first).is_none());
        budget.observe_success();
        drop(budget.clone().try_hedge(&second).unwrap());
        assert!(budget.try_retry());
        assert!(budget.try_hedge(&first).is_none());
        assert!(!budget.try_retry());
    }
}
