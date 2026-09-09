# Replica simulation reference

This comparison measures completion, latency, retry amplification, and service work
under errors, stalls, recovery, and limited capacity. No production traffic is used.

## Revisions and reproduction

- Before: `0d3cd24778407ed3d3f47938b7fd7e9969443c3a`.
- After: `dc85f8e8b29029d1ad4d944c978a66d3fe05f049`, with a clean working tree.
- Both use the simulation and fixtures from `d0754d6`, unchanged in the after revision.
  The baseline harness adapts the constructor and limits API and omits the new error
  variant; its routing and execution code are unchanged. The report records its dirty
  flag and patch digest.

Each revision has **117 runs / 307,281 arrivals**: 99 exact-retention runs across all
33 scenarios with seeds 7, 42, and 2026, plus 18 production-reservoir runs / 132,000
arrivals. The latter use `healthy_jitter`, `finite_capacity`, and
`finite_capacity_delayed_cancel`, seeds 7 and 42, and rate multipliers 1, 2, and 4.
See [README.md](README.md) for commands and model limitations.

[reference.json](reference.json) contains all 234 before/after runs, source metadata,
seeds, and resolved configurations. Configuration keys are prefixes of canonical
JSON SHA-256 digests. Recovery observations retain their count, first observation per
replica, and minimum clean-success count; ordinary campaign output retains them all.

## Availability and latency

All matched workloads preserve or improve completion. Among runs with unchanged
completion, p99 and maximum successful-read latency are unchanged. The table sums
three exact-retention seeds; p99 is the largest per-run value, not a pooled quantile.
Every scheduled arrival remains in the completion denominator.

| Scenario | Before completion | After completion | After p99 |
|---|---:|---:|---:|
| Healthy zonal / regional | 1,800/1,800 each | 1,800/1,800 each | 3 / 10 ms |
| Cold replicas; first two stall, third needs 150 ms, deadline 500 ms | 0/36 | **36/36** | 484 ms |
| Same regional outage after calibration | 1,800/1,800 | 1,800/1,800 | 412 ms |
| Independent 10% errors on all three copies | 89,910/90,000 | 89,910/90,000 | 7 ms |
| Fully correlated faults | 1,728/1,800 | 1,728/1,800 | 3 ms |
| Finite service capacity | 5,418/9,000 | 5,418/9,000 | 448 ms |
| Finite capacity with SDK retries | 5,421/9,000 | 5,421/9,000 | 448 ms |
| Regional outage, 200 arrivals/s, full-page admission | 6,990/7,200 | 6,990/7,200 | 405 ms |
| Healthy 4 s bodies, eight admission slots, 10 arrivals/s | 48/180 | 48/180 | 7.2 s |
| Healthy distant copy cannot fit the page deadline | 1,792/1,800 | 1,792/1,800 | 70 ms |

Cold failover now reserves useful time for unmeasured destinations. Its wire attempts
fall from 108 to 78. The distant-copy case uses 1,902 attempts instead of 2,798, with
less service work and unchanged completion. The SDK saturation case uses 10,464
attempts instead of 10,452: four extra requests per 3,000 arrivals, with unchanged
completion and p99. Its mean successful latency decreases from 95.143 to 94.993 ms.

The 18 production-reservoir comparisons preserve completion, latency, wire attempts,
and service work. Finite-capacity completion falls from 1,806/3,000 to 3,117/6,000 and
4,319/12,000 as load doubles and quadruples. These sampled workloads do not establish
a universal guarantee about reservoir behavior or real service distributions.

## Bounds and targeted regressions

Unit tests also cover a measured 400 ms primary that stalls with a 150 ms peer and
500 ms deadline, malformed successful responses permitting fallback, and page expiry
recording timeout health. Unstarted recovery probes remain eligible; missing copies
cannot hide another copy's failure or a denied overload retry. A 250 ms primary with
300 ms peers and a 400 ms deadline needs overlapping copies to make fallback possible:
the healthy primary still wins at 250 ms, and a stalled primary can be rescued before
expiry. Recent explicit overload preserves primary grace to limit added congestion.

The server reserves 16 MiB per copy, so the default 1 GiB budget admits 64 copies.
That bound explains the retained full-page regional failures; unconstrained small
requests all complete. Stalled primaries occupying every admission slot still leave
no room for a healthy alternative. Correlated faults and destinations slower than
the page deadline also remain availability limits. Recovery during a peer outage
completes every read but reaches 9.983 s against a 10 s deadline.

Cancellation can continue consuming backend capacity. In the separate cancellation
storm, a 100 ms cancellation delay raises service work from 3.015 to 9.223 slot-seconds
and capacity rejections from zero to 264. All 600 reads are intentionally canceled;
this measures resource cost, not availability. Those results are unchanged. Timer
ties in deterministic healthy-body and jitter fixtures can also launch redundant
requests without improving latency; their wire attempts remain counted.
