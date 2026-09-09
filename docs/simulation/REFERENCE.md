# Replica simulation reference

## Reproduction

- Before: `0d3cd24778407ed3d3f47938b7fd7e9969443c3a`.
- After: `c61231ea5fe961d44e1fd0cb06160e58d8455ce7`, clean working tree.
- Harness and fixtures: `d0754d6`. The baseline adapter changes only harness API
  compatibility; its patch digest is recorded in [reference.json](reference.json).

Each revision has **117 runs / 307,281 arrivals**:

- 99 runs retain all histogram samples: all 33 scenarios, seeds 7, 42, and 2026.
- 18 production-reservoir runs: `healthy_jitter`, `finite_capacity`, and
  `finite_capacity_delayed_cancel`; seeds 7 and 42; rate multipliers 1, 2, and 4.

[reference.json](reference.json) contains all 234 runs, source metadata, seeds, and
resolved configurations. See [running instructions and model limits](README.md).

## Results

All matched workloads preserve or improve completion. Where completion is unchanged,
p99 and maximum successful-read latency are unchanged. The table sums three seeds;
p99 is the largest per-run value. Completion counts every scheduled arrival.

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

Reserving time for unmeasured replicas fixes cold failover and reduces its wire
attempts from 108 to 78. The distant-copy case falls from 2,798 to 1,902 attempts.
SDK saturation adds four requests per 3,000 arrivals with unchanged completion and
p99. All 18 production-reservoir pairs preserve completion, latency, and backend work.

## Limits observed

Admission saturation still prevents some healthy fallbacks. Correlated faults and
copies beyond the page deadline also limit availability. Recovery during a peer
outage completes every read but reaches 9.983 s against a 10 s deadline.

In the cancellation-storm fixture, a 100 ms cancellation delay raises service work
from 3.015 to 9.223 slot-seconds and capacity rejections from zero to 264. All 600
reads are intentionally canceled. These results are unchanged between revisions.
