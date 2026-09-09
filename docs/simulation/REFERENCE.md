# Replica simulation reference

The real-code campaign supports keeping deadline feasibility and health ahead of
latency/locality, while retaining every copy as a fallback. It also found and fixed
two availability regressions that the earlier Python model missed. Resource limits,
correlated faults, and an insufficient page deadline still constrain availability.

## Revisions and reproduction

- Before: `8d3ed8814085dfb8a8a388ab84efa2484828d048` (harness, original PR policy).
- Production fixes: `5b4339c79c2399aaaf74063486bbcb799644f805`.
- Reference harness: `d564189d88272443a49878b1e878ff665b751f35`.
- All recorded runs used clean working trees. No production traffic was used.

The reference contains 84 exact-retention runs / **153,465 arrivals**, 18 production
reservoir load-sweep runs / **132,000 arrivals**, and four before-fix runs / 664
arrivals. Exact-retention seeds are 7, 42, and 2026. The native sweep uses 7 and 42.
Only these final campaigns are counted; development runs are excluded.

[reference.json](reference.json) preserves the source metadata, seeds, resolved
configurations, and per-run results. Configurations are deduplicated by a prefix of
the SHA-256 of their canonical JSON. Recovery observations are condensed to their
count, first observation per replica, and minimum clean-success count. Ordinary
campaign output includes all observations; `CACHEY_SIM_TRACE=1` adds complete traces.
See [README.md](README.md) for commands and model limitations. The before-fix filter
was `regional_deadline,long_bodies_tight_admission,queued_long_read,stalled_admission`
with seed 7. The native sweep command is the README example with seeds `7,42`.

## Regressions reproduced and fixed

These rows use identical scenario configurations and seed 7 on both revisions.

| Scenario | Before completion | After completion | Cause and fix |
|---|---:|---:|---|
| Regional, two stalled copies, healthy 150 ms third, 500 ms page deadline | 595/600 | **600/600** | Preference changed between reservation and launch. A 150 ms copy received a 20 ms remainder. Bind each reservation to its destination. |
| Two healthy 4 s reads, one admission slot, 5 s operation / 10 s page limit | 1/2 | **2/2** | Admission waiting consumed the operation timeout. Start that timeout after admission; keep the original page deadline. |
| Healthy 4 s bodies, eight global admission slots, 10 arrivals/s | 8/60 | **16/60** | The same fix stops prematurely canceling admitted reads. Capacity and FIFO waiting still prevent full completion. Wire attempts fall from 164 to 70. |
| Two stalled primaries occupy both admission slots | 0/2 | **0/2** | No capacity remains to run the healthy third before the deadline. This is a retained counterexample. |

The regional failure occurred during a changing preference, after healthy traffic
and concurrent stalls. A small test starting directly in the failed state passed
before the fix; retaining the transition was necessary to catch it. The fast CI
checks now include that full transition and the queued healthy-read case.

## Availability and latency

The table aggregates three exact-retention seeds. Completion always uses all
scheduled arrivals, including failures. Fault-window counts are shown separately
where the campaign includes healthy periods.

| Scenario | Completion | Interpretation |
|---|---:|---|
| Independent 10% errors on all three copies, entire run | **89,910/90,000 (99.9%)** | 90 failures versus the model expectation of 90 (`0.1³ × 90,000`); 99,905 wire attempts. This is a statistical comparison, not a guarantee. |
| Fully correlated faults | 1,728/1,800 | All 72 failures occur among 900 fault-window arrivals. Another copy cannot repair a fault shared by all copies. |
| First two copies return errors | 1,800/1,800 | The third copy remains usable. |
| First two copies stall in headers or bodies | 1,800/1,800 in each case | Some initial reads take **5.006 s** despite a healthy 6 ms copy. Availability within the 10 s page budget does not imply fast failover for every read. |
| Two stalled regions; third needs 150 ms, deadline 500 ms | 1,800/1,800 | Maximum read latency is 412 ms after the reservation fix. |
| Healthy distant copy needs 150 ms, deadline 100 ms; near copies each fail 10% | 1,792/1,800 | Eight failures among 900 fault-window arrivals. The distant copy cannot provide availability inside this deadline. |
| Preferred copy recovers while both peers become unavailable | 3,000/3,000 | Recovery remains possible, but the transition reaches **9.983 s** against a 10 s deadline. |

Healthy zonal and regional fixtures always choose the first bucket, completing in
3 ms and 10 ms respectively. When the local service time rises from 3 to 20 ms,
selection moves to the 5 ms peer; across the whole scenario the mean is 4.77 ms,
p99 5 ms, and maximum 20 ms. After a single failure at low traffic, the preferred
bucket returns to healthy status only after 20 clean completions; for seed 7 this
is observed at 40.003 s. Idle time alone does not restore health.

All healthy 4 s body reads finish with normal admission capacity. Their deterministic
completion time exactly coincides with a rescue timer, producing 120 transport
attempts for 60 reads: the extra copies start and cancel at the same virtual instant.
This exposes possible request overhead despite zero extra modeled service time.
Likewise, the quantized healthy-jitter fixture spends the entire 5% early-hedge
allowance without improving its 3 ms p99. These are useful budget checks, not evidence
that real S3 or Capacitor latency distributions have the same tie behavior.

## Capacity, retries, and cancellation cost

At 250 arrivals/s with three service slots and 50 ms service during the degraded
phase, seed 7 completes 1,806/3,000 overall. There are 383 completions among 1,500
fault-window arrivals; cohort-based work can finish after the fault interval ends.
With the production reservoir, increasing offered load to 500 and 1,000 arrivals/s
reduces overall completion to 3,117/6,000 and 4,319/12,000. Service concurrency stays
at three and the combined waiting queue at 24. Both native seeds give those counts;
that does not establish general reservoir determinism.

Allowing three SDK attempts with a 10 ms backoff cap changes the 250 arrivals/s
case to **1,807/3,000**, with **3,484 wire attempts instead of 3,056**: 14% more wire
requests for one additional completion. Cachey's copy count is 3,065, so counting
only its attempts would conceal that amplification. The controlled transient-error
fixture separately completes 50 reads using 50 copy operations and 100 SDK attempts;
with early hedges enabled it uses 55 copies and 105 attempts. These backoff settings
are fixture inputs, not a claim about the SDK's production defaults.

In the isolated hedge case, the user gets data after 8 ms while the canceled loser
continues consuming a service slot for another 50 ms. In the finite-queue cancellation
storm, immediate cancellation costs 3.015 service-slot seconds and no capacity
rejections; a 100 ms cancellation delay costs **9.223 service-slot seconds** and
causes **264 capacity rejections**. All 600 logical reads are intentionally canceled,
so this comparison measures resource cost, not availability. Merely dropping the
client future would have hidden this work.

The ordinary finite-capacity scenario has identical results with zero and 100 ms
cancellation delay because accepted work finishes before cancellation matters.
The separate cancellation storm is needed to expose that dimension.

## Relation to the Python exploration

The Rust results reproduce the qualitative findings: a third independent copy
reduces residual errors, correlated failures remain, deadline feasibility matters,
and low resource limits reject legitimate reads. They replace the earlier regional
availability claim with a measured before/after fix. The Rust model additionally
executes actual SDK retries, admission waiting, completion races, and delayed server
cancellation. Python counts and latency deltas are not carried over as Rust results,
and the historical two-copy routing implementation is not embedded in this harness.

Keep the current bounded policy with these fixes. The experiments justify its
failure handling and identify limits; they do not justify an adaptive concurrency
controller, more ranking constants, or a universal availability claim. A future
Capacitor backend should reuse the scenario contracts while exercising its own
transport and retry implementation.
