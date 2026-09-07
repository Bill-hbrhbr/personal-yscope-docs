# Query coordinator MVP+2: timeline aggregation

- [MVP+1: cancellation](query-mvp-plus-1-cancellation.md) — preceding generation.
- [Implementation roadmap](query-coordinator-pr-plan.md) — aggregation delivery work.

## Added

Support timeline aggregation. clp-s performs per-archive count-by-time bucketing; MongoDB performs
the cross-archive reduction. Arbitrary aggregations are not included in this generation.

## Changed from MVP+1

- Extend accepted query behavior and task/result contracts for timeline buckets.
- Prepare and execute the results-cache reduction and expose the resulting timeline.
- Update producer and web UI integration for the proposed single-job flow instead of paired
  search and aggregation jobs.
- Apply cancellation to the extended workflow, including any new work after archive execution.

## Removed

Remove the plain-search-only restriction for timeline queries. Do not carry over the Celery reducer
process or its coordinator-side connection management. The proposed producer integration removes
the separate paired aggregation-job submission; this remains implementation work, not current behavior.

## Unchanged

The coordinator orchestrates work rather than performing search or aggregation itself.
Spider schedules worker execution. Durable job lifecycle ownership remains with the coordinator
and handler; no query commit task is introduced.

## Decisions required before implementation

Specify bucket identity and retry-safe writes, reduction ownership and timing, result schema,
completion criteria, cancellation races, and producer compatibility/cutover. The precise single-job
API and graph shape remain to be designed; do not infer them from the historical RFC.

When implementation begins, append MVP+2 changes to component designs relative to MVP+1, recording
"no change" where appropriate. Other aggregations and decompression remain outside this roadmap's
three defined generations.
