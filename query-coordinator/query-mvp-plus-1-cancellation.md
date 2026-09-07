# Query coordinator MVP+1: cancellation

- [MVP design](query-mvp-design.md) — preceding generation.
- [Implementation roadmap](query-coordinator-pr-plan.md) — cancellation delivery work.

## Added

Support user-requested cancellation of query jobs. Existing producers express requests through
`CANCELLING` query-job rows; the coordinator forwards cancellation to Spider and observes the outcome.
Stopping a local handler is not sufficient to stop distributed work.

## Changed from MVP

- Discover cancellation requests in addition to pending and recoverable running jobs.
- Distinguish requested cancellation from an unexpected cancelled Spider graph.
- Persist `CANCELLED` when the cancellation contract is satisfied rather than mapping every
  cancelled graph to `FAILED`.
- Recover cancellation intent across coordinator restarts.

## Removed

Remove the MVP exclusion of user-requested cancellation. Do not introduce a query commit task or
revive the legacy startup practice of killing orphaned jobs.

## Unchanged

Archive planning, plain-search task inputs, direct results-cache writes, and ordinary successful and
failed job handling retain their MVP responsibilities unless the detailed cancellation design
requires an explicit change.

## Decisions required before implementation

Define cancellation before registration, between registration and durable persistence, during
execution, and after completion. Specify which state wins in completion/cancellation races, how
Spider cancellation reaches native child processes, and how partial cached results are presented.
Do not infer these policies from stopping a polling coroutine.

When implementation begins, append MVP+1 changes to each affected component design and record
"no change" for unaffected components. This delta document is not a claim that those details or
cancellation support are already implemented.
