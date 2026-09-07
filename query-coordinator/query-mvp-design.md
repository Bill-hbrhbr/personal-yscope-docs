# Query coordinator MVP design

- [System architecture](query-system-architecture.md) — components and package interactions.
- [Implementation roadmap](query-coordinator-pr-plan.md) — PR coverage and remaining delivery work.
- [MVP+1: cancellation](query-mvp-plus-1-cancellation.md) — changes after the MVP.

## Supported behavior

MVP supports plain searches over archives produced by the clp-s storage engine. Accepted jobs have
type `SEARCH_OR_AGGREGATION` and no aggregation configuration.

- Select archives using requested datasets, query time bounds, and retention eligibility.
- Run one `query::clp_s_search` task per selected archive.
- Write matches directly to MongoDB collection `<query_job_id>` through results-cache output.
- Complete the MySQL job after observing the Spider graph outcome.
- Recover durably submitted jobs after a coordinator restart.

Aggregation, user-requested cancellation, extraction/decompression, reducers, and file/network
output are outside the MVP. A wire type may reserve an output variant without making it supported.

## Admission and archive planning

[Coordinator planning](query-coordinator-planning-design.md) defines request decoding and preparation.

- Malformed configurations and invalid otherwise-supported searches fail with a useful message.
- Unsupported job categories are not claimed by this coordinator. Deployment must define their
  owner and prevent the legacy scheduler from also claiming supported searches.
- An explicitly empty dataset list or a requested dataset that does not exist is invalid.
- A valid query selecting no archives succeeds without creating a handle or Spider job.
- A query that searches archives but finds no log matches follows normal execution and succeeds
  when its graph succeeds.

For zero selected archives, atomically transition `PENDING` to `SUCCEEDED`, record the current
`start_time`, and set `num_tasks = 0` and `duration = 0`. If the guarded update affects no row,
leave the other owner's state unchanged.

## Nonempty-job lifecycle

```text
PENDING query_jobs row
  -> coordinator validates and selects dataset/archive pairs
  -> coordinator prepares options, output handle, and per-archive policies
  -> handler asks submitter to register the graph
  -> handler persists spider_id, RUNNING, start_time, and num_tasks
  -> submitter starts and polls Spider
  -> archive tasks execute clp-s and write matches to MongoDB
  -> Spider reports graph success or failure
  -> handler persists SUCCEEDED or FAILED
```

No query commit or termination task exists. `QueryJobHandle` owns both successful and failed terminal
MySQL updates. Register before persisting the Spider ID; start only after that ID is durable.
[Job-handler RFC](query-job-handler-rfc.md) defines guarded transitions and failure boundaries.

## Task and result contract

Each node receives the query-job ID, job-wide query options, optional dataset, archive ID, and output
handle. Archive size and execution policy remain coordinator-side metadata.

[TDL RFC](query-tdl-rfc.md) defines serialization, argument order, and initial task policy.
[Worker execution overview](query-worker-execution-overview.md) links the worker implementation design.

- Task retries and concurrent replacements rely on deterministic result identity.
- A failed attempt can leave partial results; those results do not make the job successful.
- Tasks return execution success or failure, not result documents.
- No `query_tasks` rows or per-task progress counters are written.
- `num_tasks` records selected archive count, not a placeholder or completed-task count.
- A positive result limit applies per archive invocation, not as a coordinator-enforced global
  early-stop threshold. Persisted legacy `0` values require the compatibility decision recorded in
  [configuration ownership](query-task-configuration-ownership.md).

## Recovery and failures

Persist only externally useful lifecycle information and the identity needed to recover work;
do not persist every handle-local phase.

- Recover `RUNNING` jobs with a stored Spider ID by observing that graph, never by rebuilding it.
- Preserve terminal SQL outcomes during repeated completion attempts.
- Leave durable running jobs recoverable after observation or terminal-persistence errors.
- Treat unexpected Spider cancellation as failure; explicit cancellation belongs to MVP+1.
- Registration without a durably persisted Spider ID can leave an unstarted orphan graph.
  Do not hide this ambiguity with automatic resubmission.

Coordinator startup discovery, concurrency accounting, and shutdown wiring must be integrated;
a handle-level `recover` method alone does not complete system recovery.

## Delivery boundary

MVP delivery requires end-to-end wiring, result-destination preparation, a database upgrade strategy,
worker archive access, and a deployment cutover with a single effective owner for accepted jobs.
Opened foundational PRs do not by themselves establish a runnable coordinator.

Acceptance scenarios include nonempty success, no selected archives, no matching events, malformed
input, unsupported types, retries, task failure, SQL failure, and restart at persistence boundaries.
The roadmap tracks implementation and verification separately from this behavioral contract.
