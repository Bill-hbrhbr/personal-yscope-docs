# Query system architecture and component interactions

This document provides shared, non-normative context for the query coordinator RFCs:

- The [query-job-handler RFC](query-job-handler-rfc.md) defines job-lifecycle behavior.
- The [query TDL RFC](query-tdl-rfc.md) defines the Spider graph and worker task contract.
- [Query task configuration ownership](query-task-configuration-ownership.md) records the
  cross-layer rules for defaults and optional task settings.

## MVP scope

The MVP supports plain `clp-s` searches whose results are written through the results-cache output
handler:

- Only archives produced by the `clp-s` storage engine are supported.
- Query results are written directly to MongoDB collection `<query_job_id>`.
- Aggregation, reducers, extraction jobs, and non-results-cache output handlers are outside scope.
- User-requested cancellation is deferred to MVP+1. An unexpected Spider cancellation is treated
  as a query-job failure in the MVP.
- The query graph has no commit task. The query-job handler owns the terminal MySQL update after
  Spider reports the graph outcome.

## Components and responsibilities

### `QueryCoordinator`

The coordinator owns work spanning multiple query jobs:

- Poll and categorize pending MySQL query-job rows.
- Validate the query-job configuration.
- Select eligible archives using the requested datasets and time range, plus the archive-retention
  cutoff.
- Construct job-wide query options and the output handle.
- Construct one Spider `ExecutionPolicy` for each selected archive.
- Enforce the coordinator-wide concurrency limit.
- Create and run a `QueryJobHandle` only when at least one archive was selected.
- Discover recoverable `RUNNING` rows and reattach handlers to their Spider jobs.

### `QueryJobHandle`

One handle owns the durable lifecycle of one nonempty, already-planned query job:

- Ask `QueryJobSubmitter` to register the graph with Spider.
- Persist the Spider job ID and transition the query job from `PENDING` to `RUNNING`.
- Ask the submitter to idempotently start and monitor the Spider job.
- Translate Spider's terminal graph outcome into the terminal MySQL query-job status.
- Resume monitoring an already-submitted job during coordinator recovery.

The handle does not categorize query jobs, select archives, construct per-archive policies, or
construct Spider task descriptors.

### `QueryJobSubmitter`

The query coordinator owns this local adapter interface. It separates lifecycle and SQL behavior
from Spider graph construction:

- `submit_query_job` translates prepared query inputs into a Spider `TaskGraph`, serializes the TDL
  inputs, and registers the graph.
- `run_query_job_to_completion` idempotently starts the Spider job, polls its state, and translates
  the terminal state and error into a query-job outcome.

Production implements the interface for `SpiderClient`. Tests may use a controlled fake submitter.
The TDL package does not depend on this interface or on `SpiderClient`.

### Spider and the CLP TDL package

Spider schedules the graph, applies each node's retry, concurrency, and timeout policy, and exposes
the graph outcome. The CLP TDL package supplies `query::clp_s_search`, which runs one archive query
on a Spider worker. The task writes results directly to MongoDB and returns only execution success
or failure to Spider.

## End-to-end flow

For a query with one or more eligible archives:

```text
PENDING query_jobs row
  -> QueryCoordinator validates and selects archives
  -> QueryCoordinator prepares query inputs and per-archive execution policies
  -> QueryJobHandle asks QueryJobSubmitter to register the Spider graph
  -> QueryJobHandle persists spider_id and RUNNING
  -> QueryJobSubmitter starts and polls the Spider job
  -> Spider executes one query::clp_s_search node per archive
  -> each node writes matches directly to MongoDB collection <query_job_id>
  -> Spider reports the terminal graph outcome
  -> QueryJobHandle persists SUCCEEDED or FAILED in MySQL
```

The control plane flows through MySQL, the coordinator, and Spider. Search results do not return
through the task graph: they flow directly from each `clp-s` process to MongoDB.

## Archive planning and the zero-archive path

The coordinator distinguishes invalid input from a valid query that has no work:

- An explicitly empty dataset list is invalid and fails the query job.
- A requested dataset that does not exist is invalid and fails the query job.
- A valid query whose dataset, time-range, and retention filters select no archives succeeds
  without creating a handler or Spider job.
- A query that searches archives but finds no matching log events runs normally and succeeds after
  its Spider graph completes.

For the zero-archive case, the coordinator atomically transitions the row from `PENDING` to
`SUCCEEDED`, records the current `start_time`, and sets `num_tasks = 0` and `duration = 0`. This
matches the legacy Celery scheduler. Failure of the compare-and-set means another owner has already
claimed or completed the row; the coordinator must not overwrite that state.

## Task inputs versus graph metadata

The coordinator passes the following serialized inputs to every archive task:

- The query-job ID.
- The job-wide `ClpSQueryOption`.
- The archive's optional dataset and archive ID.
- The job-wide `OutputHandle`.

Archive compressed size and Spider `ExecutionPolicy` remain coordinator-side graph-construction
metadata. They are not TDL arguments. Each selected archive is paired with its execution policy so
planning may account for archive characteristics. `ExecutionPolicy::max_num_retry` is the task's
retry budget; polling backoff is a separate handler concern.

## Durable state and recovery boundary

MySQL is the durable CLP control plane. The MVP persists only state needed by external consumers or
recovery:

- `status` records `PENDING`, `RUNNING`, or a terminal result.
- `spider_id` allows a restarted coordinator to reattach to a submitted job.
- `start_time`, `duration`, `num_tasks`, and `status_msg` expose lifecycle information.

Spider is authoritative for the submitted graph's execution state. MongoDB is authoritative for
the query-result documents already written by the archive tasks. Neither the coordinator nor the
handler reads or rewrites those result documents while completing the job.
