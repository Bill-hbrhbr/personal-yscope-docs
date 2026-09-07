# Query system architecture and component interactions

- [Configuration ownership](query-task-configuration-ownership.md) — defaults and policy across package boundaries.
- [MVP design](query-mvp-design.md) — supported behavior and system-wide constraints.
- [Coordinator planning](query-coordinator-planning-design.md) — admission and archive preparation.
- [Job-handler RFC](query-job-handler-rfc.md) — durable lifecycle and recovery.
- [TDL RFC](query-tdl-rfc.md) — graph and task contracts.
- [Implementation roadmap](query-coordinator-pr-plan.md) — delivery sequence, PR coverage, and remaining work.

## Components and responsibilities

### Query producers

API server, web UI, and other producers translate requests into durable query-job configurations.
They own user-facing policy and defaults and consume job status and search results.

### QueryCoordinator

Coordinates work across query jobs:

- Discover and categorize pending jobs in MySQL.
- Validate job configuration and select eligible dataset/archive pairs.
- Prepare job-wide query options, result destinations, and per-archive execution policies.
- Bound concurrent jobs and manage handler lifetime.
- Complete valid queries with no selected archives without dispatching work.
- Discover recoverable jobs and reattach their handlers to Spider.

### QueryJobHandle

Owns one submitted job's durable lifecycle:

- Register prepared work through the submitter.
- Persist the Spider job identity and running state.
- Monitor execution through the submitter.
- Persist the terminal MySQL outcome.
- Resume observation after coordinator recovery.

Archive selection, graph construction, and result processing are outside the handle.

### QueryJobSubmitter

Adapts the coordinator's prepared inputs to Spider:

- Construct task descriptors, serialize task inputs, and register the graph.
- Start and observe the job, translating Spider states into query-job outcomes.

This interface belongs to the coordinator crate. Production uses a SpiderClient implementation;
controlled implementations can isolate lifecycle behavior during testing.

### Spider

Owns distributed scheduling, task-instance execution, retries, concurrency, timeouts, and graph state.
It reports execution outcomes; it does not own CLP query-job status in MySQL.

### CLP TDL package and clp-s

The TDL package registers callable worker tasks and translates their inputs into native execution.
clp-s reads archives and writes results to the selected destination. Task completion communicates
execution success or failure to Spider, not the search-result payload.

## Package interactions

- `query-coordinator` contains coordination, job lifecycle, and the Spider adapter.
- `clp-rust-utils` supplies shared job types, task I/O, and configuration types.
- `clp-tdl-package` consumes the shared task I/O and worker configuration; it does not depend on
  the coordinator's submitter interface.
- Spider client APIs connect the coordinator to scheduling; Spider TDL APIs connect workers to
  registered functions.
- `clp-s` owns native archive search and result-output behavior.

Coordinator-only archive metadata and execution policies are graph-building inputs, not worker
wire arguments. Shared task I/O describes the execution request crossing that boundary.
[Configuration ownership](query-task-configuration-ownership.md) defines which layer resolves defaults.

## Control and result paths

```text
Query producer -> MySQL -> QueryCoordinator -> QueryJobHandle -> QueryJobSubmitter -> Spider
                                                                                      |
                                                                                  TDL task
                                                                                      |
                                                                                    clp-s
                                                                                      |
                                                                                Results cache
```

Completion returns through Spider and the handler to MySQL. Result documents travel directly from
clp-s to the results cache, independently of the control path.

## State ownership

- MySQL owns durable CLP job identity, status, and recovery information.
- Spider owns the registered graph's execution state.
- MongoDB owns cached result documents.

Coordinator and handler completion logic does not read or rewrite search-result documents.
Job-level destination preparation, such as collection/index setup, is separate from completion.
Exact state transitions and generation-specific behavior belong in the MVP and component designs.
