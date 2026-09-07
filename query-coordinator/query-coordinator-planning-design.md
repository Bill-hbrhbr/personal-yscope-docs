# MVP query coordinator admission and archive planning

- [MVP design](query-mvp-design.md) — supported behavior.
- [Job-handler RFC](query-job-handler-rfc.md) — handoff and durable lifecycle.
- [Configuration ownership](query-task-configuration-ownership.md) — defaults and compatibility.

## Boundary

QueryCoordinator owns preparation before QueryJobHandle and coordination across handles.
This document describes the MVP target, not an already implemented coordinator.

## Admission

Read pending job identity, type, configuration, and the timestamps required for planning.
Decode the plain MessagePack job configuration before constructing task inputs.

- Accept only `SEARCH_OR_AGGREGATION` without aggregation configuration.
- Do not claim unsupported aggregation or extraction jobs.
- Fail malformed configurations and invalid otherwise-supported searches with a useful message.
- Validate query text, dataset names/existence, and timestamp ordering.
- Enforce a defined ownership boundary with the legacy scheduler.

Resolve product policy upstream. In particular, do not invent a meaning for a persisted result limit
of `0` while converting it to an optional nonzero task argument.

## Archive selection

Apply requested datasets, time-range overlap, and retention eligibility to archive metadata.
Retain the dataset/archive association; archive identity alone must not lose dataset context.
Preserve the intended deterministic/newest-first ordering when porting selection behavior.

Produce `Vec<(ArchiveMetadata, ExecutionPolicy)>`:

- `ArchiveMetadata`: archive ID, optional dataset, and compressed size.
- `ExecutionPolicy`: the corresponding task's retry, concurrency, and timeout settings.

No per-archive MySQL task rows are created. Empty selection is a successful no-work path, not an
empty Spider graph; complete it directly as specified in the MVP design.

## Prepared submission inputs

For a nonempty selection, prepare:

- Query-job identity and Spider resource-group identity.
- Job-wide `ClpSQueryOption`, preserving resolved matching options.
- `OutputHandle::ResultsCache { uri }`, identifying the results-cache database.
- Selected archive metadata paired with execution policies.

The results-cache URI is an explicit task input. Worker configuration supplies archive storage and
credentials; the task must not replace the supplied result destination with an independent default.
Follow the deployment's MongoDB connection contract when constructing the URI.

Prepare the per-job result collection and required indexes before dispatch, separately from
job completion. This work belongs above the worker task; it does not require reading or rewriting
individual search-result documents.

Pass prepared inputs to the handler. The submitter constructs and registers the actual task graph;
the coordinator does not serialize task descriptors itself.

## Service integration

Bound concurrent jobs, track handler lifetime, and release capacity when handlers finish.
At startup, discover recoverable running rows and account for recovered jobs in the same bound.
Shutdown stops admission and follows a defined handler shutdown policy; it must not pretend that
dropping a local future cancels a Spider job.

Resource-group setup, polling cadence, configuration loading, signal handling, binary packaging,
and deployment ownership are service-level work, not features supplied by a handler alone.

## Acceptance scenarios

- Supported search, unsupported job types, malformed MessagePack, and invalid query settings.
- Dataset filtering, time overlap, retention cutoff, ordering, and zero eligible archives.
- Correct task inputs and per-archive policy; no task-row writes.
- Result-limit compatibility and output-destination construction.
- Collection/index preparation failure before execution.
- Concurrency bounds, recovery discovery, duplicate ownership, and shutdown.

The [roadmap](query-coordinator-pr-plan.md) tracks remaining integration and interface reconciliation.
