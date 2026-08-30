# Search coordinator — MVP PR implementation plan

This plan splits the search-coordinator MVP into reviewable PRs. It uses the
`search-coordinator/init` branch as a prototype, but does not assume that the
prototype's commits are the right merge boundaries. Each PR should leave the
repository buildable and include tests for the behavior it introduces.

The authoritative scope is Section 4 of `search-coordinator.md`. The
underscore-prefixed legacy appendix in that document is reference material and
must not expand the MVP accidentally.

## MVP contract and ownership

The MVP handles only `SEARCH_OR_AGGREGATION` jobs whose `aggregation_config` is
absent. The coordinator owns orchestration, Spider owns task execution, and
clp-s writes search results directly to the per-job results-cache collection.

```text
SQL query job
  -> search coordinator: categorize, enumerate archives, submit, and poll
  -> Spider graph: one clp-s search task per planned archive
  -> clp-s: write results directly to results-cache collection <job_id>
  -> Spider search::commit task: transactionally mark SQL job SUCCEEDED
  -> search coordinator: verify success or record graph failure
```

Like compression, the graph has a TDL termination task. Per-archive search tasks
write results through clp-s and return a small completion payload;
`search::commit` consumes those graph outputs, looks up the CLP query job by the
Spider ID, and transactionally changes `RUNNING` to `SUCCEEDED`. The coordinator
owns dispatch and failure reporting and verifies the committed status after a
successful graph.

The following constraints are deliberate:

- No aggregation, cancellation, decompression, reducer, or `KILLED` handling.
- No `query_tasks` writes or per-task progress. `num_tasks` remains the MVP
  placeholder described by the design.
- No network, file, or stdout result handlers; results-cache output only.
- `search::commit` uses the same row lock and `RUNNING`-to-`SUCCEEDED` CAS as
  `compression::commit`. Coordinator failure handling first preserves an
  already-committed success.
- A single coordinator deployment and an explicit legacy-scheduler cutover are
  required so two unguarded writers cannot own the same plain-search row.
- Persist the minimum lifecycle status and supporting columns needed by
  external consumers and fault-tolerant recovery. Keep reconstructible phases
  such as graph construction, Spider polling, and commit verification local to
  `QueryJobHandle`; do not expand `QueryJobStatus` or write MySQL on every local
  phase change. This reduces unnecessary updates and row contention. Add a
  durable field only when it closes a crash-recovery ambiguity, as `spider_id`
  does for reattachment and duplicate-submission prevention.

## Starting point: `search-coordinator/init`

The prototype already contains:

- A `search-coordinator` workspace crate, binary entry point, configuration
  loading, database setup, signal handling, and package-image wiring.
- Matching Python and Rust `SearchCoordinator` configuration models and a
  validator requiring Spider when the coordinator is enabled.
- Query-job schema additions (`status_msg`, `update_time`, `spider_id`, and
  `dispatch_time`) and typed Rust query-job IDs, statuses, types, and search
  configuration.
- A coordinator poll loop, two-phase pending-job fetch, deferred
  `dispatch_time` updates, a concurrency semaphore, resource-group setup, and
  initial recovery of running Spider jobs.
- `QueryJobHandle`, SQL helpers, a submitter abstraction, idempotent Spider
  start, and terminal-state polling with exponential backoff.
- Helm values and ConfigMap rendering for `search_coordinator`.

These pieces are scaffolding, not a working search path. Known gaps are:

- Pending-job fetching selects only `id`, so it cannot categorize or plan a
  job.
- Handle construction does not read/deserialise the plain MessagePack
  `job_config`; its dataset is hardcoded to `None`.
- `SpiderClient::submit_query_job` is still `todo!()`.
- `num_tasks` is hardcoded. This is an accepted MVP placeholder, but it should
  be named/documented rather than mistaken for real progress accounting.
- No `search::commit` TDL task exists, so successful Spider completion cannot
  transactionally update the SQL job to `SUCCEEDED` with duration.
- Terminal handling does not yet verify a successful commit or preserve it when
  Spider subsequently reports a failure.
- `schedule_new_jobs` builds `dispatched_job_ids` before handle construction,
  so a rejected row can receive `dispatch_time` without being dispatched.
- Recovered jobs do not consume semaphore permits, and spawned handles are not
  tracked for orderly shutdown.
- The schema edit affects newly created tables only; `CREATE TABLE IF NOT
  EXISTS` does not add columns to existing deployments.
- Coordinator, handle, and submitter tests are missing.
- Deployment is incomplete: Helm renders configuration but has no Deployment
  or scheduling/resources; Compose and package configuration do not expose or
  start the service.

## Phase 1: reusable coordinator skeleton

### PR 1.1 — Query schema and shared Rust types

Add the query-job columns and indices required for Spider coordination, plus
typed `QueryJobId`, `QueryJobStatus`, `QueryJobType`, `SearchJobConfig`, and
`AggregationConfig` definitions.

For every proposed status or column, document which external observer or crash
recovery decision requires it. Do not persist a handle-local execution phase
that can be reconstructed from the existing durable row and Spider state.

Explicitly choose and test an upgrade strategy:

- Add an idempotent migration for existing `query_jobs` tables; or
- State that MVP supports fresh databases only and track the migration before
  an upgrade-capable release.

Updating only the `CREATE TABLE IF NOT EXISTS` body is not a migration.

### PR 1.2 — Crate, configuration, and binary scaffold

Add the workspace crate and binary, Python/Rust configuration models,
Spider-required validation, database credential loading, and signal-driven
startup/shutdown. Include the binary in the package image.

Test default and invalid values, Python/Rust field parity, and the requirement
that `search_coordinator` cannot be enabled without Spider. Keep separately
named retry and timeout policies for the per-archive search task and the
`search::commit` termination task; correct the prototype's wiring so commit
timeouts are not used as search-task timeouts.

### PR 1.3 — Job handle and SQL lifecycle helpers

Introduce `QueryJobHandle` and database helpers to:

- Persist the Spider job ID and set `RUNNING`, `start_time`, the documented MVP
  `num_tasks` placeholder, and `dispatch_time` if absent.
- Read the current SQL state before deciding the next transition.
- On terminal Spider success, verify that `search::commit` already wrote
  `SUCCEEDED`; on failure, write `FAILED`, duration, and `status_msg` only if a
  successful commit did not already win.
- Report setup and polling failures without overwriting an already-terminal
  state.

The success transition belongs to `search::commit` and uses a row lock plus a
`RUNNING`-to-`SUCCEEDED` CAS. Tests should exercise repeated completion checks,
already-terminal jobs, oversized task counts, duration/status messages, and
best-effort failure reporting.
Unexpected Spider cancellation is a failure for MVP; it does not introduce
`KILLED` or coordinator-driven cancellation semantics.

### PR 1.4 — Submitter abstraction and Spider completion polling

Add `QueryJobSubmitter`, `QueryJobOutcome`, and
`run_query_job_to_completion`, keeping graph construction out of this PR.

Unit-test new and idempotently already-started jobs, exponential-backoff limits,
successful/failed/unexpected-cancelled terminal states, and failures to fetch
Spider state or its error message.

### PR 1.5 — Coordinator core and entry point

Wire the poll loop, configurable cadence, cancellation-token shutdown,
resource-group setup, two-phase fetch, semaphore permit ownership, deferred
dispatch marking, handle spawning, and binary entry point.

Correct the prototype by adding a job to `dispatched_job_ids` only after it is
accepted and can be dispatched, and by tracking handles for defined graceful
shutdown. Test fetch limits, first-phase rows, permit release, rejected jobs,
dispatch marking, and shutdown with an injected submitter and test database.

**Phase 1 merge order:** `1.1` and `1.2` can begin in parallel; `1.3` depends
on `1.1`; `1.4` is independent; `1.5` combines them.

## Phase 2: plain-search data path

### PR 2 — Job categorization and archive planning

Widen the pending-row projection to include `type`, `job_config`, and
`creation_time`. Decode `job_config` as plain MessagePack before claiming it:

- Accept only `SEARCH_OR_AGGREGATION` without `aggregation_config`.
- Return aggregation, `EXTRACT_IR`, and `EXTRACT_JSON` as unsupported so later
  phases or the legacy scheduler retain ownership.
- Mark malformed configs and invalid otherwise-supported jobs `FAILED` with a
  useful status message.

Port the minimum archive selection needed by plain search: dataset selection,
time-range filtering, retention cutoff, deterministic/newest-first ordering,
and any required archive batching. Produce an in-memory per-archive Spider plan
but do **not** create `query_tasks` rows or track `num_tasks_completed`.

Specify non-overlapping ownership with the legacy scheduler. Unsupported rows
must not be claimed, while accepted plain-search rows must not also be consumed
by the legacy path. Test mixed types, malformed configs, filters, and empty
archive sets.

### PR 3 — `clp-tdl-package` search and commit tasks

Follow the existing compression task's package/runtime structure:

- Add serializable per-archive search inputs/options under
  `clp-rust-utils::task_io::search`, plus a minimal `SearchTaskOutput` that
  echoes the archive identity for termination-task validation. It carries no
  search hits; clp-s writes those directly to MongoDB.
- Add `task/search/{mod.rs,search.rs,commit.rs}`: keep the `#[task]` wrappers
  thin and put testable implementations in `search.rs` and `commit.rs`.
- Register `search::search` and `search::commit` in
  `clp-tdl-package/src/lib.rs` and document both in the package README.
- Reuse `common.rs` for the process-global Tokio runtime, executor config,
  `CLP_HOME`, and JSON stderr tracing.

The task runs one clp-s archive search using only the results-cache output path.
It must build arguments without a shell, drain stderr while consuming any
stdout protocol, terminate/reap clp-s after protocol or callback errors, and
clean temporary files. It must not update `query_tasks` or finalize the SQL job.

The `search::commit` termination task receives the graph outputs through
`TaskContext`, validates/deserializes them, and uses `ctx.job_id` to find the CLP
query job by `spider_id`. Using DB configuration from
`SpiderTaskExecutorConfig` and credentials from `CLP_DB_USER`/`CLP_DB_PASS`, it
locks the job row and transactionally CAS-transitions `RUNNING` to `SUCCEEDED`
with a DB-clock-derived duration. It is idempotent when the row is already
`SUCCEEDED`; it neither writes `query_tasks` nor touches MongoDB.

Test argument construction, plain search options, malformed output, subprocess
failure, process cleanup, results-cache configuration, output validation,
commit idempotency, wrong-state refusal, rollback, and DB credential loading.
Validate that the release build places both registered TDL functions in the
Spider worker library at the path Spider loads.

### PR 4 — Implement `submit_query_job`

Replace the submitter's `todo!()` with Spider graph construction and job
registration. Mirror the useful portions of `submit_s3_compression_job`: keep
task-name constants synchronized with the TDL package, use typed inputs,
serialize value inputs with MessagePack, and create one search task per planned
archive. Attach `search::commit` as the graph's termination task and apply its
own retry and soft/hard timeout policy.

Persist the Spider ID and transition the query job to `RUNNING` only after
registration succeeds. Define retry/recovery behavior for the window between
Spider registration and SQL persistence so retries do not silently create
duplicate work. Explicitly define zero-archive behavior.

### PR 5 — End-to-end MVP lifecycle

Complete and test the exact MVP path:

```text
PENDING SQL job
  -> coordinator categorizes and enumerates archives
  -> coordinator registers/starts the Spider search + commit graph
  -> clp-s tasks write results to MongoDB collection <job_id>
  -> search::commit transactionally writes SUCCEEDED and duration
  -> coordinator verifies SUCCEEDED or writes FAILED and status_msg
```

Exercise empty searches, partial task failure, unexpected Spider cancellation,
SQL/commit failure, duplicate/retried submission, max-results behavior, and
restart at each persistence boundary. Assert that a coordinator-side failure
cannot overwrite committed success and that no `query_tasks` rows or
non-results-cache output paths are used.

### PR 6 — Startup recovery and shutdown hardening

Complete the MVP recovery contract before deployment:

- Reattach to `RUNNING` rows with a Spider ID without resubmitting.
- Re-dispatch accepted `PENDING` rows with `dispatch_time` but no Spider ID.
- Make recovered running jobs consume semaphore capacity, or document and test
  another bounded policy.
- Make commit and terminal handling idempotent if clp-s wrote results before
  restart or `search::commit` succeeded before the coordinator restarted.
- Track in-flight handles and define graceful-stop versus forced-abort behavior
  within `termination_timeout_secs`.
- Add a durable status/column if testing exposes a restart boundary that cannot
  be resolved from `status`, `spider_id`, timestamps, and Spider state; otherwise
  keep the phase in memory to avoid another MySQL update.

Recovery never kills orphaned work and does not add cancellation support.

### PR 7 — Helm, Kubernetes, Docker, and package integration *(later step)*

Make the completed coordinator deployable without changing the chart version
on the MVP branch.

Helm/Kubernetes:

- Keep MVP-only `search_coordinator` defaults in
  `tools/deployment/package-helm/values.yaml` and render the full block from
  `templates/configmap.yaml` only when Spider is enabled. Render distinct search
  and commit task execution policies.
- Add a search-coordinator Deployment modeled on the compression coordinator,
  including DB credentials, DB/Spider-storage readiness init containers,
  ConfigMap mount, `RUST_LOG`, telemetry environment, termination grace,
  scheduling, and resources.
- Fix replicas at one and use a rollout strategy that prevents overlap (for
  example `Recreate` or `maxSurge: 0`) because MVP deliberately has no CAS or
  leader election.
- Add `searchCoordinator` values under Helm `scheduling` and `resources`, plus
  a Helm-only log-level value if used by the Deployment.
- Do not increment the Helm chart version on this MVP branch.

Docker/package integration:

- Verify the package image contains `/opt/clp/bin/search-coordinator` and the
  generated config contains the matching section.
- Add the service to the Spider Compose deployment(s), with the package config
  mount, DB credentials, Spider settings/readiness, telemetry environment, and
  shutdown grace consistent with Helm.
- Ensure the Spider-worker image build depends on `clp-tdl-package` and copies
  the library containing the registered search and commit tasks to Spider's
  configured TDL package directory. Supply its config plus `CLP_DB_USER` and
  `CLP_DB_PASS`, since `search::commit` opens the MySQL connection itself.
- Wire package-controller/config-template enablement so the coordinator is
  started only with Spider and the legacy scheduler no longer claims supported
  plain-search jobs.

Validate with `helm lint`, rendered-manifest/config assertions, a single-writer
rollout assertion, `docker compose config`, release binary/TDL-library checks,
and container/Kubernetes startup smoke tests.

## MVP requirement-to-PR check

| Section 4 requirement | Planned PR |
|---|---|
| Schema additions | 1.1 |
| Configurable poll loop and shutdown | 1.2, 1.5 |
| Two-phase fetch and bounded concurrency | 1.5 |
| Categorization and MessagePack config | 2 |
| Archive planning and results-cache-only search task | 2, 3 |
| Spider graph submission and ID persistence | 4 |
| Transactional `search::commit` success | 3, 4, 5 |
| Coordinator failure reporting and commit verification | 1.3, 5 |
| Terminal status, duration, and status message | 1.3, 3, 5 |
| Startup recovery | 6 |
| Helm/Kubernetes and Docker/package integration | 7 |

## Dependency graph

```text
1.1 -> 1.3 --+
1.2 ---------+-> 1.5 -> 2 --+
1.4 ---------+              +-> 4 -> 5 -> 6 -> 7
1.2 -> 3 -------------------+
```

PR 3 can proceed beside PR 2 after agreeing on the per-archive input contract;
PR 4 is their integration point. Recovery is an MVP requirement, and deployment
is intentionally last so its smoke tests exercise the complete lifecycle.

## Deferred phases

- **MVP+1 — cancellation:** poll `CANCELLING`, request Spider cancellation,
  and define `CANCELLED` lifecycle semantics.
- **MVP+2 — timeline aggregation:** retain clp-s per-archive bucketing and use
  the results cache for the cross-archive reduction; do not port the reducer.
- **MVP+3 — decompression:** design Spider tasks and resource-group isolation
  for `EXTRACT_IR` and `EXTRACT_JSON`.
- **Post-MVP observability:** add `query_tasks` bookkeeping and accurate
  `num_tasks`/`num_tasks_completed` only after its schema and ownership model
  are deliberately designed.
- **MVP+N — other aggregations:** depend on future Spider functionality and
  remain outside this plan.
