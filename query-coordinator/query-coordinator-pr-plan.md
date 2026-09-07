# Query coordinator implementation roadmap

- [MVP design](query-mvp-design.md) — plain-search behavior.
- [MVP+1: cancellation](query-mvp-plus-1-cancellation.md) — changes after MVP.
- [MVP+2: timeline aggregation](query-mvp-plus-2-aggregation.md) — changes after MVP+1.

This roadmap distinguishes behavioral design, code opened for review, and remaining integration.
PR coverage below reflects the inspected heads on 2026-09-07. All six listed PRs were open and
unmerged; stacked diffs include prerequisite changes and must not be counted as independent delivery.

## MVP

### Opened PRs

| PR | Coverage | Boundary |
| --- | --- | --- |
| [#2503](https://github.com/y-scope/clp/pull/2503) | Query task signatures and shared I/O types | Task body is a placeholder; not graph construction. |
| [#2504](https://github.com/y-scope/clp/pull/2504) | Coordinator crate and submitter interface | Spider graph construction/submission remains a placeholder. |
| [#2508](https://github.com/y-scope/clp/pull/2508) | Shared task utilities for binary paths and S3 credentials | Reusable worker plumbing, not coordination. |
| [#2509](https://github.com/y-scope/clp/pull/2509) | Compound MongoDB result identities and deduplicated writes | Retry-safe result foundation, not job lifecycle. |
| [#2512](https://github.com/y-scope/clp/pull/2512) | Registered clp-s search task, output types, and native execution | Worker implementation; no Spider graph builder or job submission. |
| [#2513](https://github.com/y-scope/clp/pull/2513) | QueryJobHandle, lifecycle SQL, recovery entry point, and Spider start/poll outcomes | Does not supply the coordinator service or implement `submit_query_job`. |

The worker path and handler foundation are opened, but everything below the handler is not complete:
`SpiderClient::submit_query_job` still contains `todo!()` in #2513.

### 1. Reconcile foundational PRs with the MVP contracts

Integrate shared types, worker utilities, deduplication, and lifecycle code without retaining stacked
placeholder versions of the task or output handle.

Known differences from the target job-handler RFC:

- #2513 stores a `QueryPlan` and resource group in the handle and uses `run(self)`; the RFC passes
  submission inputs to `run` and does not require them for recovery.
- Its polling configuration is named `SpiderOption`, rather than the RFC's `SpiderPollingOption`.
- It checks task-count range but does not reject an empty archive vector before submission.
- Pre-running failure reporting allows updates to both `PENDING` and `RUNNING`; the RFC restricts
  this path to `PENDING` and preserves durable running work for recovery.

Keep these as explicit reconciliation work, not silently revised requirements or claims that the
opened PR already matches the RFC. The result-limit difference between Zhihao's planning document
and #2512 is recorded in the [worker overview](query-worker-execution-overview.md).

### 2. Implement graph construction and registration

Replace the submitter placeholder with one `query::clp_s_search` node per prepared archive.

- Serialize the exact task argument order and wire representation from the TDL RFC.
- Attach each archive's execution policy and the appropriate resource group.
- Register without starting; the handler persists the Spider ID before execution starts.
- Do not add a join, query commit, or termination task.
- Keep registration ambiguity visible; do not automatically register replacement graphs after
  uncertain responses.

This work consumes the shared/worker contract and submitter interface. It is distinct from #2512.

### 3. Implement coordinator admission and preparation

[Coordinator planning](query-coordinator-planning-design.md) defines the target boundary.

- Read and decode query configuration; categorize supported jobs before claiming them.
- Validate inputs and prepare dataset/archive pairs using time and retention filters.
- Resolve the persisted result-limit compatibility contract.
- Prepare job-wide options, result destination, collection/index setup, and per-archive policies.
- Complete valid zero-archive queries directly in MySQL.
- Define exclusive ownership with the legacy scheduler.

Graph construction and preparation can be developed against their shared interface, then integrated.

### 4. Wire the coordinator service and durable recovery

Integrate configuration, database credentials, resource-group setup, admission/polling, concurrency,
handler spawning, and binary startup/shutdown.

- Discover running rows with durable Spider IDs and reattach without rebuilding graphs.
- Include recovered jobs in concurrency accounting and track handles during shutdown.
- Preserve running state after observation or terminal-persistence failures.
- Add an existing-database migration or explicitly limit initial deployment to fresh databases.
  Editing `CREATE TABLE IF NOT EXISTS` alone does not upgrade an existing table.
- Keep reconstructible local phases out of the durable schema.

Prototype code in the archived roadmap is reference material, not proof these pieces are integrated
into the six opened PRs.

### 5. Verify and deploy the complete MVP

Exercise successful search, no selected archives, no log matches, invalid configuration, unsupported
categories, retry/deduplication, partial failure, lost SQL transitions, and restart at registration,
persistence, and completion boundaries. Verify result limits and output URI handling end to end.

Complete package/Compose/Helm integration, binary and TDL-library loading, archive mounts or S3
access, worker credentials, readiness, and graceful shutdown. Use one effective coordinator owner
and prevent overlap with the legacy scheduler during cutover. Verify that task hard timeouts do not
leave native child processes running.

These are delivery acceptance checks, not checks performed by this documentation update.

## MVP+1: cancellation

Depends on a working MVP and the cancellation delta design.

1. Settle pending/running/terminal cancellation races and durable request ownership.
2. Implement cancellation discovery and Spider cancellation requests.
3. Extend handler outcome persistence and restart recovery for requested cancellation.
4. Verify native-process termination and partial-result presentation.
5. Append MVP+1 changes to affected component designs; record "no change" for unaffected components.

No PR number is assigned here until the corresponding implementation is opened.

## MVP+2: timeline aggregation

Depends on MVP+1 and a settled timeline contract.

1. Specify bucket identity, retry-safe writes, result schema, and MongoDB reduction.
2. Extend worker/shared types and coordinator preparation for timeline queries.
3. Implement reduction orchestration, completion criteria, and cancellation behavior.
4. Update producer/web UI integration and define the paired-job cutover.
5. Verify multi-archive reduction, retries, failures, recovery, and cancellation.
6. Append MVP+2 component changes relative to MVP+1, or "no change".

Other aggregations and decompression are not implied by this milestone.

## Historical plans

[Previous implementation roadmap](archive/query-coordinator-pr-plan.md) preserves prototype coverage,
earlier PR splits, and deployment notes. It is not the current implementation checklist.
