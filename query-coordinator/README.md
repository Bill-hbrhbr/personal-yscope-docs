# Query coordinator documents

## General design

- [System architecture](query-system-architecture.md) — components, responsibilities, and package interactions.
- [Configuration ownership](query-task-configuration-ownership.md) — defaults and policy across layers.

## MVP design

- [MVP](query-mvp-design.md) — detailed plain-search baseline.
- [MVP+1: cancellation](query-mvp-plus-1-cancellation.md) — additions, removals, and changes from MVP.
- [MVP+2: timeline aggregation](query-mvp-plus-2-aggregation.md) — additions, removals, and changes from MVP+1.

## Detailed component designs

- [Coordinator admission and archive planning](query-coordinator-planning-design.md) — query inputs and prepared dataset/archive work.
- [Job-handler RFC](query-job-handler-rfc.md) — durable lifecycle, state transitions, and recovery.
- [TDL RFC](query-tdl-rfc.md) — Spider graph and serialized task contract.
- [Worker execution overview](query-worker-execution-overview.md) — execution boundary and links to Zhihao's worker design.

These designs currently describe MVP. When implementation of a later generation begins, append its
changes to each component design, or record "no change"; do not overwrite the preceding baseline.

## Implementation roadmap

- [Implementation roadmap](query-coordinator-pr-plan.md) — MVP PR coverage and remaining work, followed by MVP+1 and MVP+2 delivery sections.

## Archives

- [Celery worker behavior](archive/query-celery-tasks.md) — legacy orchestration and execution reference.
- [Query task analysis](archive/query-task-analysis.md) — historical inputs, side effects, and migration research.
- [Original coordinator design](archive/query-coordinator.md) — superseded system design and unpolished notes.
- [Previous TDL RFC](archive/query-tdl-rfc-previous.md) — earlier search-task proposal.
- [Previous implementation roadmap](archive/query-coordinator-pr-plan.md) — prototype inventory and earlier PR splits.

Archived content is historical, including any obsolete commit-task or lifecycle proposals.
Useful material not yet incorporated into the polished designs remains available here; it does not
override current contracts.
