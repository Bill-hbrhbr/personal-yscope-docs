# MVP query worker execution overview

Companion overview by Bingran Hu.

- [Zhihao Lin's search-task implementation design](other-authors/zhihao/search-task-implementation-planning-and-design.md) — worker internals, CLI construction, and implementation rationale.
- [Zhihao Lin's MongoDB deduplication design](other-authors/zhihao/mongodb-deduplication.md) — deterministic result identity and retry behavior.
- [Query TDL RFC](query-tdl-rfc.md) — coordinator-to-worker graph and wire contract.
- [PR #2512](https://github.com/y-scope/clp/pull/2512) — opened search-task implementation.

## Execution boundary

QueryJobSubmitter builds and registers the graph. Spider schedules its archive nodes and invokes
the registered `query::clp_s_search` task. The worker task does not construct or submit a Spider graph.

For each archive, the task:

- Receives job identity, matching options, dataset/archive identity, and the output handle.
- Resolves archive access from worker configuration, including filesystem or S3 location.
- Resolves `None` dataset to the default dataset.
- Constructs clp-s arguments without a shell and supplies required child-process credentials.
- Waits for clp-s and reports execution success or failure to Spider.

clp-s writes matches directly to MongoDB. The task neither finalizes MySQL job status nor returns
result documents as graph outputs. Job-level collection/index preparation belongs to the coordinator.

## Reading the linked design alongside the PR

Zhihao's document is preserved as written. It includes planning decisions and historical details
that should not be silently substituted for the current shared contract.

One concrete difference: its settled-decisions section describes `max_num_results: None` as passing
`u32::MAX`. The inspected #2512 implementation omits the flag, matching the current TDL RFC.
[Configuration ownership](query-task-configuration-ownership.md) records the remaining producer
compatibility issue; this overview does not change either author's underlying design.

#2512 adds `OutputHandle::ResultsCache { uri }` and a reserved `File` variant; only results-cache
execution is supported. Worker implementation and graph submission remain separate roadmap items.
