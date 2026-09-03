# RFC: MVP Spider TDL package for `clp-s` queries

This RFC defines the normative Spider graph and TDL worker contract for MVP `clp-s` queries.
Shared architecture and terminology are described in
[Query system architecture](query-system-architecture.md). Query-job state transitions, recovery, and
failure reporting are defined in the [query-job-handler RFC](query-job-handler-rfc.md).

## 1. Scope and requirements

The MVP defines one Spider-visible task:

```text
query::clp_s_search
```

The task searches one archive and writes its matches directly through `clp-s`'s results-cache
output handler. The graph has one independent node per selected archive and no join or commit task.
The task returns no application payload; Spider observes only node success or failure.

The implementation must satisfy these requirements:

- The annotated wrapper and implementation live under
  `components/clp-tdl-package/src/task/query/` and the package registers the task.
- Query-specific serialized types use Serde and live in, or are re-exported from,
  `clp_rust_utils::task_io::query`.
- The coordinator-side graph builder uses the task name, descriptors, argument order, and
  MessagePack representation specified here.
- Native query results go directly to MongoDB; they do not pass through Spider outputs.
- Repeated execution follows Zhihao Lin's
  [results-cache deduplication design](other-authors/zhihao/mongodb-deduplication.md) and is
  idempotent by logical result identity.
- Every configuration, process, archive-access, or non-duplicate results-cache failure becomes
  `TdlError::ExecutionError`.
- Timestamp bounds remain signed Unix epoch milliseconds from the wire type through the `clp-s`
  arguments.

Aggregation, query cancellation, and file or network output handlers are outside this RFC.

## 2. Task graph

Each prepared `(ArchiveMetadata, ExecutionPolicy)` pair becomes one `query::clp_s_search` node. The
submitter uses `ArchiveMetadata` to construct the node's task inputs and attaches the paired policy
to `TaskDescriptor.execution_policy`. Archive compressed size and the policy are not serialized as
TDL inputs.

All archive nodes belong to one graph. Spider reports graph success only after every node succeeds;
one node exhausting its attempts fails the graph. The handler consumes this terminal graph outcome
as specified by the handler RFC.

### 2.1 Execution policy

The initial MVP policy is:

| Task | `max_num_instances` | `max_num_retry` | Soft / hard timeout |
| --- | ---: | ---: | ---: |
| `query::clp_s_search` | 2 | 1 | 600 s / 1,200 s |

Retry and timeout values come from coordinator configuration rather than constants in the TDL
function. The graph builder expresses timeouts in milliseconds. The coordinator rejects a policy
whose hard timeout is not strictly greater than its soft timeout.

A soft timeout allows Spider to start a replacement instance while the original may still be
running. `max_num_instances = 2` limits execution to the original plus one concurrent replacement;
`max_num_retry = 1` permits one additional attempt. Result-cache deduplication makes these repeated
executions converge on the same logical result set.

## 3. Shared wire types

The task uses `QueryJobId` from `clp_rust_utils::job_config`:

```rust
pub type QueryJobId = i32;
```

It uses these MessagePack-serialized query types from `clp_rust_utils::task_io::query`:

```rust
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClpSQueryOption {
    pub query_string: NonEmptyString,
    pub max_num_results: Option<NonZeroU32>,
    pub begin_timestamp_millisecs: Option<i64>,
    pub end_timestamp_millisecs: Option<i64>,
    pub ignore_case: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum OutputHandle {
    // The task implementation adds the MVP results-cache variant.
}
```

`ClpSQueryOption` contains job-wide query behavior and is copied unchanged into every archive node.
`OutputHandle` is the job-wide result destination and remains separate because it controls where
results are written rather than query matching. `dataset` and `archive_id` are per-node archive
context and must not be folded into `ClpSQueryOption`.

`max_num_results`, when present, is nonzero and applies independently to each archive invocation.
When absent, the task omits `--max-num-results` and uses the `clp-s` default. Timestamp bounds are
inclusive; the coordinator rejects a begin timestamp greater than the end timestamp.
`NonEmptyString` prevents empty query strings, dataset names, and archive IDs from crossing the task
boundary, while the coordinator remains responsible for stricter validation.

The ownership and current compatibility issue surrounding the persisted `0` result-limit sentinel
are documented in [Query task configuration ownership](query-task-configuration-ownership.md).

There is no query equivalent of `CompressionTaskOutput`. Query results are already durable in
MongoDB, and no downstream graph node consumes an archive task output.

## 4. `query::clp_s_search`

### 4.1 Signature

```rust
#[task(name = "query::clp_s_search")]
pub(crate) fn clp_s_search_task(
    ctx: TaskContext,
    query_job_id: QueryJobId,
    clp_s_query_option: ClpSQueryOption,
    dataset: Option<NonEmptyString>,
    archive_id: ArchiveId,
    output_handle: OutputHandle,
) -> Result<(), TdlError>;
```

The task executes exactly one query against one selected archive. A missing dataset resolves to the
default dataset before constructing the archive locator and result metadata.

### 4.2 Inputs and exact uses

| Input | Producer | Exact use |
| --- | --- | --- |
| `ctx` | Spider | Supplies Spider job, task, and task-instance identities for tracing and error context. It does not replace the CLP query-job ID. |
| `query_job_id` | Coordinator | Converted to decimal and used as the results-cache collection name. |
| `dataset` | Archive planning | Resolves the filesystem or S3 archive location and becomes the nonempty dataset recorded with every result. `None` means the default dataset. |
| `archive_id` | Archive planning | Selects the archive, identifies it in logs and result documents, and contributes to deterministic result identity. |
| `query_string` | Query-job configuration | Passed as the positional `clp-s` query without reinterpretation. |
| `max_num_results` | Query-job configuration | Adds `results-cache --max-num-results <n>` when present; omitted otherwise. |
| `begin_timestamp_millisecs` | Query-job configuration | Adds the inclusive `--tge <milliseconds>` bound when present. |
| `end_timestamp_millisecs` | Query-job configuration | Adds the inclusive `--tle <milliseconds>` bound when present. |
| `ignore_case` | Query-job configuration | Adds `--ignore-case` when true. |
| `output_handle` | Coordinator | Selects the supported `clp-s` output-handler subcommand and supplies its destination configuration. |

Deployment-wide `CLP_HOME`, archive storage, results-cache connection information, and credentials
come from worker configuration or secrets, not from repeated task inputs. For S3 storage, the task
resolves credentials and injects them into the child environment.

For filesystem archives, the results-cache command shape is:

```text
<CLP_HOME>/bin/clp-s s <archive-root>/<dataset>
    --archive-id <archive-id>
    <query-string>
    [--tge <begin-ms>]
    [--tle <end-ms>]
    [--ignore-case]
    results-cache
    --uri <results-cache-uri>
    --collection <query-job-id>
    [--max-num-results <n>]
    --dataset <dataset>
```

For S3 archives, the locator becomes:

```text
<CLP_HOME>/bin/clp-s s <s3-url-for-key-prefix/dataset/archive-id> --auth s3
```

The query and results-cache arguments remain unchanged. The implementation constructs the argument
vector without a shell, waits for the child, and drains its standard streams. Zero matching log
events is a successful task execution.

## 5. Outputs and persistent effects

The returned value is `()`. `Ok(())` tells Spider the archive node completed successfully; no graph
node consumes an application output.

`clp-s` writes one MongoDB document per retained match into collection `<query_job_id>`. Each result
contains the deterministic identity required by the results-cache deduplication design, plus its
message, timestamp, archive ID, archive-local log-event index, and resolved dataset. Neither the
query coordinator nor the handler reads or rewrites these documents during completion.

The Spider graph state is the only coordinator-visible task outcome. SQL completion behavior
belongs to the handler RFC.

## 6. Failure propagation and retry safety

- `clp-s` exits successfully only after completing the results-cache write. A query with zero
  matches and a correctly handled duplicate-only replay are successful.
- Configuration, credentials, archive-location construction, process spawning or waiting,
  nonzero exit, and non-duplicate MongoDB failures become `TdlError::ExecutionError` with query-job,
  dataset, and archive context.
- The wrapper never converts a logged error into `Ok(())` and never returns success before the child
  exits.
- The child process must not outlive its Spider task instance. Normal error handling terminates and
  reaps it; the deployment must ensure a hard timeout terminates the child with the worker instance.
- A failed attempt may leave partial MongoDB results. A retry or soft-timeout replacement relies on
  deterministic result identity to converge without duplicate logical results.
- When all allowed instances of a node fail, Spider fails the graph. The handler performs the SQL
  transition defined by the handler RFC.
