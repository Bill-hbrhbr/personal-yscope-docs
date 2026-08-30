# RFC: CLP Spider TDL package for compression

Status: descriptive RFC for the current implementation

This RFC defines the Spider compression TDL contract implemented by CLP. Its
primary purpose is to make the task boundary explicit: the Spider-visible task
names and signatures, the serialized representation of every input and output,
the exact use of each field inside a task, and the destination and consumer of
every output. The current Rust implementation is described rather than
redesigned.

## Current scope summary

The implemented graph has the following scope:

- **CLP-S only.** Compression is performed by `clp-s`.
- **S3 input only.** Each compression task receives a partition of S3 object
  keys. Filesystem inputs are not part of this task contract.
- **S3 archive output only.** Produced archives are staged locally, indexed,
  and uploaded to the configured S3 archive store.
- **One dataset per graph.** Every compression task in a job receives the same
  `Option<String>` dataset value.
- **Map and commit.** Archive-producing tasks return metadata through Spider;
  a termination task publishes that metadata to MySQL and marks the CLP
  compression job successful.

## 1. Context and baseline architecture

The compression coordinator prepares the work for one CLP compression job.
It resolves the requested S3 object-metadata rows, partitions those objects
into task-sized `S3InputSource` values, and asks its Spider-backed submitter to
construct the task graph.

```text
CompressionCoordinator / CompressionJobHandle
  -> S3CompressionJobSubmitter / SpiderClient
    -> Spider task graph
      -> N x compression::clp_s_s3_compress
      -> compression::commit
```

Spider schedules one compression task for each prepared input partition. Each
task creates and uploads one or more archives and returns their metadata to the
graph. After all compression tasks succeed, Spider invokes
`compression::commit` with access to all graph outputs. The commit task writes
the archive metadata and successful job state to MySQL in one transaction.

The coordinator constructs the graph but does not call the TDL functions as
ordinary Rust functions. It observes the final Spider state; it does not
receive or publish individual `CompressionTaskOutput` values.

## 2. Purpose of this RFC

This RFC specifies the complete interface of each compression TDL task. For
every task input, it states the Rust and Spider type, the component that
produces it, any validation or defaulting, and exactly how the task uses it.
For every output, it states how the value is constructed, whether it remains
inside the Spider graph or is written to persistent storage, and which
component consumes it.

The specification should be sufficient to reconstruct the graph descriptors,
serialize compatible task payloads, and implement the TDL wrappers without
inferring the contract from coordinator code.

## 3. TDL contract goals

The compression TDL package must provide:

- Stable Spider-visible names and Rust signatures for the archive-producing
  and commit tasks.
- Serializable wire types shared by the coordinator-side graph builder and
  the TDL package under `clp_rust_utils::task_io::compression`.
- Deterministic use of every serialized field in the receiving task.
- An explicit distinction between graph values, worker configuration, secrets,
  native-process side effects, S3 output, and MySQL output.
- Enough archive metadata for the commit task to publish every archive without
  asking the coordinator to reconstruct native-task results.
- Atomic publication of archive metadata and the successful CLP compression
  job state.

## 4. Requirements

- The package MUST register `compression::clp_s_s3_compress` and
  `compression::commit` under the `clp` TDL package.
- The compression node MUST declare three value inputs, in the order specified
  in Section 6.1, and one `CompressionTaskOutput` value output.
- The graph builder MUST serialize value inputs with MessagePack using
  `rmp_serde::to_vec`; the TDL package MUST deserialize the same Serde types.
- Shared task types MUST live in or be re-exported from
  `clp_rust_utils::task_io::compression`.
- The task wrappers MUST live under
  `components/clp-tdl-package/src/task/compression/` and be registered by the
  package task list.
- `compression::commit` MUST run as the graph termination task and consume the
  graph's `CompressionTaskOutput` values through `TaskContext`.
- Deployment-wide output configuration and MySQL credentials MUST come from
  worker configuration or secrets rather than being copied into every graph
  input.

## 5. Constraints and assumptions

- The coordinator and TDL worker compile separately. Task names, Spider type
  descriptors, argument order, and Serde representations must therefore remain
  synchronized.
- Every graph contains at least one compression node, and every input
  partition is expected to contain at least one S3 object key.
- All compression outputs in one graph must carry the same `dataset` value.
- A missing dataset means the CLP-S default dataset. The original
  `Option<String>` is preserved in graph values; resolution to the default
  occurs where a concrete dataset name or path is required.
- The configured archive output must be S3-backed. There is no filesystem
  fallback for this task.
- Persisted MySQL state is limited to externally observable or
  fault-tolerance-relevant facts. Attempt-local execution phases and temporary
  paths remain worker state, avoiding unnecessary MySQL updates.

## 6. Proposed design

The implemented graph has `N` independent compression nodes and one
termination task:

```text
compression::clp_s_s3_compress(partition 1) --+
compression::clp_s_s3_compress(partition 2) --+--> compression::commit
...                                           |
compression::clp_s_s3_compress(partition N) --+
```

For every compression node, `SpiderClient` installs this descriptor contract:

```text
package: "clp"
task_func: "compression::clp_s_s3_compress"
inputs:
  0: Value("ClpSCompressionOption")
  1: Value("Option<String>")
  2: Value("S3InputSource")
outputs:
  0: Value("CompressionTaskOutput")
```

The corresponding three `TaskInput::ValuePayload` values are serialized in
that exact order. `TaskContext` is supplied by Spider and is not an element of
the serialized input vector.

The termination descriptor names `compression::commit` and contains no
ordinary serialized arguments. Spider makes the completed compression-node
outputs available through its termination-task `TaskContext`.

Execution policy is graph metadata, not a TDL argument. The coordinator
currently assigns compression-task retries from configuration and scales the
timeouts by partition size: three minutes per object for the soft timeout and
five minutes per object for the hard timeout. The commit task has
`max_num_instances = 1`; its configured defaults are one retry and 45-second
soft / 60-second hard timeouts.

### Shared wire types

The task signatures use these MessagePack-serialized definitions from
`clp_rust_utils::task_io::compression`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClpSCompressionOption {
    pub target_encoded_size: u64,
    pub compression_level: u8,
    pub timestamp_key: Option<String>,
    pub unstructured: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3InputSource {
    pub endpoint_url: Option<NonEmptyString>,
    pub region_code: Option<NonEmptyString>,
    pub bucket: NonEmptyString,
    pub aws_authentication: AwsAuthentication,
    pub object_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionTaskOutput {
    pub dataset: Option<String>,
    pub archives: Vec<ArchiveMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveMetadata {
    pub id: String,
    pub begin_timestamp: i64,
    pub end_timestamp: i64,
    pub size: i64,
    pub uncompressed_size: i64,
}
```

`ClpSCompressionOption` contains job-wide native compression options and is
copied into every compression node. `S3InputSource` is the per-node object
partition. `CompressionTaskOutput` is a graph-internal result consumed only by
`compression::commit`; it is not returned to the compression coordinator.

### 6.1 `compression::clp_s_s3_compress`

#### 6.1.1 Signature and Spider descriptors

```rust
#[task(name = "compression::clp_s_s3_compress")]
pub(crate) fn s3_compress_task(
    ctx: TaskContext,
    clp_s_option: ClpSCompressionOption,
    dataset: Option<String>,
    input_source: S3InputSource,
) -> Result<CompressionTaskOutput, TdlError>;
```

One invocation processes exactly one coordinator-prepared S3 partition. Its
serialized input descriptors and payload order are:

| Position | Descriptor | Rust argument |
| ---: | --- | --- |
| 0 | `Value("ClpSCompressionOption")` | `clp_s_option` |
| 1 | `Value("Option<String>")` | `dataset` |
| 2 | `Value("S3InputSource")` | `input_source` |

Its only output descriptor is `Value("CompressionTaskOutput")`. Returning
`Ok(output)` gives Spider one serialized graph value. Returning
`Err(TdlError)` fails the node and prevents the termination task from
publishing the job.

#### 6.1.2 Inputs and exact uses

| Input | Producer and validation | Exact use inside the task |
| --- | --- | --- |
| `ctx.job_id` | Spider | Included in tracing and in attempt-local filenames. It is the Spider job ID, not the CLP compression-job ID. |
| `ctx.task_id` | Spider | Included in tracing and attempt-local filenames so concurrent graph nodes do not collide. |
| `ctx.task_instance_id` | Spider | Included in attempt-local filenames so retries do not reuse another attempt's temporary list or conversion directory. |
| `clp_s_option.target_encoded_size` | Compression job configuration | Converted to decimal and passed to `clp-s --target-encoded-size`. The wrapper performs no additional range validation. |
| `clp_s_option.compression_level` | Compression job configuration | Converted to decimal and passed to `clp-s --compression-level`. The wrapper relies on the validated job configuration and native binary for accepted values. |
| `clp_s_option.timestamp_key` | Compression job configuration | For structured input, `Some(key)` adds `--timestamp-key <key>` and `None` omits it. For unstructured input this field is ignored because `log-converter` emits the fixed key `timestamp`. |
| `clp_s_option.unstructured` | Compression job configuration | `false` selects direct structured S3 ingestion by `clp-s`; `true` runs `log-converter` first and compresses the converted local directory. |
| `dataset` | Compression job configuration; the same value is copied to every graph node | Selects the resolved local archive-staging directory, output S3 key prefix, indexer dataset/table, and the `dataset` field returned in `CompressionTaskOutput`. `None` resolves to the default dataset when a concrete name is needed. |
| `input_source.endpoint_url` | Coordinator, from the ingestion job's S3 configuration | When present, used with the region, bucket, and object key to generate every input S3 URL. `NonEmptyString` prevents an explicitly empty endpoint. |
| `input_source.region_code` | Coordinator, from the ingestion job's S3 configuration | Used to generate S3 URLs and select the input credential-provider region. When absent, credential resolution uses CLP's default AWS region. |
| `input_source.bucket` | Coordinator, from validated S3 object metadata | Used as the bucket component of every input S3 URL. `NonEmptyString` rejects an empty bucket before serialization. |
| `input_source.aws_authentication` | Coordinator, from the ingestion job's S3 configuration | Resolves the AWS access key, secret, and optional session token injected into `clp-s` or `log-converter`. This input may represent explicit credentials or the default AWS provider chain. |
| `input_source.object_keys` | Coordinator partitioner | Each key becomes one line in the temporary `--files-from` URL list. The task rejects an empty individual key. The coordinator guarantees a non-empty vector. |

The CLP compression-job ID, Spider resource-group ID, and execution policy are
not TDL arguments. The CLP job ID is used by the coordinator for logging and
lifecycle management; Spider supplies its own job ID in `TaskContext`.

#### 6.1.3 Worker configuration and execution

The task also consumes process-wide worker inputs:

| Source | Value | Use |
| --- | --- | --- |
| `CLP_HOME` | CLP installation path | Locates `clp-s`, `log-converter`, and `indexer`. |
| `SpiderTaskExecutorConfig.tmp_directory` | Temporary root | Stores the S3 URL list and, for unstructured input, converted files. |
| `SpiderTaskExecutorConfig.archive_output` | Staging path and S3 output configuration | Selects the local archive directory and the destination bucket/key for uploads. The task fails if this output is filesystem-backed. |
| `SpiderTaskExecutorConfig.database` | MySQL endpoint, database, and table prefix | Supplies `indexer` arguments for each produced archive. |
| Archive-output S3 authentication | Worker configuration | Creates the S3 client used to upload archives. It is independent of the input authentication carried by `S3InputSource`. |

The worker performs this pipeline:

```text
S3InputSource.object_keys
  -> newline-delimited temporary S3 URL list
  -> structured input:
       clp-s reads the URL list directly with S3 authentication
     unstructured input:
       log-converter reads the URL list and writes a local converted directory
       clp-s compresses that local directory
  -> clp-s emits one JSON archive-stat record per archive
  -> parse each record as ArchiveMetadata
  -> for each archive, concurrently:
       run indexer against the staged archive
       upload the staged archive to configured S3 output
  -> wait for clp-s and every archive finisher
  -> return CompressionTaskOutput
```

Both branches invoke `clp-s` with `--print-archive-stats`,
`--single-file-archive`, `--target-encoded-size`, and
`--compression-level`. The structured branch adds `--auth s3` and
`--files-from`; the unstructured branch adds `--timestamp-key timestamp` and
passes the converted directory positionally.

The task constructs subprocess arguments without a shell. It drains `clp-s`
stderr while parsing stdout and kills and reaps the process when output parsing
or an archive callback fails. Any input preparation, conversion, native
process, JSON parsing, indexing, or upload failure becomes
`TdlError::ExecutionError`. Attempt-local temporary paths are removed on scope
exit; cleanup errors are logged without replacing the primary task result.

#### 6.1.4 Output construction and consumers

The task constructs:

```rust
CompressionTaskOutput {
    dataset,
    archives,
}
```

`dataset` is the exact `Option<String>` received by the task. `archives`
contains one `ArchiveMetadata` for every archive-stat JSON line emitted by
`clp-s`, after that archive has been indexed and uploaded successfully.
Unknown fields in the native JSON record are ignored by Serde; missing,
malformed, or incorrectly typed required fields fail the task.

| Output field | Construction | Consumer and use |
| --- | --- | --- |
| `dataset` | Copied unchanged from the task input | `compression::commit` verifies that every node output has the same dataset, resolves the default when needed, registers the dataset, and selects its archive metadata table. |
| `archives` | Accumulates successfully parsed native archive-stat records | `compression::commit` flattens all node vectors into the full archive set. An individual vector may be empty, but the graph-wide flattened set must not be empty. |
| `archives[].id` | Parsed from `clp-s --print-archive-stats` | Names the staged and uploaded archive and becomes the MySQL archive row's `id`. |
| `archives[].begin_timestamp` | Parsed from native archive stats as a signed Unix epoch timestamp in milliseconds | Inserted into the archive metadata table for time-range archive selection. |
| `archives[].end_timestamp` | Parsed from native archive stats as a signed Unix epoch timestamp in milliseconds | Inserted into the archive metadata table for time-range archive selection. |
| `archives[].size` | Parsed compressed archive size in bytes | Inserted into the archive row and summed into `compression_jobs.compressed_size`. |
| `archives[].uncompressed_size` | Parsed uncompressed input size in bytes | Inserted into the archive row and summed into `compression_jobs.uncompressed_size`. |

In addition to its returned graph value, a successful invocation has two
persistent side effects:

- Every archive named by `archives[].id` has been uploaded to the configured
  S3 archive-output bucket and dataset key prefix.
- `indexer` has populated the dataset's MySQL column-metadata table from every
  produced archive so later CLP-S queries can resolve column names and types.

These side effects occur before the task returns and are not Spider output
values. The archive-row metadata itself remains internal to the graph until
`compression::commit` publishes it.

### 6.2 `compression::commit`

#### 6.2.1 Signature and graph input

```rust
#[task(name = "compression::commit")]
pub(crate) fn commit_task(ctx: TaskContext) -> Result<(), TdlError>;
```

The function has no ordinary serialized arguments. It must run as Spider's
termination task; otherwise `ctx.get_task_graph_outputs()` has no output set
and the function fails. Spider supplies one serialized
`CompressionTaskOutput` for each successfully completed compression node, and
the wrapper deserializes all of them with `rmp_serde::from_slice`.

| Input | Producer | Exact use |
| --- | --- | --- |
| `ctx.job_id` | Spider | Reverse-looks up `compression_jobs.spider_id`, identifies and locks the CLP compression-job row, and supplies tracing context. |
| `ctx.get_task_graph_outputs()` | Spider termination-task context | Supplies every serialized `CompressionTaskOutput` returned by the compression nodes. The wrapper deserializes them, verifies one common dataset value, and flattens their archive vectors. |

The task obtains the MySQL host, port, CLP database name, and table prefix from
`SpiderTaskExecutorConfig.database`. It reads `CLP_DB_USER` and `CLP_DB_PASS`
from the worker environment. Those deployment secrets are not graph inputs.

#### 6.2.2 Validation and transaction

Before publishing anything, the task:

1. Fails if any graph output cannot be deserialized as
   `CompressionTaskOutput`.
2. Fails if the outputs do not all carry exactly the same `dataset` value.
3. Flattens every `archives` vector and fails if the graph-wide set is empty.
4. Validates a present dataset against the allowed dataset-name pattern before
   using it to derive a table name. `None` resolves to the default dataset.

It then performs one MySQL transaction:

```text
SELECT compression job WHERE spider_id = ctx.job_id FOR UPDATE
  -> no row: fail
  -> status SUCCEEDED: return idempotent success
  -> status other than RUNNING: fail
  -> status RUNNING:
       upsert dataset registration
       insert every archive metadata row in chunks of 1,000
       CAS UPDATE compression_jobs RUNNING -> SUCCEEDED
         + total compressed size
         + total uncompressed size
         + duration derived from the MySQL clock
       require exactly one updated job row
       COMMIT
```

Dataset registration, archive insertion, and the successful job-state
transition therefore become visible atomically. Re-execution after the row is
already `SUCCEEDED` is a no-op success. Any other database or validation error
returns `TdlError::ExecutionError` and prevents Spider from declaring the graph
successful.

#### 6.2.3 Outputs and their consumers

**Returned task output:** `()`. `Ok(())` tells Spider that the termination task
and graph completed successfully. There is no value output for another graph
node or for the coordinator.

**Persistent output:** MySQL receives the dataset registration, one metadata
row for every produced archive, and a `compression_jobs` row containing
`SUCCEEDED`, total sizes, and duration.

**Coordinator-visible outcome:** the compression job handle observes Spider's
terminal state. On Spider success, the handle treats the MySQL publication as
already completed by `compression::commit`. On failure or cancellation, it
checks for an already committed `SUCCEEDED` row before recording a failure or
killed state, preserving a commit that succeeded even if Spider failed to
report it cleanly.
