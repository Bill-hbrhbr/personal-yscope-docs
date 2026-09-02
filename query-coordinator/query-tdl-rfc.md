# RFC: MVP Spider TDL package for CLP-S queries

This RFC defines the normative Spider query TDL contract. Its primary outcome is the final field list for each query task input and output, including every field's Rust type, producer, validation and defaulting rules, exact use inside the task, and consumer. The contract is forward-looking and must be complete enough to implement the TDL functions without inferring behavior from the Python implementation.

## MVP scope summary

The MVP covers only plain CLP-S queries using the results-cache output path:

- **CLP-S only.** The MVP supports archives produced by the CLP-S storage engine and invokes `clp-s` to query them. The legacy CLP storage engine, `clo`, and all other storage engines are outside the MVP scope.
- **Result-cache only.** The MVP will use clp-s's `results-cache` output handler to write query results directly to MongoDB. The task signature reserves an explicit `OutputHandle` argument for selecting the handler, but the current contract leaves that enum empty until the task implementation defines its concrete variant. The `network` and `file` output handlers are outside the MVP scope.
- **No aggregation.** The MVP does not support a reducer or any count/count-by-time/min/max/unique aggregation. An accepted query job has no aggregation configuration.
- **No cancellation.** The MVP does not consume `CANCELLING` query-job rows, request Spider cancellation, or define cancellation behavior for an active query task. `CANCELLED` is reserved for a future phase that adds explicit cancellation support. `KILLED` is removed from the query-job status model.

## 1. Context and background

The proposed query architecture separates job-level orchestration from archive-level execution:

- The query coordinator discovers and categorizes query jobs and plans their archive work. It creates a per-job handle with that prepared plan.
- Spider owns distributed scheduling and execution of the submitted task graph.
- The CLP TDL package supplies the query functions that Spider workers can execute.
- clp-s writes results directly to the per-job results-cache collection.
- After Spider reports that every archive task succeeded, the query job handler marks the query job `SUCCEEDED` in MySQL. There is no query commit task.

### 1.1 Component responsibilities

#### 1.1.1 `QueryCoordinator`

The coordinator owns concerns spanning all query jobs:

- Poll and categorize MySQL query-job rows.
- Select the target archives and build a `QueryPlan`.
- Enforce the global concurrency limit.
- Create a `QueryJobHandle` containing the query job and its prepared plan.
- Provide coordinator-level recovery and fault tolerance.

#### 1.1.2 `QueryJobHandle`

One handle owns the durable lifecycle of one query job:

- Hold the coordinator-prepared `QueryPlan` through submission.
- Ask `QueryJobSubmitter` to submit the graph definition to Spider.
- Persist `spider_id` and `RUNNING` after Spider accepts it.
- Ask the submitter to start and poll the Spider job.
- Ensure that the MySQL query-job row reaches a terminal status after Spider reaches a terminal state. For the MVP, mark it `SUCCEEDED` and record its completed duration when the graph succeeds; mark it `FAILED` with the error when the graph fails or is unexpectedly cancelled. `CANCELLED` is reserved for future explicit cancellation support.

The handle does not select archives and does not construct Spider's graph representation.

#### 1.1.3 `QueryJobSubmitter` trait and its `SpiderClient` implementation

`QueryJobSubmitter` is a trait owned by the query coordinator. It defines the job-submission and terminal-outcome operations that `QueryJobHandle` needs without making the handle construct Spider task descriptors or call the full Spider API.

`SpiderClient` already exposes the generic Spider client API. It does not require or know about `QueryJobSubmitter`. Instead, the query coordinator implements its local `QueryJobSubmitter` trait for `SpiderClient`, adapting that generic API into the query-specific operations required by `QueryJobHandle`:

- `submit_query_job` translates `QueryPlan` into a `TaskGraph`, declares the TDL function names and I/O descriptors, serializes the task inputs, and calls `SpiderClient::submit_job`.
- `run_query_job_to_completion` starts the Spider job when necessary, polls it through `SpiderClient`, and translates its terminal state and error into `QueryJobOutcome`.

This implementation is coordinator-side integration code, not a second client or a worker-side dependency. It references the TDL wire contract but does not directly invoke the Rust TDL functions. The TDL package neither imports nor uses `SpiderClient`; its task functions run later on workers selected by Spider.

#### 1.1.4 Spider

Spider owns distributed graph execution:

- Schedule graph nodes on available workers.
- Run one `query::clp_s_search` task for each planned archive.
- Apply each node's retry, concurrency, and timeout policy.
- Expose the Spider job's state and error to the submitter.

#### 1.1.5 CLP TDL package

The TDL package defines how each graph node executes:

- `query::clp_s_search` interprets one CLP-S dataset/archive input and launches clp-s.
- clp-s writes matches directly to MongoDB collection `<query_job_id>`; the archive task returns no application data and reports only execution success or failure to Spider.

The package does not select archives, construct the job-specific graph, or return query results to the coordinator. It does not update the MySQL `query_jobs` row.

### 1.2 New-job planning and lifecycle

For a new job, the coordinator owns planning and the handle owns the resulting job lifecycle:

```text
PENDING query_jobs row
  -> QueryCoordinator creates QueryPlan
  -> QueryJobHandle asks the submitter to submit the graph to Spider
  -> QueryJobHandle persists spider_id and RUNNING
  -> QueryJobHandle starts and polls the Spider job
  -> Spider reports a terminal graph outcome
  -> QueryJobHandle marks the MySQL query job SUCCEEDED or FAILED
```

## 2. Purpose of this RFC

This RFC defines the Spider task interface required by the MVP query flow. It will determine the required tasks and specify each task's signature, including the complete set of input and output fields. For every input, it will describe who produces it and exactly how it is used when constructing or executing the Spider task graph.

For every output, the RFC will describe how it is produced and where it goes: whether it remains internal to the task graph, is consumed by another task, is uploaded to persistent storage, or is represented in the final Spider job outcome. It will also state how each output or persistent side effect is used by `QueryJobHandle` and `QueryCoordinator`. The resulting specification must make the complete data flow traceable without assuming the number or names of the tasks before the design is complete. Once the RFC is complete, an implementer must be able to construct the task graph and implement every task without consulting an existing implementation or making unstated behavioral assumptions.

## 3. TDL design goals

Given the baseline architecture, the query TDL package must provide:

- The Spider-executable tasks required to perform an MVP query.
- A Spider-visible name and Rust function signature for every task, defined in the TDL package.
- Serializable input and output types under `clp_rust_utils::task_io::query`, shared by the coordinator-side graph builder and the TDL package.
- Deterministic handling and validation of every task input, with each input used only for the behavior assigned to the receiving task.
- Clear output behavior for every task, distinguishing values passed through the Spider graph from data written to MongoDB or other persistent storage.
- Sufficient completion information and persistent side effects for `QueryJobHandle` and `QueryCoordinator` to observe and manage the query job's lifecycle.
- Worker-side task implementations that remain separate from coordinator-side query planning and task-graph construction.
- A contract complete enough for the TDL implementation to be generated and reviewed from this RFC without treating the existing Python implementation as the source of truth.

## 4. Requirements

- Section 6 MUST give the final Spider-visible task names and Rust signatures and, for every input and output, its type, producer, validation/defaulting, exact use, side effect, and consumer.
- Every CLP-S query-specific serialized type in a task signature MUST use Serde and live in or be re-exported from `clp_rust_utils::task_io::query`.
- The annotated task wrappers and implementations MUST live under `components/clp-tdl-package/src/task/query/` and be registered in the package task list in `components/clp-tdl-package/src/lib.rs`.
- The coordinator-side graph builder MUST use the same task names, type descriptors, argument order, and MessagePack representation specified here.
- Native query results MUST go directly to MongoDB; only control-plane success or failure returns through Spider.
- The CLP-S results-cache writer MUST make repeated execution of the same archive query idempotent by following the contract in [Results-cache deduplication](https://app.notion.com/p/3c904e4d9e6b80d68854d02b96aaf267). Section 6.3.4 defines only how the TDL task propagates that writer's outcome to Spider.
- A TDL task MUST return `TdlError::ExecutionError` for every configuration, process, or non-duplicate results-cache failure. It MUST return `Ok(())` only after clp-s exits successfully and completes its writes.
- `begin_timestamp_millisecs` and `end_timestamp_millisecs` MUST use Unix epoch milliseconds across the shared wire type, TDL task, and clp-s `--tge` and `--tle` arguments.

## 5. Constraints and assumptions

- The coordinator and TDL worker compile separately. The coordinator references task names and wire descriptors; it does not import or call the TDL functions as ordinary Rust functions.
- The component responsibility boundaries described in Section 1 apply throughout this design: the coordinator plans archives, the submitter constructs the graph, Spider schedules it, and the TDL package executes individual nodes.
- Deployment-wide configuration and credentials belong to worker configuration or secrets, not repeated task inputs.
- The deterministic result key assumes that `archive_id` is globally unique across every dataset that may contribute to the same query-job collection.
- Persist a MySQL status or supporting column only when an external consumer needs it or it resolves a real restart/fault-tolerance ambiguity. Keep reconstructible execution phases in memory to avoid unnecessary MySQL updates.

## 6. Proposed design

The MVP defines one Spider-visible task function:

```text
query::clp_s_search
```

### 6.1 Task graph

#### 6.1.1 Execution policy

The coordinator-side `QueryJobSubmitter for SpiderClient` implementation attaches execution policy to the graph descriptors; execution policy is not a TDL argument. The initial MVP policies are:

| Task | `max_num_instances` | `max_num_retry` | Soft / hard timeout | Rationale |
| --- | ---: | ---: | ---: | --- |
| `query::clp_s_search` | 2 | 1 | 600 s / 1,200 s | Allows Spider to start at most one replacement instance after a failure or soft timeout. Re-execution is safe only because the results-cache writer follows the idempotency contract in [Results-cache deduplication](https://app.notion.com/p/3c904e4d9e6b80d68854d02b96aaf267). |

The timeout and retry values MUST be coordinator configuration rendered into the deployment configuration rather than constants in the TDL functions. The `SpiderClient` implementation expresses the configured timeout values in milliseconds when constructing `ExecutionPolicy`.

Spider's soft timeout is a replacement threshold, not a graceful-termination signal. When an instance has run for 600 seconds, Spider may enqueue another instance of the same logical archive node while the first instance is still running. `max_num_instances = 2` limits this to the original and one replacement. The 1,200-second hard timeout terminates an individual instance and treats that instance as failed. `max_num_retry = 1` permits at most one additional attempt; after the retry budget is exhausted, the logical node and therefore the graph fail.

The coordinator MUST reject a policy whose hard timeout is not strictly greater than its soft timeout. These initial values preserve the existing query-worker limits; they SHOULD be revisited using observed per-archive runtimes. Different logical archive nodes may run concurrently subject to the Spider resource group.

#### 6.1.2 Archive and dataset preprocessing

Preprocessing happens before any TDL function is invoked:

1. `QueryCoordinator` deserializes and validates `QueryJobConfig` and accepts only jobs for the CLP-S storage engine. It constructs the job-wide `ClpSQueryOption` once. After the TDL implementation defines a concrete results-cache variant, the coordinator will also construct one `OutputHandle` and copy it into every archive task.
2. The coordinator interprets a missing dataset selection as the default dataset, deduplicates and validates explicitly selected datasets, and queries their archive-metadata tables. It may preserve the missing selection as `None` in the task input; `None` is the wire representation of the default dataset. When more than one dataset is selected, it combines the per-dataset `SELECT` statements with `UNION ALL`, includes the dataset name with every selected row, and globally orders the rows by `end_timestamp DESC`.
3. The coordinator applies the query time range and archive-retention cutoff while selecting archives. The resulting in-memory mapping has one `(Option<NonEmptyString>, NonEmptyString)` dataset/archive pair per matching archive. `None` denotes the default dataset; `Some(dataset)` denotes an explicitly named dataset.
4. `QueryCoordinator` gives the prepared inputs to `QueryJobHandle`, which calls the `QueryJobSubmitter` trait. In production, the trait implementation for `SpiderClient` creates one graph node per input and serializes that input as the node's MessagePack payload. The vector itself is not sent to a TDL function.
5. All archive nodes for the query job are registered in one Spider graph; the coordinator does not divide them into sequential dispatch batches. A graph may contain archive tasks for different datasets.
6. Spider reports graph success only after every archive-query node succeeds. `QueryJobHandle` uses this terminal graph state, rather than a termination task output, to update the MySQL query-job row to `SUCCEEDED`. A failed or unexpectedly cancelled Spider graph is updated to `FAILED` in the MVP.

The planning-time SQL `UNION ALL` combines only archive-metadata rows from the selected datasets; it does not combine query results. The coordinator flattens the selected rows into one ordered vector of dataset/archive pairs, and the submitter creates one logical graph node from each pair. A node receives only its optional scalar `dataset` and non-empty `archive_id` alongside the job-wide `ClpSQueryOption` and `OutputHandle`, never the complete vector or a dataset-to-archives map. The result-level union is the per-query MongoDB collection: every node writes to collection `<query_job_id>` and records the resolved dataset name in each result document.

#### 6.1.3 Graph shape

Each `(dataset, archive_id)` entry produced by preprocessing becomes one independent `query::clp_s_search` node. The graph contains no join or commit task. The following diagram shows the graph shape for three archives selected from two datasets:

```mermaid
flowchart LR
    subgraph task_graph["Spider task graph"]
        Q1["query::clp_s_search<br/>dataset-a, archive-1"]
        Q2["query::clp_s_search<br/>dataset-a, archive-2"]
        Q3["query::clp_s_search<br/>dataset-b, archive-3"]
    end

    RC[("MongoDB results-cache<br/>collection = query_job_id")]
    O{"Spider graph outcome"}
    S["QueryJobHandle<br/>sets MySQL status = SUCCEEDED"]
    F["QueryJobHandle<br/>sets MySQL status = FAILED"]

    Q1 -. writes results .-> RC
    Q2 -. writes results .-> RC
    Q3 -. writes results .-> RC

    Q1 --> O
    Q2 --> O
    Q3 --> O
    O -->|all nodes succeed: SUCCEEDED| S
    O -->|any node exhausts its attempts: FAILED| F
```

The solid lines converge on Spider's graph outcome, not on another task node. The dashed lines represent each task's direct results-cache side effect. `QueryJobHandle` converts the terminal Spider graph outcome into the terminal MySQL query-job status. Each task box represents one logical archive node; a retry or soft-timeout replacement is another instance of that same node, not another planned archive node.

The archive-query function returns no application data. Returning `Ok(())` tells Spider that the archive node completed successfully; returning `Err(TdlError)` fails that graph node. The dataset and archive ID remain input and error-context fields rather than being echoed through Spider as output.

### 6.2 Shared task I/O types

The signature in Section 6.3 uses the following query-job identifier from `clp_rust_utils::job_config`:

```rust
pub type QueryJobId = i32;
```

It also uses the following MessagePack-serialized types from `clp_rust_utils::task_io::query`:

```rust
use std::num::NonZeroU32;

use non_empty_string::NonEmptyString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClpSQueryOption {
    pub query_string: NonEmptyString,
    pub max_num_results: Option<NonZeroU32>,
    /// Inclusive `--tge` bound in Unix epoch milliseconds.
    pub begin_timestamp_millisecs: Option<i64>,
    /// Inclusive `--tle` bound in Unix epoch milliseconds.
    pub end_timestamp_millisecs: Option<i64>,
    pub ignore_case: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum OutputHandle {
}
```

`QueryJobId` mirrors the signed MySQL `INT` type of `query_jobs.id`, matching the compression side's `CompressionJobId` pattern. When `max_num_results` is present, its value is non-zero because the clp-s result-cache handler rejects zero. When it is absent, the task omits `--max-num-results` and uses clp-s's default. The task contract does not copy that default value. When both timestamp bounds are present, the coordinator MUST reject a begin timestamp greater than the end timestamp. Timestamp bounds are inclusive Unix epoch milliseconds. `NonEmptyString` prevents an empty query, dataset name, or archive ID from crossing the MessagePack task boundary. It rejects only a zero-length string; the coordinator remains responsible for any stricter query or dataset validation.

`OutputHandle` reserves the shared wire type that will select the clp-s output handler. Its current definition is intentionally empty, so the current contract does not yet define or serialize any output-handler choice. Concrete variants belong to the task implementation PR.

For illustration only, a future definition could contain variants such as:

```rust
pub enum OutputHandle {
    ResultsCache,
    File,
    Network,
}
```

This example is theoretical: it does not register these variants, prescribe their payloads, or bring file and network output into the MVP. The initial implementation is expected to add only the variant needed for the results-cache path. Keeping handler selection outside the task name allows future handlers to be added without renaming the Spider-visible search task.

The relationship to the existing compression wire types is:

| Query type | Role | Compression-side analogue |
| --- | --- | --- |
| `QueryJobId` | Identifies the durable query job and its MongoDB collection. | `CompressionJobId` identifies the durable compression job. |
| `ClpSQueryOption` | Job-wide clp-s query behavior copied unchanged into every archive-task payload. Its fields control how clp-s evaluates the query; it does not identify which archive or dataset a node searches. | `ClpSCompressionOption` contains job-wide native compression options copied into every compression-task payload. |
| `OutputHandle` | Reserved job-wide selection of the clp-s output handler. The current enum has no variants; the task implementation will define the initial results-cache choice. | No analogue. A compression task always writes archives to the configured archive output. |

There is no query-side analogue of `CompressionTaskOutput`. `CompressionTaskOutput` carries archive metadata into `compression::commit`, whereas a query task has no downstream consumer: clp-s has already persisted the results in MongoDB, and Spider needs only the node's success or failure.

#### 6.2.1 Query options versus archive-task context

`ClpSQueryOption`, `OutputHandle`, and the task's `dataset` argument have different scopes and MUST remain separate:

- `ClpSQueryOption` is **job-wide query behavior**. The coordinator constructs it once from the query-job configuration, and the submitter copies the same value into every archive node. Its fields determine what clp-s searches for and how it evaluates the query: query string, result limit, time bounds, and case sensitivity.
- `OutputHandle` is the **reserved job-wide result destination**. Once concrete variants are defined, one value will be constructed and copied into every archive node. It selects where clp-s writes results rather than how clp-s evaluates the query. The current empty enum records this ownership boundary without yet specifying a handler.
- `dataset: Option<NonEmptyString>` and `archive_id: NonEmptyString` are **per-node archive context**. The coordinator obtains them from archive selection, and each graph node receives the pair identifying the one archive that it must search. `None` means the default dataset; `Some(dataset)` names an explicit dataset. Nodes in the same query graph share one `ClpSQueryOption` and one `OutputHandle` but may have different datasets and always have independently selected archive IDs.

`dataset` therefore MUST NOT be added to `ClpSQueryOption`. It does not change query matching semantics; it locates the selected archive and labels that archive's MongoDB result documents. Keeping it as a separate task argument also makes the task payload's three parts explicit:

```text
job-wide behavior:     ClpSQueryOption
job-wide destination:  OutputHandle
per-archive context:   dataset + archive_id
```

For example, a query graph containing `(dataset-a, archive-1)` and `(dataset-b, archive-2)` sends the same `ClpSQueryOption` and `OutputHandle` to both nodes, while each node receives its own `dataset` and `archive_id`. Putting `dataset` inside `ClpSQueryOption` would incorrectly imply that this per-node routing value is a job-wide clp-s query option.

### 6.3 `query::clp_s_search`

#### 6.3.1 Signature

```rust
#[task(name = "query::clp_s_search")]
pub(crate) fn clp_s_search_task(
    ctx: TaskContext,
    query_job_id: QueryJobId,
    clp_s_query_option: ClpSQueryOption,
    dataset: Option<NonEmptyString>,
    archive_id: NonEmptyString,
    output_handle: OutputHandle,
) -> Result<(), TdlError>;
```

The task executes exactly one clp-s query against exactly one archive in one resolved dataset. `clp_s_query_option` is the job-wide search behavior; `dataset` and `archive_id` identify the per-node archive target; `output_handle` selects the clp-s output handler that receives the results. The task resolves `dataset: None` to `default` before constructing the archive locator or clp-s arguments. Different invocations in the same graph reuse the same options and output handle but may use different datasets and archive IDs.

#### 6.3.2 Inputs and exact uses

| Input | Producer | Exact use |
| --- | --- | --- |
| `ctx` | Spider | Supplies Spider job, task, and task-instance identities for tracing and error context. `ctx.job_id` MUST NOT replace the query-job ID. |
| `query_job_id` | `QueryCoordinator`, copied into every archive input for the query job | Converted to its decimal string and passed as `results-cache --collection <query_job_id>`. Every archive task in the graph therefore writes to the same per-query MongoDB collection. |
| `dataset` | `QueryCoordinator`, from the archive selection | Optional, non-empty per-node archive context, not a field of `ClpSQueryOption`. The task calls `clp_rust_utils::dataset::resolve_dataset_name(dataset.as_deref())`, so `None` becomes `default`. For filesystem storage, the resolved name selects `<archive-root>/<dataset>` and is passed as `results-cache --dataset <dataset>`. For S3 storage, it forms `<key-prefix><dataset>/<archive-id>` and is passed through `--dataset` so every MongoDB result records a non-empty dataset. |
| `archive_id` | `QueryCoordinator`, from the selected dataset's archive-metadata row | Non-empty per-node archive context paired with `dataset`. For filesystem storage, passed as `--archive-id <archive-id>`. For S3 storage, forms the final component of the archive object key. It also identifies the archive in task logs and result documents and is an input to the deterministic result `_id` defined by [Results-cache deduplication](https://app.notion.com/p/3c904e4d9e6b80d68854d02b96aaf267). |
| `clp_s_query_option.query_string` | Query-job configuration | A non-empty string passed as clp-s's positional query without reinterpretation by the TDL task. |
| `clp_s_query_option.max_num_results` | Query-job configuration | When `Some(n)`, passes `results-cache --max-num-results <n>`; the limit applies independently to this archive invocation. When `None`, omits `--max-num-results` and uses clp-s's default. |
| `clp_s_query_option.begin_timestamp_millisecs` | Query-job configuration | Inclusive lower bound in Unix epoch milliseconds. When present, passed unchanged as `--tge <milliseconds>`; omitted otherwise. |
| `clp_s_query_option.end_timestamp_millisecs` | Query-job configuration | Inclusive upper bound in Unix epoch milliseconds. When present, passed unchanged as `--tle <milliseconds>`; omitted otherwise. |
| `clp_s_query_option.ignore_case` | Query-job configuration | Adds `--ignore-case` when true; adds no argument when false. |
| `output_handle` | `QueryCoordinator`, copied into every archive input for the query job | Reserved to select the clp-s output-handler subcommand. The current `OutputHandle` enum is empty, so no value can yet be constructed or handled. The task implementation will define the initial results-cache variant and its exact argument mapping. |

The task obtains `CLP_HOME`, `archive_output`, and `results_cache` from process-global worker configuration. `results_cache` therefore must be added to `SpiderTaskExecutorConfig`; the task constructs its URI from the configured host, port, and database name. For S3 archive storage it also obtains the endpoint, region, bucket, key prefix, and AWS authentication configuration from `archive_output`, resolves the credentials, and injects them into the clp-s child environment. These deployment-wide values are not serialized into every task.

For filesystem archives, the intended command for the future results-cache variant is:

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

For S3 archives, the archive locator is instead:

```text
<CLP_HOME>/bin/clp-s s <s3-url-for-key-prefix/dataset/archive-id> --auth s3
```

The query and result-cache arguments following that locator are unchanged. The clp-s command contract interprets `--tge` and `--tle` as Unix epoch milliseconds. The TDL task MUST pass the signed values unchanged without rescaling them. `--tge` means timestamp greater than or equal to the inclusive lower bound; `--tle` means timestamp less than or equal to the inclusive upper bound.

The implementation MUST construct the argument vector without a shell, wait for the child process, and drain its standard streams. Exit code zero returns `Ok(())`; a configuration, credential, URL-construction, spawn/wait, or non-zero-exit failure returns `TdlError::ExecutionError`. Zero matching log events is successful.

#### 6.3.3 Outputs and their consumers

**Returned task output:** `()`. The task returns no application payload and no downstream node consumes an output. `Ok(())` reports successful completion of the archive node to Spider; the query job handler uses the terminal graph state, not task output, to determine job success.

**Persistent output:** clp-s writes one MongoDB document per retained match into collection `<query_job_id>`. clp-s supplies the deterministic `_id` specified by [Results-cache deduplication](https://app.notion.com/p/3c904e4d9e6b80d68854d02b96aaf267), as well as `orig_file_path`, `message`, `timestamp`, `archive_id`, `log_event_ix`, and `dataset`. `log_event_ix` is determined by clp-s while reading the archive and is not a TDL input. The current clp-s results-cache handler writes an empty `orig_file_path`. Results-cache readers use `dataset` and `archive_id` to identify the result source. Neither `QueryJobHandle` nor `QueryCoordinator` reads or rewrites these documents.

**Coordinator-visible outcome:** Spider's terminal graph state crosses back to the query job handler. Query results remain in MongoDB. A successful graph means every archive node returned successfully; the handler then marks the MySQL query-job row `SUCCEEDED` and records its completed duration. The handler marks the row `FAILED` when Spider reports failure or unexpected cancellation, ensuring that an accepted query job does not remain `RUNNING` after its Spider graph has terminated.

#### 6.3.4 Failure propagation and retry safety

[Results-cache deduplication](https://app.notion.com/p/3c904e4d9e6b80d68854d02b96aaf267) is the authoritative design for deterministic result identity, duplicate handling, and convergence after partial writes. This RFC defines how the TDL task exposes that behavior to Spider:

- clp-s MUST exit zero only after it has completed the results-cache write. A duplicate-only replay handled according to the deduplication design and a query with zero matches are successful executions.
- Any non-duplicate MongoDB error MUST cause clp-s to exit nonzero. The TDL wrapper MUST convert a configuration, credential, archive-locator, spawn, wait, signal-termination, or nonzero-exit failure into `TdlError::ExecutionError` containing the query-job ID, dataset, and archive ID as error context.
- The task MUST NOT return `Ok(())` before the child process exits or after any failure. It MUST NOT catch, log, and then convert a failure into `Ok`.
- The clp-s child MUST NOT outlive its Spider task instance. The TDL implementation MUST launch and supervise the child so that Spider's hard timeout terminates the child as well as the task executor, and it MUST reap the child during normal error handling. Deduplication does not replace process cleanup.
- A failed instance may leave a partial result set in MongoDB. Spider may create the one replacement instance allowed by Section 6.1.1; the deduplication contract makes that replacement converge on the same final result set without adding duplicate documents.
- If no allowed instance succeeds, Spider marks the logical archive node and the graph `FAILED`. `QueryJobHandle` then marks the MySQL query-job row `FAILED` with Spider's error rather than leaving it `RUNNING`.

### 6.4 Job-completion ownership

Compression has a separate `compression::commit` termination task because its archive tasks return `CompressionTaskOutput` values containing newly created archive metadata. A Spider worker running `compression::commit` gathers those outputs, publishes the archives, and marks the compression job successful in MySQL. Therefore, compression-job success is committed from the worker side.

The query data path has no equivalent publication boundary. Each CLP-S archive task writes its final query results directly to the MongoDB results cache, so a standalone query commit task would have no result payload to publish. The query graph consequently contains only archive-query nodes. When Spider reports that the graph succeeded, `QueryJobHandle`—the query job handler—MUST mark the MySQL query-job row `SUCCEEDED` and record its completed duration. If Spider reports graph failure or unexpected cancellation, the handler MUST mark the row `FAILED` instead. The handler is therefore responsible for ensuring that every MVP query job whose Spider graph terminates reaches either `SUCCEEDED` or `FAILED` in MySQL. A future cancellation-capable phase may also use `CANCELLED`; `KILLED` is not part of the query-job status model.
