# RFC: MVP query-job handler

This RFC defines the normative lifecycle contract for `QueryJobHandle` and its
`QueryJobSubmitter` dependency. Shared architecture and terminology are described in
[Query system architecture](query-system-architecture.md). The Spider graph, serialized inputs, and worker
task behavior are defined in the [query TDL RFC](query-tdl-rfc.md).

## 1. Scope

The handler drives one already-planned, nonempty `clp-s` query job from Spider registration through
terminal MySQL persistence. It also supports reattaching to a previously submitted job after a
coordinator restart.

The handler does not:

- Validate or categorize query-job configurations.
- Select archives or handle the zero-archive path.
- Calculate an archive's `ExecutionPolicy`.
- Construct Spider task descriptors or serialize TDL inputs.
- Read or modify MongoDB query results.
- Implement user-requested cancellation.

## 2. Interfaces

### 2.1 Prepared query inputs

The coordinator passes submission inputs individually. The handler API does not define a
`QueryPlan` wrapper:

```rust
pub struct SpiderPollingOption {
    pub initial_poll_backoff: Duration,
    pub max_poll_backoff: Duration,
}

pub struct QueryJobHandle<SubmitterType: QueryJobSubmitter> {
    // Database, query-job identity, submitter, and polling configuration only.
}

impl<SubmitterType: QueryJobSubmitter> QueryJobHandle<SubmitterType> {
    pub fn new(
        db_pool: MySqlPool,
        query_job_id: QueryJobId,
        job_submitter: SubmitterType,
        spider_polling_option: Arc<SpiderPollingOption>,
    ) -> Self;

    pub async fn run(
        self,
        resource_group_id: ResourceGroupId,
        clp_s_query_option: ClpSQueryOption,
        output_handle: OutputHandle,
        archives_to_search: Vec<(ArchiveMetadata, ExecutionPolicy)>,
    ) -> Result<(), Error>;

    pub async fn recover(self, spider_job_id: SpiderJobId) -> Result<(), Error>;
}
```

`run` requires a nonempty `archives_to_search`. The coordinator handles zero selected archives
before constructing the handle. Every `ExecutionPolicy` contains the corresponding archive task's
retry, concurrency, and timeout settings, including `max_num_retry`. `SpiderPollingOption` contains
only the backoff used to observe the job and does not duplicate task execution settings.

`ArchiveMetadata` is coordinator-side graph metadata containing the archive ID, optional dataset,
and compressed size. It is not a TDL wire type: the submitter serializes only the dataset and
archive ID from it.

### 2.2 Submitter contract

`QueryJobSubmitter` exposes the two query-specific Spider operations needed by the handler:

```rust
#[async_trait]
pub trait QueryJobSubmitter: Clone + Send + Sync {
    async fn submit_query_job(
        &self,
        query_job_id: QueryJobId,
        resource_group_id: ResourceGroupId,
        clp_s_query_option: ClpSQueryOption,
        output_handle: OutputHandle,
        archives_to_search: Vec<(ArchiveMetadata, ExecutionPolicy)>,
    ) -> Result<SpiderJobId, Error>;

    async fn run_query_job_to_completion(
        &self,
        spider_job_id: SpiderJobId,
        initial_poll_backoff: Duration,
        max_poll_backoff: Duration,
    ) -> Result<QueryJobOutcome, Error>;
}
```

`submit_query_job` registers but does not start the graph. `run_query_job_to_completion` is safe for
a registered, running, or already-terminal Spider job. Its outcomes are:

```rust
pub enum QueryJobOutcome {
    Succeeded,
    Failed { error_message: String },
    UnexpectedlyCancelled,
}
```

The `SpiderClient` implementation owns task names, graph descriptors, input serialization, and the
translation from Spider states to `QueryJobOutcome`. The trait lets handler lifecycle tests inject
controlled submission and terminal outcomes without running Spider.

## 3. New-job lifecycle

`run` performs these operations in order:

1. Reject an empty archive vector as an internal contract violation; the coordinator must have
   completed the zero-archive job without creating the handle.
2. Convert the archive count to the MySQL `num_tasks` representation. An out-of-range count fails
   before Spider registration.
3. Ask the submitter to register the graph and obtain its Spider job ID.
4. Atomically update the MySQL row only when its status is `PENDING`, setting:
   - `spider_id` to the registered Spider job ID.
   - `status` to `RUNNING`.
   - `num_tasks` to the number of selected archives.
   - `start_time` to the database's current timestamp.
5. Ask the submitter to idempotently start the job and poll it to a terminal state.
6. Persist the terminal outcome only while the row remains `RUNNING`.

Registration must precede persistence because `spider_id` is allocated by Spider. Starting must
follow persistence so a coordinator crash cannot leave executing work without a durable Spider ID.

### 3.1 Pending compare-and-set

Exactly one row must be affected by the `PENDING`-to-`RUNNING` update. A zero-row result returns
`JobNotPending`. The handler must not report that race as a job failure or overwrite the state
written by another owner.

### 3.2 Terminal outcomes

The handler maps Spider outcomes as follows:

| Spider outcome | MySQL status | Status message |
| --- | --- | --- |
| Succeeded | `SUCCEEDED` | Empty |
| Failed | `FAILED` | Spider's job error, truncated to the schema limit |
| Unexpectedly cancelled | `FAILED` | States that Spider unexpectedly cancelled the job |

Terminal updates set the elapsed duration from `start_time` using the database clock. A terminal or
otherwise ineligible row is left unchanged. Explicit `CANCELLED` handling belongs to MVP+1.

## 4. Failure behavior

Failures are divided by the durable `RUNNING` boundary:

- A validation, task-count, graph-registration, or submission-persistence failure occurs before a
  durable running job exists. The handler returns the original error and makes a best-effort
  `PENDING`-to-`FAILED` update, except when it lost the pending compare-and-set.
- Once `RUNNING + spider_id` is durable, polling or terminal-persistence errors leave the row
  `RUNNING`. The coordinator can recover it and retry observation or terminal persistence without
  resubmitting the graph.
- Failure to persist the terminal state is distinguishable from a Spider execution failure. A
  successful Spider graph must never be relabelled `FAILED` merely because its success update
  failed.
- Failure to fetch Spider's error message is logged. The handler still records a fallback message
  so the terminal graph failure does not leave the query job running.

Best-effort failure reporting must preserve all existing terminal states. A failure-reporting error
is logged without replacing the original lifecycle error.

## 5. Recovery

At startup, the coordinator selects query jobs whose status is `RUNNING` and whose `spider_id` is
not null. It constructs a handle with only the common lifecycle dependencies and calls `recover`
with the persisted Spider ID. Recovery does not require a resource group, query options, output
handle, archive metadata, or execution policies, and it never registers a second graph.

`recover` calls the same idempotent start-and-poll operation used after new submission, then applies
the same terminal mapping. Repeating recovery after a transport or SQL failure is safe because the
Spider start operation and the MySQL terminal update are idempotent with respect to already-running
or already-terminal state.

## 6. Registration ambiguity

The Spider API currently provides no coordinator-supplied idempotency key for graph registration.
There is therefore an unavoidable ambiguity if Spider accepts a graph but the client does not
receive its ID, or if MySQL fails before `spider_id` becomes durable. Because the handler has not
started the graph, such a graph should remain non-executing, but it may be orphaned in Spider.

For the MVP:

- The handler does not automatically retry graph registration inside one `run` call.
- It best-effort marks the still-pending CLP job `FAILED` when the failure is known.
- Operators may need to garbage-collect registered, unstarted Spider jobs.
- Eliminating this ambiguity requires a future Spider idempotency key or another durable
  pre-registration identity; it must not be hidden by silently resubmitting.

## 7. Required verification

Handler tests must cover:

- Successful registration, `PENDING`-to-`RUNNING` persistence, polling, and success persistence.
- Spider failure and unexpected cancellation mapping.
- Rejection of an empty archive vector and an oversized task count before registration.
- A lost pending compare-and-set without failure-state overwrite.
- Submission failures and best-effort pre-running failure reporting.
- Polling and terminal-persistence failures leaving a durable job `RUNNING`.
- Recovery without submission inputs and without graph registration.
- Idempotent recovery of running and already-terminal Spider jobs.
- Failure to retrieve Spider's error string while preserving a terminal CLP failure.
