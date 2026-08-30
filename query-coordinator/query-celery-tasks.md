# Query Celery Worker Tasks

A summary of the Celery worker tasks executed from
`components/job-orchestration/job_orchestration/executor/query/` — the per-archive
query workhorses launched by the Python query scheduler.

## Architecture at a glance

```text
Python query scheduler
  └─ submits a Celery group (one task message per archive)
       └─ query queue / Celery worker
            ├─ search                         registered Celery task
            │    └─ search_entry_point          task-specific Python logic
            │         ├─ run_query_task          shared Python helper
            │         │    └─ clo s / clp-s s   native subprocess
            │         └─ optional S3 upload      task-specific post-processing
            └─ extract_stream                 registered Celery task
                 └─ extract_stream_entry_point  task-specific Python logic
                      ├─ run_query_task          shared Python helper
                      │    └─ clo i / clp-s x   native subprocess
                      └─ optional S3 upload      task-specific post-processing
```

Celery directly invokes only the two registered tasks, `search` and
`extract_stream`. Their entry-point functions prepare the command, call the
shared `run_query_task` helper, and perform any task-specific post-processing.
`run_query_task` is a normal Python function, not another Celery task.

## 1. Scheduler: select and dispatch work

The Python query scheduler owns job-level orchestration. It selects the target
archives, inserts one SQL task row per archive, creates the corresponding Celery
signatures, and determines the final job status from their returned results.

The scheduler maintains two different kinds of state. `QueryJobStatus` is the
durable, externally visible lifecycle in MySQL. `InternalJobState` is ephemeral
bookkeeping in `active_jobs`: `WAITING_FOR_REDUCER`, `WAITING_FOR_DISPATCH`, and
`RUNNING` tell the scheduler whether to acquire a reducer, dispatch an archive
batch, or poll/revoke a Celery group. One MySQL `RUNNING` job can move through
several of these internal phases without changing its durable status.

This separation is intentional. A status or supporting column belongs in MySQL
when another process must observe it or when it is needed to make restart
recovery unambiguous. Reconstructible execution phases should remain in memory,
which avoids unnecessary MySQL updates and row contention. A replacement
coordinator may persist new recovery facts such as an external scheduler job ID,
but should not copy `InternalJobState` into the public status enum merely to
describe where its current function is executing.

- `SEARCH_OR_AGGREGATION` creates one `search.s(...)` per archive.
- `EXTRACT_JSON` and `EXTRACT_IR` create one `extract_stream.s(...)` per archive.

Search jobs may cover many archives, so the scheduler dispatches at most
`query_scheduler.num_archives_to_search_per_sub_job` in each Celery group. If
the job should continue, it creates another group after the current group
finishes. An extraction job resolves to one archive and therefore uses a
one-task group.

Every task receives a `job_id`, an `archive_id`, a per-archive `task_id`, the job
configuration, CLP metadata database connection parameters, the results-cache
URI, and an optional `dataset`.

## 2. Celery: deliver work to a registered task

`celery.py` creates the `Celery("query")` application and loads
`celeryconfig.py`. The configuration:

- Registers the `fs_search_task` and `extract_stream_task` modules.
- Routes both registered tasks to the `SchedulerType.QUERY` queue and permits
  Celery to create the queue if it is missing.
- Reads the broker and result-backend URLs from `BROKER_URL` and
  `RESULT_BACKEND`.
- Persists results, expires them after 7200 seconds, and enables
  `task_track_started` to distinguish queued tasks from started tasks.
- Reads the soft and hard task time limits from the `query_worker`
  configuration.
- Accepts JSON and pickle content and serializes results as JSON.
- Installs CLP's JSON log formatter on Celery and task loggers.

When a worker consumes a message, Celery invokes the registered `search` or
`extract_stream` wrapper. The wrapper adds structured log context, calls its
task-specific entry point, and logs and re-raises unexpected exceptions and
`SoftTimeLimitExceeded`.

## 3. Registered task: `search`

`fs_search_task.search` runs one `SEARCH_OR_AGGREGATION` query against one
archive.

```text
search (Celery wrapper)
  └─ search_entry_point
       ├─ load and validate configuration
       ├─ build one archive-search command
       ├─ run_query_task
       └─ optionally upload file output to S3
```

Its signature is:

```text
search(job_id, task_id, job_config_blob, archive_id,
       clp_metadata_db_conn_params, results_cache_uri, dataset=None)
```

`job_config_blob` is msgpack decoded into `SearchJobConfig`.

### Command selection

- With the **CLP** storage engine, it runs
  `clo s <archive_dir>/<archive_id>`. CLP search does not support S3 archive
  input or `write_to_file` here.
- With the **CLP_S** storage engine, it runs `clp-s s` against a filesystem
  archive directory or an S3 URL. S3 input uses `--auth s3` and credential
  environment variables.

Both variants add the query string and any configured timestamp bounds
(`--tge`/`--tle`), case-insensitive matching (`--ignore-case`), and path filter
(`--file-path`). CLP_S telemetry is sampled using
`query_trace_sampling_probability`; sampled commands receive
`--enable-telemetry` plus `CLP_QUERY_ID` and `CLP_TASK_ID` environment variables.

### Output selection

The first matching output mode wins:

1. `aggregation_config` selects `reducer` with `--host`, `--port`, and
   `--job-id`. `--count` is currently added whenever
   `do_count_aggregation` is non-null, including when explicitly `False`;
   `--count-by-time` is added when configured.
2. `network_address` selects `network` with `--host` and `--port`.
3. `write_to_file` selects `file` with
   `--path <stream_output>/<job_id>/<archive_id>`.
4. Otherwise, `results-cache` receives the results-cache URI, job ID as the
   collection, maximum result count, and optional dataset.

### Search-specific post-processing

After `run_query_task` reports subprocess success, file output configured with
S3 stream storage is uploaded to `{job_id}/{archive_id}` relative to the S3 key
prefix. Missing or empty output is skipped. The local file is removed whether
the upload succeeds or fails. The task then returns a serialized
`QueryTaskResult` containing `status`, `task_id`, and `duration`.

## 4. Registered task: `extract_stream`

`extract_stream_task.extract_stream` extracts an IR or JSON stream from one
archive.

```text
extract_stream (Celery wrapper)
  └─ extract_stream_entry_point
       ├─ load and validate configuration
       ├─ build one archive-extraction command
       ├─ run_query_task
       └─ optionally upload produced streams to S3
```

Its signature is:

```text
extract_stream(job_id, task_id, query_job_type, job_config, archive_id,
               clp_metadata_db_conn_params, results_cache_uri, dataset=None)
```

`job_config` is a dictionary validated as `ExtractIrJobConfig` or
`ExtractJsonJobConfig`.

### IR versus JSON selection

The package storage engine, not `query_job_type`, selects the extraction mode:

- **CLP** builds an IR extraction command using `clo i` and
  `ExtractIrJobConfig`.
- **CLP_S** builds a JSON extraction command using `clp-s x` and
  `ExtractJsonJobConfig`.

The Celery task receives `query_job_type`, but uses it only as log context; it
does not pass it to `extract_stream_entry_point`. The scheduler therefore
implicitly relies on `EXTRACT_IR` jobs running with CLP and `EXTRACT_JSON` jobs
running with CLP_S.

### Command selection

- **CLP / IR:** Runs `clo i` with the archive path, file-split ID, stream-output
  directory, results-cache URI, and stream collection as positional arguments.
  `file_split_id` is required, `--target-size` is added when configured, and S3
  archive input is unsupported.
- **CLP_S / JSON:** Runs `clp-s x <archive_dir|s3_url> <stream_output_dir>`.
  Filesystem input adds `--archive-id`; S3 input adds `--auth s3` and credential
  environment variables. It always adds `--ordered`, the results-cache URI and
  stream collection, and optionally `--target-ordered-chunk-size`.

The extraction process writes stream metadata to `stream_collection_name` in
the results cache.

### Extraction-specific post-processing

S3 stream output enables `--print-ir-stats` or
`--print-ordered-chunk-stats`. Each stdout line then identifies a generated
local stream as JSON: `{"path": "<local stream file>", ...}`. Validated worker
configuration allows S3 stream output only with CLP_S, so in practice this path
is reachable for JSON extraction, not IR extraction.

After subprocess success, the task parses the lines and uploads each file under
its basename relative to the configured S3 key prefix. The first parse or
upload error disables further upload attempts, although later lines are still
parsed. Every path obtained from a valid line is unlinked, including the file
whose upload failed and files skipped after an earlier error. A malformed line
or one without `path` cannot identify a local file to remove.

Any such error changes the returned `QueryTaskResult` to `FAILED`; otherwise the
task returns the result produced by `run_query_task`.

## 5. Shared helper: `run_query_task`

Both task-specific entry points call `utils.py:run_query_task` with a fully
constructed command. The helper owns native-process execution and the normal
SQL task-status lifecycle:

1. Updates the SQL task row to `RUNNING` with its `start_time`.
2. Opens `<clp_logs_dir>/<job_id>/<task_id>-clo.log` and launches the command in
   its own process group using `subprocess.Popen` and `os.setpgrp`.
3. Captures stdout and redirects stderr to the log file.
4. Installs a SIGTERM handler that terminates the entire child process group.
5. Waits for completion, maps exit code zero to `SUCCEEDED` and any other exit
   code to `FAILED`, calculates the duration, and updates the SQL task row.
6. Replays the subprocess's stderr file through the task logger and returns
   `(QueryTaskResult, stdout_str)` to the task-specific entry point.

`run_query_task` does not know whether its caller will subsequently upload
files. Uploading therefore remains task-specific post-processing outside the
shared helper.

### Related helpers

- `report_task_failure` writes `FAILED` with duration zero when worker-config
  loading returns `None` or command construction explicitly returns no command.
- `update_query_task_metadata` updates fields in `QUERY_TASKS_TABLE_NAME` for a
  task ID. It constructs the `SET` expressions using
  `f'{k}="{v}"'` string interpolation.
- `get_query_hash` computes a SHA-256 hash of a search query for log context.

## Status and failure caveats

The Celery result, SQL task row, and overall SQL job row are related but are not
the same source of state:

- The scheduler derives the overall job result from the values returned by the
  Celery tasks.
- `run_query_task` updates the SQL task row before task-specific S3 upload.
  Consequently, an upload failure returns `FAILED` and fails the job, but the
  task row remains `SUCCEEDED`.
- Exceptions such as malformed msgpack, Pydantic validation failures, missing
  environment variables, or subprocess-launch failures bypass
  `report_task_failure`. Depending on when the exception occurs, the SQL task
  row can remain `PENDING` or `RUNNING`, while the scheduler still marks the job
  failed after observing the exceptional Celery result.
- Celery revocation with termination sends SIGTERM; `run_query_task` terminates
  the native process group, while the scheduler separately changes pending and
  running task rows to `CANCELLED`.
- `SoftTimeLimitExceeded` is logged and re-raised by the registered task wrapper,
  but that path does not call `report_task_failure` or explicitly terminate the
  native subprocess group.
