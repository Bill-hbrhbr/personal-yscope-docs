# CLP search task: inputs, environment, outputs, and side effects

Research input for the design of a Rust **search-coordinator** (analogous to
`components/compression-coordinator`) and a Spider **TDL search task** (analogous to the compression
task in `components/clp-tdl-package`).

Everything below is derived from source on branch `docs/2026-08-04-spider-helm-guide`. Every
non-obvious claim carries a `path:line` citation.

## Scope and exclusions

Per the stated assumptions this report analyses **only** the result-cache output path with **no
aggregation**:

- **(a) Result-cache only.** Search results are written to MongoDB by the native binary's
  `results-cache` output handler. The `network` and `file` output handlers are out of scope.
- **(b) No aggregation.** No reducer, no count/count-by-time/min/max/unique.

The Python task selects exactly one output handler through a 4-way chain whose order is
`aggregation_config` > `network_address` > `write_to_file` > results-cache
(`components/job-orchestration/job_orchestration/executor/query/fs_search_task.py:188-231`).
Reaching the result-cache branch therefore requires **all three** of the following to hold on the
job config: `aggregation_config is None`, `network_address is None`, `write_to_file is False`.

Code paths excluded by the scope, listed so they can be deliberately *not* ported:

| Excluded path | Where it lives |
| --- | --- |
| Reducer handler args (`reducer --host --port --job-id [--count] [--count-by-time]`) | `fs_search_task.py:188-202` |
| `AggregationConfig` model | `job_orchestration/scheduler/job_config.py:79-84` |
| Reducer acquisition, reducer TCP listener, `InternalJobState.WAITING_FOR_REDUCER` | `scheduler/query/query_scheduler.py:747-791`, `:1273`, `:1287-1293`; all of `scheduler/query/reducer_handler.py` |
| `network` handler (`network --host --port`) | `fs_search_task.py:203-210` |
| `file` handler + stream-output dir + post-run S3 upload/unlink | `fs_search_task.py:211-220`, `:236-251`, `:319-328` |
| C++ reducer library and reducer output handlers | `core/src/reducer/*`; `core/src/clp/clo/OutputHandler.hpp:228-288`; `core/src/clp_s/OutputHandlerImpl.hpp:220-361`; `core/src/clp_s/AggregationSink.hpp:20-127` |
| `clp-s` `stdout` output handler, kv-IR search fallback | `core/src/clp_s/clp-s.cpp:355-367`, `:500-547` |
| Presto search backend (writes the same Mongo cache, never touches `query_jobs`) | `webui/packages/server/src/routes/api/presto-search/index.ts` |

One consequence worth stating up front: **result-cache output is not the default today.** The Rust
api-server maps `write_to_file: !value.buffer_results_in_mongodb`
(`components/api-server/src/client.rs:219`) and `buffer_results_in_mongodb` is a bare
`#[serde(default)] bool` (`client.rs:203-207`), i.e. `false`. So an api-server caller gets **file
output** unless it explicitly passes `buffer_results_in_mongodb: true`. Only the webui
(`webui/packages/server/src/routes/api/search/index.ts:75-105`) and the MCP server
(`clp-mcp-server/clp_mcp_server/clp_connector.py:60-69`) submit result-cache jobs today; the CLI
(`clp-package-utils/clp_package_utils/scripts/native/search.py:50-59`) always sets `network_address`
and therefore never uses the result cache.

## 0. What the search task is, in one paragraph

The Celery-registered task is `job_orchestration.executor.query.fs_search_task.search`, a bound task
with 6 required parameters and 1 optional one (`fs_search_task.py:333-343`). It binds structlog
context and delegates to `search_entry_point` (`fs_search_task.py:344-356`, entry point at
`:254-262`). The entry point reads four env vars, loads a worker config YAML, opens a MySQL
connection to write task-status rows, builds an argv for a native binary (`bin/clo` or `bin/clp-s`),
spawns it with its stderr redirected to a per-task log file, waits, and maps the child's exit code
to SUCCEEDED/FAILED. **The task never sees a search result**: the native binary writes the hits
directly into MongoDB. The Python layer's entire contribution to the output is a 3-field status
dict.

## 1. What is the input of a search task?

There are three distinct input surfaces: (1.1) the per-job config blob, (1.2) the per-task Celery
kwargs, and (1.3) the implicit worker-side configuration that the scheduler never sends. A Spider
TDL task must re-supply all three.

### 1.1 Per-job configuration — `SearchJobConfig` (msgpack blob)

Delivered as the `job_config_blob: bytes` kwarg; the task does
`SearchJobConfig.model_validate(msgpack.unpackb(job_config_blob))` (`fs_search_task.py:287`, and
again for log context at `:59`). The blob is `msgpack.packb(self.get_config().model_dump())`
produced by `QueryJob.get_cached_config_blob()`
(`job_orchestration/scheduler/scheduler_data.py:56-59`) — it is **not** the raw
`query_jobs.job_config` column bytes, though the api-server writes that column with
`rmp_serde::to_vec_named` (`api-server/src/client.rs:298`), which produces the same named-map
msgpack encoding.

Model at `job_orchestration/scheduler/job_config.py:104-123`. A Rust mirror already exists at
`components/clp-rust-utils/src/job_config/search.rs:15-26`, with `aggregation_config: Option<()>` as
an explicit placeholder — already aligned with assumption (b).

| Field | Type / default | Consumed by | Effect on the result-cache path |
| --- | --- | --- | --- |
| `datasets` | `list[str] \| None = None` (`job_config.py:105`) | scheduler only (`query_scheduler.py:1390`, `:1451-1460`) | Chooses which archive tables to query; never sent to the task. Per-archive `dataset` is derived from it. |
| `query_string` | `str`, required (`job_config.py:106`) | task | Positional query argument (`fs_search_task.py:178`). Also sha256-hashed for the log context (`fs_search_task.py:64`; `executor/query/utils.py:19-20`). |
| `max_num_results` | `int`, required, no default (`job_config.py:107`) | task | `--max-num-results <n>` on the `results-cache` handler (`fs_search_task.py:227`). **Must be >= 1** — both binaries reject 0 (`core/src/clp/clo/CommandLineArguments.cpp:694-696`; `core/src/clp_s/CommandLineArguments.cpp:1283-1285`). |
| `begin_timestamp` | `int \| None = None` (`job_config.py:108`) | task | `--tge <ms>` (`fs_search_task.py:179-181`). |
| `end_timestamp` | `int \| None = None` (`job_config.py:109`) | task | `--tle <ms>` (`fs_search_task.py:182-184`). |
| `ignore_case` | `bool = False` (`job_config.py:110`) | task | `--ignore-case` (`fs_search_task.py:185-186`). |
| `path_filter` | `str \| None = None` (`job_config.py:111`) | task | `--file-path <p>`, **clo/CLP engine only** (`fs_search_task.py:91-93`); silently ignored on the clp-s branch. |
| `network_address` | `tuple[str,int] \| None = None`, port validated to [1,65535] (`job_config.py:112-113`, `:117-122`) | task | **Must be None** for the result-cache path (`fs_search_task.py:203`). |
| `aggregation_config` | `AggregationConfig \| None = None` (`job_config.py:114`) | task + scheduler | **Must be None** (`fs_search_task.py:188`). Excluded. |
| `write_to_file` | `bool = False` (`job_config.py:115`) | task | **Must be False** (`fs_search_task.py:211`). Note the api-server's default makes it True (`api-server/src/client.rs:219`). |

Normalizations the api-server applies before persisting (`api-server/src/client.rs:282-292`), which
a coordinator must replicate because nothing downstream enforces them:

- if `datasets.is_none()` and the storage engine is `ClpS`, set `datasets = Some(vec!["default"])`
  (`client.rs:283-288`);
- if `max_num_results == 0`, set it to `ApiServer.default_max_num_query_results`, default `1000`
  (`client.rs:289-291`; `clp-py-utils/clp_py_utils/clp_config.py:738`).

### 1.2 Per-task parameters — the Celery kwargs

Task signature (`fs_search_task.py:334-343`):

```python
def search(self: Task, job_id: str, task_id: int, job_config_blob: bytes, archive_id: str,
           clp_metadata_db_conn_params: dict, results_cache_uri: str,
           dataset: str | None = None) -> dict[str, Any]
```

The only live dispatch site for search is `DispatchExecutor.dispatch_job_and_update_db`
(`query_scheduler.py:212-233`), run inside a `ProcessPoolExecutor` (`query_scheduler.py:915-916`).
The second `search.s(...)` inside `get_task_group_for_job` (`query_scheduler.py:694-706`) is
unreachable for search jobs: its only caller chain terminates at the stream-extraction path
(`query_scheduler.py:725-745`, `:794-816`, `:1603-1610`).

| Parameter | Type | Scope | Origin |
| --- | --- | --- | --- |
| `job_id` | `str` | per-job | `str(job["job_id"])` from the `query_jobs.id` auto-increment INT (`query_scheduler.py:845`; DDL `clp-py-utils/clp_py_utils/initialize-orchestration-db.py:134`). Used verbatim as the **MongoDB collection name** (`fs_search_task.py:226`) and as the log-directory name (`executor/query/utils.py:23-26`). |
| `task_id` | `int` | per-task | `cursor.lastrowid` of the `query_tasks` row inserted per archive (`query_scheduler.py:537-549`, called at `:220`). |
| `job_config_blob` | `bytes` (msgpack) | per-job | `QueryJob.get_cached_config_blob()` (`scheduler_data.py:56-59`). See 1.1. |
| `archive_id` | `str` | **per-task** | `archives[i]["archive_id"]` from `SELECT id AS archive_id, end_timestamp FROM <prefix>archives ...` (`query_scheduler.py:573-575`) or the per-dataset `UNION ALL` variant (`query_scheduler.py:603-609`). |
| `clp_metadata_db_conn_params` | `dict` | per-deployment | `clp_config.database.get_clp_connection_params_and_type(True)` (`query_scheduler.py:1314-1316`; builder at `clp_config.py:259-286`). Re-validated as `Database` and wrapped in a `SqlAdapter` (`fs_search_task.py:273`). **Ships the DB password in-band in the Celery message.** `table_prefix` is injected at `clp_config.py:282` but silently dropped by `Database.model_validate` (not a model field, `clp_config.py:200-213`). |
| `results_cache_uri` | `str` | per-deployment | `clp_config.results_cache.get_uri()` = `mongodb://{host}:{port}/{db_name}`, db_name default `clp-query-results` (`clp_config.py:451-461`; passed at `query_scheduler.py:1317`). The trailing path component is load-bearing: both binaries take the database name from `mongo_uri.database()` (`core/src/clp_s/ResultsCacheUtils.cpp:14-30`; `core/src/clp/clo/OutputHandler.cpp:54-61`). |
| `dataset` | `str \| None = None` | **per-task** | `archives[i].get("dataset")` — present only when `SearchJobConfig.datasets is not None`, i.e. only on clp-s (`query_scheduler.py:1451-1460`, comment "CLP-Text does not support datasets"). |

**Task granularity: one task == exactly one archive.** The scheduler inserts one `query_tasks` row
per archive and builds a Celery group with one signature per archive (`query_scheduler.py:220-233`,
`:537-549`). Archives are dispatched in "sub-jobs" of at most `num_archives_to_search_per_sub_job`
(default 16, `clp_config.py:370-380`), so a job with N archives becomes `ceil(N/16)` sequential
rounds (`query_scheduler.py:890-899`, re-entry at `:1016`).

### 1.3 Implicit input the scheduler never sends: `WorkerConfig`

`worker_config = load_worker_config(Path(os.getenv("CLP_CONFIG_PATH")), logger)`
(`fs_search_task.py:276-277`), where `load_worker_config` is
`WorkerConfig.model_validate(read_yaml_config_file(path))` wrapped in a broad `except Exception`
returning `None` (`job_orchestration/executor/utils.py:36-50`). Model at `clp_config.py:1111-1119`.

| Field read | Used for |
| --- | --- |
| `package.storage_engine` (`clp_config.py:162-163`, default `CLP_S`) | Binary selection: `CLP` -> `bin/clo`, `CLP_S` -> `bin/clp-s`, anything else logs "Unsupported storage engine" and fails the task (`fs_search_task.py:161-173`). |
| `archive_output.storage.type` | S3 vs FS archive addressing (`fs_search_task.py:74`, `:111`). **CLP engine refuses S3 outright** (`fs_search_task.py:75-80`). |
| `archive_output.get_directory()` (`clp_config.py:683-684`, helper at `:650-658`) | FS archive path: `<dir>/<archive_id>` for clo (`fs_search_task.py:89-90`), `<dir>/<dataset>` + `--archive-id` for clp-s (`fs_search_task.py:129-137`). |
| `archive_output.storage.s3_config` (`clp_config.py:557-562`) | S3 URL `f"{key_prefix}{dataset}/{archive_id}"` via `generate_s3_url` (`fs_search_task.py:112-127`; `clp-py-utils/clp_py_utils/s3_utils.py:257-284`), plus AWS credential env vars. |
| `query_worker.query_trace_sampling_probability` (`clp_config.py:399`, default `0.01`) | Telemetry sampling (`fs_search_task.py:140-144`). |
| `stream_output` | **Excluded** — `write_to_file` path only (`fs_search_task.py:212`, `:319-326`). |

The file at `CLP_CONFIG_PATH` is a full `ClpConfig`-shaped YAML, not a purpose-built worker config;
pydantic's default extra-ignore lets it validate as a `WorkerConfig`. Docker Compose writes
`yaml.safe_dump(container_clp_config.dump_to_primitive_dict())` to `<logs_dir>/.clp-config.yaml`
(`clp-package-utils/clp_package_utils/general.py:405-411`, `:416-421`; called from
`controller.py:1086-1089`) and bind-mounts it read-only at `/etc/clp-config.yaml`
(`tools/deployment/package/docker-compose-all.yaml:47-52`, `:462`). Helm renders a hand-written
ConfigMap key from `.Values.clpConfig.*` instead
(`tools/deployment/package-helm/templates/configmap.yaml`, mounted at
`query-worker-deployment.yaml:62-65`). Note `generate_worker_config()` (`general.py:379-388`) has
**zero callers repo-wide**, so `WorkerConfig.stream_collection_name` always falls back to
`"stream-files"` — harmless for search, a live bug for `extract_stream`.

### 1.4 The command line actually produced

clp-s, FS archives, result-cache only:

```
<CLP_HOME>/bin/clp-s s <archive_output_dir>/<dataset> --archive-id <archive_id> \
    [--enable-telemetry] <query_string> [--tge N] [--tle N] [--ignore-case] \
    results-cache --uri <results_cache_uri> --collection <job_id> \
    --max-num-results <n> [--dataset <dataset>]
```

Assembly order: `[clp-s, "s"]` (`fs_search_task.py:106-109`) -> archive locator (`:111-137`) ->
optional `--enable-telemetry` (`:145-146`) -> query string (`:178`) ->
`--tge`/`--tle`/`--ignore-case` (`:179-186`) -> handler (`:221-231`). With S3 archives the locator
is `<s3_url> --auth s3` and there is **no** `--archive-id` (`fs_search_task.py:113-127`).

clo (unstructured CLP engine), FS archives only:

```
<CLP_HOME>/bin/clo s <archive_output_dir>/<archive_id> [--file-path <p>] \
    <query_string> [--tge N] [--tle N] [--ignore-case] \
    results-cache --uri <uri> --collection <job_id> --max-num-results <n>
```

`clo` has **no** `--dataset` and **no** `--enable-telemetry` option
(`core/src/clp/clo/CommandLineArguments.cpp:302-394`), and its handler-argument parser rejects
unknown options (`core/src/clp/cli_utils.cpp:6-23`), so a CLP-engine job carrying a non-None
`dataset` would fail at CLI parse — `fs_search_task.py:230-231` appends `--dataset` regardless of
engine. Today this is only avoided because `dataset` is always None for CLP-engine archives.

`--batch-size` is never passed by the task, so the C++ default of 1000 applies on both engines
(`core/src/clp_s/CommandLineArguments.hpp:36-42`;
`core/src/clp/clo/CommandLineArguments.hpp:40-41`).

## 2. What environment variables does a search task require?

### 2.1 Read directly by the task process

| Variable | Read at | Required? | Default / behaviour if unset |
| --- | --- | --- | --- |
| `CLP_LOGS_DIR` | `fs_search_task.py:266` | **Required** | `Path(os.getenv(...))` raises `TypeError` if unset — and this is the *first* dereference, before the DB adapter and before `CLP_CONFIG_PATH`. |
| `CLP_LOGGING_LEVEL` | `fs_search_task.py:267-268` | Optional | `set_logging_level` maps `None` to INFO silently; an unrecognized value logs `"Invalid logging level: %s, using INFO as default"` (`clp-py-utils/clp_py_utils/clp_logging.py:132-149`). |
| `CLP_CONFIG_PATH` | `fs_search_task.py:276` | **Required** | `TypeError` if unset. Also read at **worker boot** by `celeryconfig` (`executor/query/celeryconfig.py:23`) via `load_clp_config_from_config_path_env_var()`, which falls back to a default `ClpConfig()` when unset/empty (`executor/utils.py:27-33`). |
| `CLP_HOME` | `fs_search_task.py:286` | **Required** | `TypeError` if unset. Locates `bin/clo` / `bin/clp-s`. |
| `CLP_DISABLE_TELEMETRY` | `clp-py-utils/clp_py_utils/telemetry_config.py:9-13` | Optional | Disables telemetry sampling when stripped/lowercased value is in `{"1","true","yes","y"}` (`telemetry_config.py:6`). |
| `DO_NOT_TRACK` | `telemetry_config.py:9-13` | Optional | Same semantics as above. |
| `BROKER_URL` | `celeryconfig.py:17` | Required at worker boot | Celery-specific; disappears under Spider. |
| `RESULT_BACKEND` | `celeryconfig.py:18` | Required at worker boot | Celery-specific; disappears under Spider. |

### 2.2 Passed *into* the child process

For clp-s the child env is `dict(os.environ)` (`fs_search_task.py:110`) plus:

| Variable | Source | When |
| --- | --- | --- |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | `get_credential_env_vars(s3_config.aws_authentication)` (`fs_search_task.py:128`; `s3_utils.py:69-107`, key constants `:33-35`) | S3 archive storage only. `env_vars` auth type returns `{}`; `profile`/`default` resolve via boto3 and **raise `ValueError` that propagates out of the task** on failure (`s3_utils.py:88-98`). Read by `try_sign_url` (`core/src/clp_s/InputConfig.cpp:204-220`); the first two are mandatory for `--auth s3`. |
| `CLP_QUERY_ID` = `job_id`, `CLP_TASK_ID` = `str(task_id)` | `fs_search_task.py:145-147` | Only when `--enable-telemetry` is sampled in. Read as span attributes at `core/src/clp_s/search/SearchTelemetry.cpp:242-247`. |

For the clo path `_make_core_clp_command_and_env_vars` returns `(command, None)`
(`fs_search_task.py:94`), so `Popen(env=None)` and the child inherits the worker's environment
verbatim (`executor/query/utils.py:68-75`).

### 2.3 Read by the native binaries themselves

| Variable | Binary | Purpose |
| --- | --- | --- |
| `HOME` | clo | Default `--config-file` = `$HOME/.clp.rc`; falls back to `./` (`core/src/clp/clo/CommandLineArguments.cpp:39-46`; `core/src/clp/Defs.h:32`). |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | clp-s | S3 presigning (`core/src/clp_s/InputConfig.cpp:204-220`; names at `InputConfig.hpp:18-20`). |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_ENDPOINT` | clp-s | If either is set, CLP's own endpoint override is skipped (`core/src/clp_s/search/TelemetryContext.cpp:72-76`). |
| `CLP_TELEMETRY_ENDPOINT` | clp-s | Trailing `/` stripped, `"/v1/traces"` appended (`TelemetryContext.cpp:78-90`). Deployment default `https://telemetry.yscope.io` (`docker-compose-all.yaml:638`; `controller.py:1003`). |
| `OTEL_SERVICE_NAME` | clp-s | `service.name` attribute; defaults to `clp-search` when unset (`TelemetryContext.cpp:33-35`, `:95`). |
| `CLP_QUERY_ID`, `CLP_TASK_ID` | clp-s | Span attributes (`core/src/clp_s/search/SearchTelemetry.cpp:242-247`). |
| `CURL_CA_BUNDLE`, `SSL_CERT_FILE` | clp-s (curl) | TLS trust store (`core/src/clp/CurlDownloadHandler.cpp:139-146`). |

`CLP_DB_USER`/`CLP_DB_PASS` (`core/src/clp/GlobalMetadataDBConfig.cpp:122-128`) are **not**
reachable from clo or clp-s — that translation unit is not in either binary's CMake target. Neither
search binary touches MySQL; clo validates the archive purely by the presence of the archive
metadata file (`core/src/clp/clo/clo.cpp:368-383`).

### 2.4 What the deployments actually set on the query worker

Docker Compose (`tools/deployment/package/docker-compose-all.yaml:441-478`, telemetry anchor at
`:41-44`): `CLP_DISABLE_TELEMETRY`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `BROKER_URL`,
`CLP_CONFIG_PATH=/etc/clp-config.yaml`, `CLP_HOME=/opt/clp`, `CLP_LOGGING_LEVEL` (default INFO),
`CLP_LOGS_DIR=/var/log/query_worker`, `OTEL_METRIC_EXPORT_INTERVAL`,
`OTEL_SERVICE_NAME=query-worker`, `PYTHONPATH`, `RESULT_BACKEND`.

Helm sets the same set on the query-worker pod
(`tools/deployment/package-helm/templates/query-worker-deployment.yaml:31-50`, with helpers at
`_helpers.tpl:138-146`, `:474-482`, `:490-497`).

## 3. What is the explicit output of a search task?

### 3.1 Return value

`search_entry_point` returns `task_results.model_dump()` (`fs_search_task.py:330`) where the model
is

```python
class QueryTaskResult(BaseModel):
    status: QueryTaskStatus
    task_id: int
    duration: float
```

(`job_orchestration/scheduler/scheduler_data.py:100-103`).

| Field | Type | Value |
| --- | --- | --- |
| `status` | `QueryTaskStatus(StatusIntEnum)` — `PENDING=0, RUNNING, SUCCEEDED, FAILED, CANCELLED, KILLED` via `auto()` (`scheduler/constants.py:66-72`; base at `:16-26`) | Serializes as an **int** under `result_serializer = "json"` (`celeryconfig.py:42`). |
| `task_id` | `int` | Echo of the input `task_id`. |
| `duration` | `float` | `(datetime.now() - start_time).total_seconds()` (`executor/query/utils.py:109`); hard-coded `0` on early-failure paths (`utils.py:38`, `:44`). |

Note the return value contains **no result count, no archive_id, no hit data**. The scheduler
consumes only these three fields: it validates each dict with `QueryTaskResult.model_validate`,
records `task_duration_histogram.record(task_result.duration)`, and marks the job FAILED if any
status differs from SUCCEEDED (`query_scheduler.py:982-995`).

`run_query_task` actually returns a tuple `(QueryTaskResult, stdout_str)`
(`executor/query/utils.py:115-121`); `search_entry_point` **discards the stdout half**
(`task_results, _ = run_query_task(...)`, `fs_search_task.py:307`). By contrast `extract_stream`
parses that stdout as newline-delimited JSON (`executor/query/extract_stream_task.py:265`,
`:277-318`). The stdout channel is therefore currently unused by search.

### 3.2 Success / failure contract

Success is decided **solely by the child's exit code** (`executor/query/utils.py:95-102`): a
non-zero return code maps to FAILED, zero maps to SUCCEEDED. There is no stdout parsing and no
result-count check.

Exit codes differ by binary: **clo returns `-1` (255)** on every failure and `0` on success or
`--help`/`--version` (`core/src/clp/clo/clo.cpp:574-616`); **clp-s returns `1`** on every failure
(`core/src/clp_s/clp-s.cpp:406-431`, `:469-577`).

Three failure modes return a FAILED dict without raising:

1. `load_worker_config` returned `None` -> `report_task_failure` (`fs_search_task.py:278-283`).
2. `not task_command` -> log `f"Error creating {task_name} command"` + `report_task_failure`
   (`fs_search_task.py:299-305`). Causes: unsupported storage engine (`:161-173`), S3 archives on
   the CLP engine (`:75-80`), `write_to_file` on the CLP engine (`:82-87`), `generate_s3_url`
   ValueError (`:118-120`).
3. Non-zero child exit (`utils.py:97-108`), with the stderr log file echoed at ERROR level.

Exceptions that **propagate** (Celery marks the task failed and no dict is returned):
`SoftTimeLimitExceeded`, logged and re-raised (`fs_search_task.py:357-359`); any other `Exception`,
logged and re-raised (`:360-362`). The `SqlAdapter`/`Database.model_validate` construction at
`fs_search_task.py:273` happens **before** any DB status write, so a bad DB config propagates with
no status row. `get_credential_env_vars`'s ValueErrors propagate too.

Benign zero-result cases still exit 0: clo returns success without even calling
`output_handler->flush()` when `GrepCore::process_raw_query` yields nothing
(`core/src/clp/clo/clo.cpp:504-518`); clp-s returns success for no-matching-metadata,
no-matching-timestamp-range and no-matching-schema, all before the output handler is constructed
(`core/src/clp_s/clp-s.cpp:223-254` vs handler construction at `:293`).

One asymmetric hard-failure worth designing around: **clp-s exits 1 when `--tge`/`--tle` are given
but the archive has no authoritative timestamp column** (`core/src/clp_s/clp-s.cpp:179-198`),
whereas clo would simply return zero results.

Timeouts: the only bound today is Celery's, `task_soft_time_limit = 600` and `task_time_limit =
1200` seconds read from the config file at import (`celeryconfig.py:23-25`; defaults at
`clp_config.py:387-390`). Neither binary has any self-imposed wall clock or MongoDB timeout, and the
generated `results_cache_uri` carries no query-string options (`clp_config.py:460-461`).

Cancellation: `run_query_task` installs a SIGTERM handler that does
`os.killpg(os.getpgid(task_proc.pid), SIGTERM)`, `os.waitpid`, then `sys.exit(_signo + 128)`
(`executor/query/utils.py:77-90`); the child is spawned with `preexec_fn=os.setpgrp`
(`utils.py:70`). The scheduler triggers it with `revoke(terminate=True)` (`query_scheduler.py:370`).

## 4. What is the implicit output?

Four side-effect surfaces, with distinct writers.

### 4.1 MongoDB result-cache documents — written by the **native binary**, not by Python

Target: database = the path component of `results_cache_uri` (default `clp-query-results`),
collection = the job id string (`fs_search_task.py:223-228`). Both binaries resolve the database via
`mongo_uri.database()` (`core/src/clp_s/ResultsCacheUtils.cpp:14-30`;
`core/src/clp/clo/OutputHandler.cpp:54-61`).

**clp-s document — 6 fields**, emitted in this kvp order by `ResultsCacheOutputHandler::finish()`
(`core/src/clp_s/OutputHandlerImpl.cpp:96-125`), key constants in `namespace
clp_s::constants::results_cache::search` (`core/src/clp_s/archive_constants.hpp:55-67`):

| Field | BSON type | Value |
| --- | --- | --- |
| `orig_file_path` | string | **Always the empty string** — `write()` passes `string_view{}` in both heap branches (`OutputHandlerImpl.cpp:158`, `:170`). |
| `message` | string | The marshalled record. |
| `timestamp` | int64 (`epochtime_t = int64_t`, `core/src/clp_s/Defs.hpp:9`) | Event timestamp in ms. |
| `archive_id` | string | Archive that produced the hit. |
| `log_event_ix` | int64 | Event index within the archive. |
| `dataset` | string | From `--dataset`; **empty string when the flag is omitted** (no presence check, `core/src/clp_s/CommandLineArguments.cpp:871-874`). |
| `_id` | ObjectId | Server-generated; nothing sets it. |

**clo document — 5 different fields** (`core/src/clp/clo/constants.hpp:14-20`; built at
`core/src/clp/clo/OutputHandler.cpp:104-130`): `orig_file_id` (string), `orig_file_path` (string, a
real path here), `log_event_ix` (int64), `timestamp` (int64), `message` (string). **No `archive_id`,
no `dataset`.**

The webui's `SearchResult` interface is the union of both shapes plus a dead `filePath` field
(`webui/packages/client/.../SearchResultsVirtualTable/typings.tsx:23-33`); `getStreamId` picks
`orig_file_id` on the CLP engine and `archive_id` on clp-s
(`webui/packages/client/.../Native/utils.ts:65-69`).

Semantics that matter for a coordinator:

- **`max_num_results` is enforced per task, i.e. per archive.** Each handler keeps a bounded
  min-heap on timestamp (`OutputHandlerImpl.cpp:149-179`; `clo/OutputHandler.cpp:64-96`), so a job
  over K archives can leave up to `K * max_num_results` documents in the shared collection.
- **Insert order is ascending by timestamp within a task**, because the heap is drained top-first;
  `insert_many` fires every `batch_size` (default 1000) documents plus a final partial batch
  (`OutputHandlerImpl.cpp:128-145`; `clo/OutputHandler.cpp:133-150`). There is no cross-task
  ordering guarantee — every reader applies its own sort/limit (webui sorts descending
  `useSearchResults.ts:28-43`; the api-server applies `limit` and **no sort**,
  `api-server/src/client.rs:640-650`).
- clp-s's `flush()` is a deliberate no-op; the heap drains in `finish()` "so that `max_num_results`
  is enforced across all ERTs in the archive" (`core/src/clp_s/OutputHandlerImpl.hpp:178-183`).
- clo additionally uses the heap for an early-exit optimization, `can_skip_file() =
  is_latest_results_full() && get_smallest_timestamp() > it.get_end_ts()`
  (`core/src/clp/clo/OutputHandler.hpp:190-194`, used at `clo.cpp:448`, `:462`).

### 4.2 Other MongoDB collections in the same database

| Collection | Written by | Notes |
| --- | --- | --- |
| `<job_id>` (per job) | native binary | See 4.1. Created by the **webui** (`webui/.../routes/api/search/index.ts:113`) or the **MCP server** (`clp_connector.py:85`); the api-server and the scheduler rely on Mongo auto-creating it on first insert. |
| `results-metadata` (`WebUi.results_metadata_collection_name`, `clp_config.py:704`) | webui and MCP only — **never the search task** | Doc `{_id, errorMsg, errorName, lastSignal, numTotalResults?, queryEngine}` (`webui/packages/common/src/metadata.ts:36-43`); inserted at submit with `lastSignal: RESP_QUERYING` (`search/index.ts:116-122`), finalized with `numTotalResults = min(countDocuments, maxNumResults)` (`search/utils.ts:76-91`). MCP hardcodes the collection name (`clp_connector.py:87-94`). |
| `<aggregation_job_id>` | reducer / clp-s aggregation sink | **Excluded** by assumption (b). The webui submits two jobs per user search (`search/index.ts:90-114`). |
| `stream-files` | `extract_stream` tasks | Not search. The only collection with indexes created by `initialize-results-cache.py:132-137`. |

**Indexes** on a per-job search collection are created only by the webui, at submit time, and only
two: `timestamp-ascending {timestamp:1,_id:1}` and `timestamp-descending {timestamp:-1,_id:-1}`
(`webui/.../routes/api/search/utils.ts:102-128`, called from `index.ts:139-143`).
api-server-submitted and MCP-submitted jobs run with **no indexes**.

**Lifetime.** There is no TTL index anywhere (`expireAfterSeconds` has zero hits repo-wide). A
sweeper drops the whole collection once expired
(`job_orchestration/garbage_collector/search_result_garbage_collector.py:40-66`), governed by
`ResultsCache.retention_period` (default 60 minutes, `clp_config.py:458`) and
`GarbageCollector.sweep_interval.search_result` (default 30 minutes, `clp_config.py:709-713`); the
collector only runs at all when `retention_period is not None` (`garbage_collector.py:56-67`). Two
hard constraints it imposes:

1. **Collection names must be all digits** — `if not job_id.isdigit(): continue`
   (`search_result_garbage_collector.py:51-52`) and `int(job_id)` at `:61`. UUID-named collections
   would never be collected.
2. **`_id` must be an ObjectId** — `_get_latest_doc_timestamp` raises `ValueError` otherwise
   (`search_result_garbage_collector.py:27-30`), and that exception terminates the collector task
   for the life of the process (`:81-83`).

### 4.3 Metadata-DB rows (`query_tasks`)

The scheduler INSERTs one row per archive with only `(job_id, archive_id)` and commits
(`query_scheduler.py:537-549`). The **task** then issues UPDATEs via `update_query_task_metadata`
(`executor/query/utils.py:124-141`), which builds `UPDATE query_tasks SET k="v", ... WHERE id =
<task_id>` by raw f-string interpolation:

- before `Popen`: `{status: RUNNING, start_time}` (`utils.py:62-65`);
- after the child exits: `{status: SUCCEEDED|FAILED, start_time, duration}` (`utils.py:111-113`);
- on early failure: `{status: FAILED, duration: 0, start_time}` (`utils.py:29-39`).

**These UPDATEs appear never to be committed.** `sql_adapter.create_connection(True)` binds `True`
to `disable_localhost_socket_connection`, the first positional parameter — it is *not* an autocommit
flag (`clp-py-utils/clp_py_utils/sql_adapter.py:69-73`). Autocommit comes from
`Database.auto_commit`, which defaults to `False` (`clp_config.py:210`) and is forwarded as
`"autocommit": self.auto_commit` into the connector (`clp_config.py:253`); Helm renders
`auto_commit: false` explicitly (`package-helm/templates/configmap.yaml:115`).
`update_query_task_metadata` never calls `db_conn.commit()` before the `closing()` context closes
the connection. The compression executor does commit explicitly
(`executor/compress/compression_task.py:564`, `:697`). Verified statically only — see open
questions.

Schema (`clp-py-utils/clp_py_utils/initialize-orchestration-db.py:152-170`): `id BIGINT
AUTO_INCREMENT`, `status INT DEFAULT PENDING`, `creation_time DATETIME(3)`, `start_time DATETIME(3)
NULL`, `duration FLOAT NULL`, `job_id INT`, `archive_id VARCHAR(255) NULL`, FK to `query_jobs(id)`.
There is **no `dataset` column**. `query_jobs` (`:131-148`) has **no `spider_id` column**, unlike
`compression_jobs` (`:65-88`).

### 4.4 Filesystem side effects

On the result-cache path the task creates **exactly one file**: `get_task_log_file_path` does
`(<CLP_LOGS_DIR>/<job_id>).mkdir(exist_ok=True, parents=True)` and returns
`<CLP_LOGS_DIR>/<job_id>/<task_id>-clo.log` (`executor/query/utils.py:23-26`) — the `-clo.log`
suffix is used for the clp-s engine too. It is opened `"w"` as the child's stderr (`utils.py:59-60`,
`:73`), closed at `:104`, echoed into the logger at ERROR on failure / INFO on success (`:105-108`,
via `executor/utils.py:11-18`, which logs only when the file is non-empty), and **never deleted**.

Read-only filesystem prerequisites: `<CLP_HOME>/bin/{clo,clp-s}`; the archive directory
(`/var/data/archives`, mounted `:ro` in Compose, `docker-compose-all.yaml:464`; a PVC in Helm,
`query-worker-deployment.yaml:66-71`); the config file at `/etc/clp-config.yaml`; and `~/.aws` at
`/opt/clp/.aws` only for S3 archives with profile/default auth (`docker-compose-all.yaml:465`).
`tmp_directory` is never used on the search path.

### 4.5 Telemetry

With probability `query_trace_sampling_probability` (default 0.01) the clp-s child gets
`--enable-telemetry` and the two env vars from 2.2, and publishes OTLP spans named by
`OTEL_SERVICE_NAME` (default `clp-search`). The Python task also emits structlog context `{job_id,
task_id, query_job_type="SEARCH_OR_AGGREGATION", archive_id, dataset?, query, query_hash}`
(`fs_search_task.py:42-65`; `to_str()` at `scheduler/constants.py:25-26`). Note the config blob is
msgpack-unpacked and validated **twice** per task, at `fs_search_task.py:59` and `:287`.

## 5. Mapping onto the Spider TDL task model

The concrete template is `components/clp-tdl-package`, whose compression task is a Rust
`#[task]`-annotated function registered through `spider_tdl::register_tdl_package!`
(`clp-tdl-package/src/lib.rs:28-32`). Note this is a **different** template from the legacy Python
Spider adapter `job_orchestration/executor/compress/spider_compress.py` — the Rust package is the
current one and is what a search task should follow.

### 5.1 How the compression TDL task is shaped

```rust
#[task(name = "compression::clp_s_s3_compress")]
pub(crate) fn s3_compress_task(
    ctx: TaskContext,
    clp_s_option: ClpSCompressionOption,
    dataset: Option<String>,
    input_source: S3InputSource,
) -> Result<CompressionTaskOutput, TdlError>
```

(`clp-tdl-package/src/task/compression/mod.rs:13-28`). It immediately delegates to a pure worker
function taking `&SpiderTaskExecutorConfig` from process-global state (`mod.rs:20-27`; worker at
`src/task/compression/compress.rs:56-170`) and maps any `anyhow::Error` into
`TdlError::ExecutionError(format!("{e:#}"))`.

Argument types are declared structurally on the coordinator side and payloads are msgpack:

```rust
inputs: vec![
    DataTypeDescriptor::Value(ValueTypeDescriptor::struct_from_name("ClpSCompressionOption")?),
    DataTypeDescriptor::Value(ValueTypeDescriptor::struct_from_name("Option<String>")?),
    DataTypeDescriptor::Value(ValueTypeDescriptor::struct_from_name("S3InputSource")?),
],
outputs: vec![DataTypeDescriptor::Value(
    ValueTypeDescriptor::struct_from_name("CompressionTaskOutput")?)],
...
inputs.push(TaskInput::ValuePayload(rmp_serde::to_vec(&clp_s_option)?));
```

(`compression-coordinator/src/compression_job_submitter/spider.rs:65-90`). The task-function names
are string constants that "must be kept in sync with the TDL package definitions"
(`spider.rs:47-51`). Outputs are collected by a **termination task** — `#[task(name =
"compression::commit")]` calls `ctx.get_task_graph_outputs()` and `rmp_serde::from_slice` on each
output blob (`clp-tdl-package/src/task/compression/mod.rs:30-57`).

### 5.2 What each Python input becomes

| Python search-task input | TDL equivalent | Rationale |
| --- | --- | --- |
| `job_config_blob: bytes` (msgpack `SearchJobConfig`) | **Task argument**, `ValueTypeDescriptor::struct_from_name("SearchJobConfig")` + `TaskInput::ValuePayload(rmp_serde::to_vec(&cfg))` | The Rust mirror already exists at `clp-rust-utils/src/job_config/search.rs:15-26`, with `aggregation_config: Option<()>`. Under scope (a)+(b) the coordinator should pass a *narrowed* struct instead — see 5.5. |
| `archive_id: str` (+ `dataset`) | **Task argument**, one task per archive (a new `task_io::search::ArchiveInput`-style struct holding `archive_id` and `dataset: Option<String>`) | Preserves the existing 1-archive-per-task granularity and mirrors `S3InputSource` being the per-task-varying argument on the compression side. |
| `job_id: str` | **Task argument** (an explicit numeric CLP query-job id), *not* `ctx.job_id` | `ctx.job_id` is the Spider job id; the Mongo collection name must remain the numeric `query_jobs.id` because the GC filters on `isdigit()` (`search_result_garbage_collector.py:51-52`) and because every reader derives the collection name from the SQL job id (`api-server/src/client.rs:638-639`; `webui/.../search/index.ts:113`). |
| `task_id: int` | **Dropped** or replaced by `ctx.task_id` / `ctx.task_instance_id` | The compression task logs `ctx.job_id`, `ctx.task_id`, `ctx.task_instance_id` (`compress.rs:69-75`) and uses them to name tmp files (`compress.rs:64-67`). Whether `query_tasks` rows survive at all is a design decision (see 6). |
| `clp_metadata_db_conn_params: dict` | **Config, not an argument** | `SpiderTaskExecutorConfig` already carries `database: Database` (`clp-rust-utils/src/clp_config/package/config.rs:61-66`, `:140-149`). The search task does not need the DB at all if `query_tasks` writes are dropped. |
| `results_cache_uri: str` | **Config field to add** | `SpiderTaskExecutorConfig` currently has only `package`, `archive_output`, `tmp_directory`, `database` (`config.rs:61-66`). A `ResultsCache` mirror already exists at `config.rs:274-290` (host/port/db_name, no `retention_period`/`stream_collection_name`) — it needs to be added to `SpiderTaskExecutorConfig` and to the executor's config YAML. Alternatively pass the URI as a task argument; config is more consistent with how `archive_output` is handled. |
| `worker_config.package.storage_engine`, `archive_output.storage{.type,.directory,.staging_directory,.s3_config}` | **Config** | Already present: `config.package`, `config.archive_output`, plus the resolvers `abs_archive_output_staging(clp_home)` (`config.rs:87-95`) and `resolve_dataset_name` (`clp-rust-utils/src/dataset.rs`, used at `compress.rs:104`). |
| `query_worker.query_trace_sampling_probability` | **Config field to add** if telemetry sampling is kept | Not currently in `SpiderTaskExecutorConfig`. |
| AWS credentials | **Resolved in-task, injected into the child env** | Same pattern as compression: `s3_credential_env(&runtime, region, &aws_authentication)` then `Command::envs(credential_env)` (`compress.rs:99`, `:821-827`). |

### 5.3 What stays an environment variable

The TDL package reads only three, all at package init, all process-global:

| Variable | Read at | Required? |
| --- | --- | --- |
| `CLP_CONFIG_PATH` | `clp-tdl-package/src/common.rs:167-171` (const at `:138`), via `init_config` (`:41-48`) | **Required** — `std::env::var` error is contexted and fails `package_init` (`src/lib.rs:18-25`). |
| `CLP_HOME` | `common.rs:63-66` (const at `:141`), via `init_clp_home` (`:59-67`) | **Required** — same failure path. |
| `RUST_LOG` | `tracing_subscriber::EnvFilter::from_default_env()` (`common.rs:86`) | Optional. Deployments set `RUST_LOG: "INFO"` for the Rust services (`docker-compose-all.yaml:560`, `:601`; `package-helm/templates/compression-coordinator-deployment.yaml:56`). |

So of the Python task's four env vars, `CLP_CONFIG_PATH` and `CLP_HOME` carry over unchanged,
`CLP_LOGGING_LEVEL` becomes `RUST_LOG`, and **`CLP_LOGS_DIR` disappears**: the Rust task pipes the
child's stdout and stderr and logs stderr through `tracing` rather than writing a per-task log file
(`compress.rs:821-881`). `BROKER_URL`/`RESULT_BACKEND` disappear with Celery.

The telemetry env vars (2.3) are read by the *child* binary and must still be present in the Spider
task executor's environment if telemetry is wanted; `CLP_QUERY_ID`/`CLP_TASK_ID` would be injected
per-invocation exactly as today.

I could not find a repo-side deployment manifest for the Spider task executor itself — Compose and
Helm only set `CLP_CONFIG_PATH`/`CLP_HOME` on the compression-worker and query-worker containers
(`docker-compose-all.yaml:290-291`, `:451-452`;
`package-helm/templates/compression-worker-deployment.yaml:39-41`). Where the executor's env is
defined is an open question.

### 5.4 What the return type becomes

The Python `QueryTaskResult` dict is largely Celery bookkeeping and does not translate:

- `status` is redundant — Spider represents failure as `Err(TdlError::ExecutionError(...))`
  (`clp-tdl-package/src/task/compression/mod.rs:27`), exactly as the compression task does.
- `task_id` is available from `ctx`.
- `duration` is better emitted as a `tracing` field / OTel histogram than as a return value.

The useful shape is a msgpack `SearchTaskOutput` struct in a new
`clp-rust-utils/src/task_io/search.rs`, mirroring `CompressionTaskOutput`
(`clp-rust-utils/src/task_io/compression.rs:28-33`), carrying whatever a termination task needs to
finalize the job. Concretely, the information that exists today but is *thrown away* and that a
coordinator would need for a correct early-exit:

- `archive_id` (echo);
- `num_results_written` — **not currently available**: the binaries do not report it, the Python
  task discards stdout (`fs_search_task.py:307`), and the scheduler recovers it only by
  `count_documents({})` against Mongo (`query_scheduler.py:959`);
- `min_timestamp_written` / `max_timestamp_written` — likewise not currently emitted.

Emitting those would require a C++ change (the stdout channel is free — clo already uses it for
`--print-ir-stats` ndjson, `core/src/clp/clo/clo.cpp:229-234`) or would have to be replaced by the
same Mongo count/sort the scheduler does today.

A `search::commit`-style termination task is the natural place for the per-job finalization the
Python scheduler does inline: set `query_jobs.status`, `num_tasks_completed`, `duration`
(`query_scheduler.py:1042-1052`), and decide SUCCEEDED vs FAILED from the per-task outputs
(`query_scheduler.py:982-995`).

### 5.5 Narrowing the config struct under the stated scope

Under (a)+(b), four of the ten `SearchJobConfig` fields are dead weight and three of them are
*hazards* because the Python code branches on them before reaching the result-cache handler. A
result-cache-only TDL task should take a struct with exactly:

`query_string: String`, `max_num_results: NonZeroU32`, `begin_timestamp: Option<i64>`,
`end_timestamp: Option<i64>`, `ignore_case: bool`, `path_filter: Option<String>` (clo only).

Dropping `network_address`, `aggregation_config` and `write_to_file` makes the result-cache path
unconditional rather than a fallback `else`, and typing `max_num_results` as non-zero encodes the
constraint the two binaries enforce at parse time (`clo/CommandLineArguments.cpp:694-696`;
`clp_s/CommandLineArguments.cpp:1283-1285`).

### 5.6 Coordinator-side schema and submission

Status and schema design should follow a minimal-durability rule: persist a job
status or supporting column when an external consumer needs to observe it or a
restarted coordinator needs it to recover without ambiguity. Keep transient,
reconstructible phases—such as graph construction, Spider polling, and commit
verification—in the job handle rather than adding public statuses or writing
MySQL on every phase change. This reduces unnecessary updates and row
contention while still allowing fault tolerance. `spider_id` is a necessary
durable addition because it distinguishes reattachment from resubmission after
a crash; a separate `POLLING_SPIDER` status would not add recovery information.

- `query_jobs` has no `spider_id` column while `compression_jobs` does
  (`initialize-orchestration-db.py:131-148` vs `:65-88`) — a schema change is needed to track the
  Spider job id, mirroring `compression-coordinator/src/job_handle.rs:460-463`.
- Consumers observe completion **only** by polling `query_jobs.status` until it reaches {SUCCEEDED,
  FAILED, CANCELLED, KILLED}: webui every 500 ms (`webui/.../QueryJobDbManager/index.ts:95-129`,
  interval at `typings.ts:4`), the CLI (`clp-package-utils/.../native/utils.py:97-122`), the MCP
  server every 1 s (`clp-mcp-server/clp_mcp_server/constants.py:7`). A search-coordinator **must**
  preserve this contract; there is no notification channel out of the scheduler today.
- Cancellation is `query_jobs.status = CANCELLING` set by the submitter
  (`webui/.../QueryJobDbManager/index.ts:75-83`) and polled by the scheduler
  (`query_scheduler.py:477-534`). The Spider equivalent has to be designed.

## 6. Open questions / decisions for the search-coordinator

Genuinely unresolved, in rough priority order.

1. **Are the `query_tasks` status UPDATEs actually being rolled back today, and should the Spider
   task keep that table at all?** `update_query_task_metadata` never commits and the connection is
   not autocommit (`executor/query/utils.py:124-141`; `sql_adapter.py:69-73`; `clp_config.py:210`,
   `:253`). Verified statically only. The query scheduler itself relies on Celery results, not this
   table (`query_scheduler.py:982-995`), so it may be dead weight — but the webui surfaces
   `num_tasks`/`num_tasks_completed`, and it is unclear who else reads `query_tasks.status`.
2. **Does the search-coordinator serve a different API contract than the current api-server
   default?** Result-cache output requires `buffer_results_in_mongodb: true` today; the default is
   file output (`api-server/src/client.rs:203-207`, `:219`). Either the coordinator serves only
   explicit result-cache callers, or the api-server default changes.
3. **Job-id namespace.** Result collections must be all-digits for GC
   (`search_result_garbage_collector.py:51-52`) and every reader derives the collection name from
   the SQL `query_jobs.id`. A Spider-native job id cannot replace it. Confirm the coordinator keeps
   allocating a numeric `query_jobs.id` and passes it as a task argument.
4. **Who creates the results collection and its indexes?** Today only the webui does both
   (`search/index.ts:113`, `utils.ts:102-128`); the MCP creates the collection without indexes
   (`clp_connector.py:85`); the api-server does neither. If the coordinator takes ownership, it also
   fixes the api-server's unindexed-collection case; if not, api-server jobs keep doing collection
   scans for the scheduler's timestamp query.
5. **Early termination without a reducer.** Today it is `found_max_num_latest_results`
   (`query_scheduler.py:951-973`), which requires (i) archives ordered `end_timestamp DESC`
   (`query_scheduler.py:575`, `:609`), (ii) sequential sub-job rounds so there is a "highest end
   timestamp still unsearched" (`query_scheduler.py:1005-1013`), and (iii) reading Mongo between
   rounds. A Spider task graph that dispatches all archives at once loses (ii) entirely. Also note
   the existing query looks wrong: `find(sort=..., limit=...).sort(...).limit(1)` — PyMongo
   `Cursor.sort()`/`Cursor.limit()` replace the `find()` arguments, so the comparison is against the
   **global minimum** timestamp, not the minimum of the top N. Not executable in this environment;
   worth a targeted test before porting.
6. **Per-archive vs global `max_num_results`.** Each task independently writes up to
   `max_num_results` documents (`OutputHandlerImpl.cpp:149-179`), so a K-archive job can leave
   K times `max_num_results` documents; the webui then reports `min(countDocuments, maxNumResults)`
   (`search/utils.ts:84-87`) and the api-server applies `limit` with **no sort**
   (`api-server/src/client.rs:640-650`). Whether the coordinator should trim/merge, and whether the
   returned set must be globally top-N-by-timestamp, is undecided.
7. **Error propagation and per-archive failure policy.** Today one FAILED task fails the whole job
   (`query_scheduler.py:991-995`). Two cases argue for a per-archive skip instead: clp-s exits 1
   when timestamp filters are given and the archive has no authoritative timestamp column
   (`clp-s.cpp:179-198`), and exit codes differ between binaries (clo 255, clp-s 1). Should the TDL
   task normalize exit codes, and should the graph tolerate partial failure?
8. **Timeouts.** The only bound today is Celery's 600 s soft / 1200 s hard
   (`celeryconfig.py:23-25`). Neither binary self-limits, and the generated `results_cache_uri`
   carries no MongoDB timeout/write-concern options (`clp_config.py:460-461`). Should the
   coordinator own URI construction (adding `serverSelectionTimeoutMS`, `socketTimeoutMS`, `w`) and
   a per-task deadline?
9. **`directConnection`.** The webui (`mongo.ts:6-12`), api-server (`client.rs:256-259`) and
   `initialize-results-cache.py:129` all use `directConnection=true` against the single-node replica
   set `rs0` (`initialize-results-cache.py:79-89`), but `ResultsCache.get_uri()` does not
   (`clp_config.py:460-461`). Whether a Rust writer needs it in-cluster is unverified.
10. **Document-schema normalization.** clo and clp-s write different shapes into the same cache
    (4.1); only `message` is consumed by the api-server, while the MCP reader hard-depends on
    `archive_id` and `log_event_ix` (`clp_connector.py:163-171`). Should a Spider search task
    normalize?
11. **`--dataset` on the clo engine.** `fs_search_task.py:230-231` appends `--dataset` for any
    engine, but clo has no such option and rejects unknown handler options
    (`clp/cli_utils.cpp:6-23`). The invariant "datasets set iff engine is clp-s" is currently upheld
    only by convention (`api-server/src/client.rs:283-288`). Should the coordinator enforce it?
12. **Spider task-executor deployment.** No Compose or Helm manifest in this repo defines the Spider
    task executor's environment, so where `CLP_CONFIG_PATH`, `CLP_HOME`, `RUST_LOG` and the OTel
    variables are set for it could not be determined from source.
13. **Cancellation semantics under Spider.** Today cancellation is polled from `query_jobs.status =
    CANCELLING` and implemented as `revoke(terminate=True)` plus a SIGTERM process-group kill
    (`query_scheduler.py:477-534`; `executor/query/utils.py:77-90`). There is also a confirmed gap:
    a job that is CANCELLING but absent from the scheduler's in-memory `active_jobs`
    (`query_scheduler.py:485-488`) is never transitioned out — `fetch_new_query_jobs` selects only
    PENDING (`:399-409`) and `kill_hanging_jobs` only RUNNING (`scheduler/utils.py:49-55`). A job
    cancelled while PENDING, or a CANCELLING row surviving a scheduler restart, stays CANCELLING
    indefinitely.
14. **Should the task report result statistics?** stdout is free on the search path
    (`fs_search_task.py:307`). Emitting `{num_results, min_ts, max_ts}` per archive would let a
    coordinator do early termination without reading Mongo — but it requires a C++ change.
