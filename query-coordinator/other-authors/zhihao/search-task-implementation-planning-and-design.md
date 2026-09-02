> Author: Zhihao · [Original Notion page](https://app.notion.com/p/3cc04e4d9e6b80dbb110d7ae3ed6e4eb?pvs=204)

# `clp-s` Search Task Implementation Plan (`clp-tdl-package`)
Target: implement `clp_s_query_to_results_cache_task` in `components/clp-tdl-package/src/task/query/mod.rs`, mirroring the Celery task in `components/job-orchestration/job_orchestration/executor/query/fs_search_task.py`.
Every claim below was read out of the tree and independently re-verified against the cited file/line.
> **Revision 4 (2026-08-30)** — incorporates review. Changed since revision 1: `dataset` becomes optional (§5.3), `max_num_results` becomes optional (§5.4), `deny_unknown_fields` on all wire types (§5), and §3/§4 answer the explanation questions. **§6 (telemetry) is optional and deferred to a future PR — it is not part of this implementation.** §8 is the test plan; §10 records the settled decisions.
---
## 1. Evaluation of the stated premises
### 1.1 Confirmed
<table header-row="true">
<tr>
<td>Premise</td>
<td>Verdict</td>
</tr>
<tr>
<td>Mirror the Python Celery search task</td>
<td>Confirmed. `fs_search_task.py:95-231` is the reference; arg order is load-bearing and we reproduce it.</td>
</tr>
<tr>
<td>Drop aggregation support</td>
<td>Confirmed and clean. Aggregation is entirely the `reducer` output handler plus `--count`/`--count-by-time`/`--min`/`--max`/`--unique` (`CommandLineArguments.cpp:782-801`). Nothing else depends on it.</td>
</tr>
<tr>
<td>Load `SpiderTaskExecutorConfig` from env</td>
<td>Confirmed. Already loaded process-wide from `CLP_CONFIG_PATH` and cached; the task just calls `crate::common::spider_task_executor_config()` exactly as `compression/mod.rs:22` does. **No new loading code is needed.**</td>
</tr>
<tr>
<td>Config decides FS vs S3 archive resolution</td>
<td>Confirmed — `config.archive_output.storage` is the `ArchiveOutputStorage::{Fs, S3}` discriminant.</td>
</tr>
<tr>
<td>S3 needs clp-s set up with credentials</td>
<td>Confirmed, and it is env-var-only. clp-s exposes **no** credential flags; it reads `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` (`InputConfig.hpp:18-20`). `s3_credential_env` in `compress.rs:364-414` already produces exactly this vector.</td>
</tr>
<tr>
<td>Add an `OutputHandle` enum, reject `File`</td>
<td>Confirmed as a good fit — it maps 1:1 onto clp-s's own output-handler subcommands (`file`, `network`, `reducer`, `results-cache`, `stdout`, `CommandLineArguments.cpp:29-33`).</td>
</tr>
<tr>
<td>`OutputHandle` must be a task parameter, with a struct variant</td>
<td>Confirmed; rationale retained below as §1.2a/b.</td>
</tr>
</table>
### 1.2 Corrections to the original brief
**(a) The original signature could not build the command line.** `clp-s`'s `results-cache` handler hard-requires `--uri`; `parse_results_cache_output_handler_options` throws `"uri must be specified."` (`CommandLineArguments.cpp:1257-1286`). `SpiderTaskExecutorConfig` has exactly four fields — `package`, `archive_output`, `tmp_directory`, `database` (`clp_config/package/config.rs:62-67`) — and **no ****`results_cache`**. So no MongoDB URI is reachable from the config; `OutputHandle` must be a task parameter. This matches Python, where `results_cache_uri` is a per-task Celery argument (`fs_search_task.py:341`) sourced from `clp_config.results_cache.get_uri()` (`query_scheduler.py:1343`). **Settled: signature change approved.**
**(b) ****`ResultCache(url: String)`**** cannot be written as a tuple variant.** The repo's enum convention is internally tagged — `#[serde(tag = "type")]`, see `AwsAuthentication` at `clp_config/s3_config.rs:15-25`. Serde cannot internally-tag a newtype variant wrapping a scalar; it compiles and fails at *serialize* time. It must be a struct variant. **Settled: struct variant confirmed.** Two refinements: spell it `ResultsCache` (plural, matching `clp_config::package::config::ResultsCache` and clp-s's `results-cache` handler name), and type the field `NonEmptyString`, since clp-s rejects an empty URI at runtime (`"uri cannot be an empty string."`, `CommandLineArguments.cpp:1268-1270`).
**(c) The *task* needs no Rust MongoDB client — the coordinator does.** clp-s writes to MongoDB itself: `connect_to_results_cache` uses mongocxx and resolves the database from the URI path (`ResultsCacheUtils.cpp:14-30`), and `ResultsCacheOutputHandler::finish()` does the batched `insert_many` (`OutputHandlerImpl.cpp:89-147`). Our task only passes `--uri` and `--collection`. The coordinator is a different story and **does** need `mongodb`: mongocxx's `insert_many` creates the collection implicitly but creates **no index**, so the coordinator must create the per-job collection and its `timestamp`-descending index up front, exactly as `query_scheduler.py:362-385` does — the `api-server` read path sorts on those fields (`client.rs:692-697`). That work is out of scope for this PR; it is noted here so it does not get lost.
Consequently the runner must be modelled on `run_log_converter` (`compress.rs:742-789` — spawn, drain stderr, check exit status), **not** on `run_clp_s` (`compress.rs:813-881`), whose line-by-line stdout streaming exists solely to parse `--print-archive-stats` JSON.
---
## 2. Ground truth: the `clp-s s` CLI
Usage (`print_search_usage`, `CommandLineArguments.cpp:1319-1324`):
```javascript
s [OPTIONS] ARCHIVES_DIR KQL_QUERY [OUTPUT_HANDLER [OUTPUT_HANDLER_OPTIONS]]
```
Positionals are order-enforced (`:731-733`): `archive-path` (1), `query` (1), `output-handler-args` (-1).
Match controls (`:737-774`) — verified spellings:
<table header-row="true">
<tr>
<td>Flag</td>
<td>Value</td>
<td>Notes</td>
</tr>
<tr>
<td>`--tge TS`</td>
<td>yes</td>
<td>"UNIX epoch timestamp \>= TS **ms**" (`:740`)</td>
</tr>
<tr>
<td>`--tle TS`</td>
<td>yes</td>
<td>"... \<= TS **ms**" (`:744`)</td>
</tr>
<tr>
<td>`--ignore-case`, `-i`</td>
<td>no</td>
<td>bool switch</td>
</tr>
<tr>
<td>`--archive-id ID`</td>
<td>yes</td>
<td>"in a **subdirectory of** archive-path" (`:755-757`)</td>
</tr>
<tr>
<td>`--auth AUTH_METHOD`</td>
<td>yes</td>
<td>`s3` \\</td>
</tr>
<tr>
<td>`--enable-telemetry`</td>
<td>no</td>
<td>**not used in this PR** — deferred, see §6</td>
</tr>
<tr>
<td>`--projection COLS...`</td>
<td>multitoken</td>
<td>unused</td>
</tr>
</table>
`results-cache` handler options (`:848-875`, defaults at `CommandLineArguments.hpp:36-42`): `--uri` (required), `--collection` (required), `--batch-size` (default 1000), `--max-num-results` (default **1000**), `--dataset` (default **empty string**).
Flags that do **not** exist for search: `--raw`, `--file-path`, `--mongodb-uri`, `--mongodb-collection` (the last two are extract-only).
**Target command lines:**
```bash
# FS archives
$CLP_HOME/bin/clp-s s <archives_dir>/<dataset> --archive-id <archive_id> \
  <query_string> [--tge N] [--tle N] [--ignore-case] \
  results-cache --uri <mongodb_uri> --collection <query_job_id> \
    --max-num-results <N> --dataset <dataset>

# S3 archives (plus AWS_* env vars on the child)
$CLP_HOME/bin/clp-s s <s3_object_url> --auth s3 \
  <query_string> [--tge N] [--tle N] [--ignore-case] \
  results-cache --uri <mongodb_uri> --collection <query_job_id> \
    --max-num-results <N> --dataset <dataset>
```
Two traps:
- **`--archive-id`**** is filesystem-only.** `validate_archive_paths` does `std::filesystem::path(archive_path) / archive_id` and throws `"Requested archive does not exist"` (`:135-163`). The S3 branch must pass the object URL positionally with `--auth s3` and **no** `--archive-id` — a Network-source path is emplaced as exactly one archive with no stat (`InputConfig.cpp:119-128`).
- **`abs_archive_output_staging`**** returns ****`staging_directory`**** when storage is S3** (`config.rs:88-96`). Using it unconditionally makes the S3 path silently search an empty local directory: zero results, exit code 0. A wrong answer, not a crash.
---
## 3. Review questions, answered
### 3.1 With `--auth s3`, does clp-s put the archive ID in each result document?
**Yes, and it derives it from the URL.** Every results-cache document carries an `archive_id` field (`OutputHandlerImpl.cpp:110-114`, alongside `orig_file_path`, `message`, `timestamp`, `log_event_ix`, `dataset`). The value comes from `m_archive_reader->get_archive_id()` (`search/Output.cpp:99`), which `ArchiveReader::open` sets via `get_archive_id_from_path` (`ArchiveReader.cpp:30`). That function is source-aware (`InputConfig.cpp:141-151`):
<table header-row="true">
<tr>
<td>Source</td>
<td>Archive ID</td>
</tr>
<tr>
<td>`InputSource::Network` (S3)</td>
<td>`UriUtils::get_last_uri_component(url)`</td>
</tr>
<tr>
<td>`InputSource::Filesystem`</td>
<td>`FileUtils::get_last_non_empty_path_component(path)`</td>
</tr>
</table>
So on the S3 path the archive ID is **the last component of the object key**, not something read out of archive metadata. Our key is `{key_prefix}{dataset}/{archive_id}`, whose last component is exactly `archive_id` — correct by construction. On the FS path clp-s resolves `<archives_dir>/<dataset>` + `--archive-id <id>` into a concrete path whose last component is again `<id>`.
The consequence worth writing down: **if we ever build a key whose last component is not the archive ID, every document in that job silently gets a wrong ****`archive_id`** — no error, no warning. This is the strongest argument for the two sides (compression's write, query's read) sharing a single key builder, which is §3.2.
One related clarification: **`--dataset`**** is a pure label.** Its help text is literally "The dataset name to include in each result document" (`CommandLineArguments.cpp:870-874`) and it is only ever copied into the document (`OutputHandlerImpl.cpp:116-121`, `:155-176`). It does **not** participate in archive resolution. The dataset affects resolution only because *we* put it in the archive path or object key.
### 3.2 What is `ArchiveOutput::dataset_archive_object_key`?
It does not exist yet — it is a proposed **hoist**, not new logic.
Today `create_archive_s3_key` is a module-private helper in the compression task (`compress.rs:623-632`):
```rust
fn create_archive_s3_key(
    archive_output: &ArchiveOutput,
    dataset: Option<&str>,
    archive_id: &str,
) -> String {
    format!(
        "{}/{archive_id}",
        archive_output.dataset_archive_storage_directory(dataset)
    )
}
```
`dataset_archive_storage_directory` (`config.rs:366-384`) returns the storage base joined with the dataset — `s3_config.key_prefix` for S3, `directory` for `Fs` — resolving `None` to `default` through `resolve_dataset_name`. The search task needs **the identical key** to find what compression wrote. The proposal is to move `create_archive_s3_key` onto `ArchiveOutput` as an inherent method sitting next to `dataset_archive_storage_directory`:
```rust
impl ArchiveOutput {
    /// Derives the S3 object key for an archive in a dataset.
    #[must_use]
    pub fn dataset_archive_object_key(&self, dataset: Option<&str>, archive_id: &str) -> String {
        format!("{}/{archive_id}", self.dataset_archive_storage_directory(dataset))
    }
}
```
Why this and not the alternatives:
- Copying it into `search.rs` duplicates the archive-layout contract in two crates. Given §3.1, a drift between the two spellings corrupts `archive_id` in results *silently*.
- Making `compress.rs`'s helper `pub(crate)` does not work — `compress` lives under `mod task;` (`lib.rs:4`), so `clp-rust-utils` could not see it, and the layout knowledge belongs with the type that owns the layout.
- The name says "object key" because it is only meaningful for S3; the FS branch uses `dataset_archive_storage_directory` plus `--archive-id` instead of a joined path.
**Verified byte-for-byte against Python.** `fs_search_task.py:110-111` builds `f"{s3_config.key_prefix}{dataset}/{archive_id}"` with no separator after `key_prefix`, because `key_prefix` is validated to end in `/` (`clp_config.py:598-605`). `Path::new("pre/").join("default")` normalizes to `pre/default`, so both sides produce `pre/default/<archive_id>`. No off-by-one slash.
This is step 3 of §7 and can land as its own small commit.
---
## 4. Wire-format constraint (verified empirically)
Task parameters do **not** travel as one struct. Each parameter is msgpack-encoded separately with compact `rmp_serde::to_vec` (see the compression submitter, `compression-coordinator/src/compression_job_submitter/spider.rs:87-89`) and framed; the receiving side positionally decodes one payload per field of the generated params struct (`spider-core/src/types/io.rs:297-301`). Compact msgpack encodes structs as **arrays**, which is exactly the case where internally-tagged enums usually break.
They do not break here, and this was confirmed rather than reasoned about — a throwaway round-trip test was compiled and run in `clp-rust-utils`:
```rust
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "type")]
pub enum OutputHandle {
    #[serde(rename = "results_cache")]
    ResultsCache { uri: NonEmptyString },
    #[serde(rename = "file")]
    File,
}
```
Both variants round-tripped through `rmp_serde::to_vec` → `rmp_serde::from_slice`. (The probe module has been removed.) This holds for struct and unit variants because serde's internal tagging emits a msgpack **map** for the variant itself; only a newtype variant wrapping a scalar fails, which is §1.2b. `AwsAuthentication` already relies on this over the same wire, inside `S3InputSource`.
**`deny_unknown_fields`**** is compatible with ****`tag = "type"`** — that combination is what the probe tested. It is *not* compatible with `#[serde(flatten)]`, and none of these types use flatten.
---
## 5. Items to add *in addition to* the task body
<table header-row="true">
<tr>
<td>#</td>
<td>Item</td>
<td>Location</td>
</tr>
<tr>
<td>1</td>
<td>`OutputHandle` enum</td>
<td>`clp-rust-utils/src/task_io/query.rs` (new type)</td>
</tr>
<tr>
<td>2</td>
<td>`output_handle: OutputHandle` task parameter</td>
<td>`clp-tdl-package/src/task/query/mod.rs`</td>
</tr>
<tr>
<td>3</td>
<td>`dataset` → `Option<NonEmptyString>`</td>
<td>both of the above</td>
</tr>
<tr>
<td>4</td>
<td>`max_num_results` → `Option<NonZeroU32>`</td>
<td>`clp-rust-utils/src/task_io/query.rs`</td>
</tr>
<tr>
<td>5</td>
<td>`#[serde(deny_unknown_fields)]` on every `task_io::query` type</td>
<td>`clp-rust-utils/src/task_io/query.rs`</td>
</tr>
<tr>
<td>6</td>
<td>Delete orphaned `QueryTaskOutput` import</td>
<td>`clp-tdl-package/src/task/query/mod.rs:4`</td>
</tr>
<tr>
<td>7</td>
<td>Fix microseconds → milliseconds doc comments</td>
<td>`clp-rust-utils/src/task_io/query.rs:17,20`</td>
</tr>
<tr>
<td>8</td>
<td>New `task::clp_s` module (hoist 2 helpers)</td>
<td>`clp-tdl-package/src/task/clp_s.rs` (new)</td>
</tr>
<tr>
<td>9</td>
<td>New `task::query::search` worker module</td>
<td>`clp-tdl-package/src/task/query/search.rs` (new)</td>
</tr>
<tr>
<td>10</td>
<td>`ArchiveOutput::dataset_archive_object_key`</td>
<td>`clp-rust-utils/src/clp_config/package/config.rs`</td>
</tr>
<tr>
<td>11</td>
<td>README `### Query` section</td>
<td>`clp-tdl-package/README.md`</td>
</tr>
<tr>
<td>12</td>
<td>**No** new Cargo dependency</td>
<td>`Cargo.toml` unchanged</td>
</tr>
</table>
Telemetry is **not** in this list; see §6.
### 5.1 The revised protocol types
```rust
/// Where a query task's matches are written.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "type")]
pub enum OutputHandle {
    /// The results cache, addressed by a MongoDB URI whose path names the database. The collection
    /// is the query job's ID.
    #[serde(rename = "results_cache")]
    ResultsCache { uri: NonEmptyString },

    /// A file per archive. Not yet supported by the Spider query flow.
    #[serde(rename = "file")]
    File,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClpSQueryOption {
    pub query_string: String,
    /// The maximum number of results retained by one archive-query invocation, or `None` for no
    /// limit.
    pub max_num_results: Option<NonZeroU32>,
    /// The inclusive lower timestamp bound (`--tge`), in Unix epoch milliseconds.
    pub begin_timestamp: Option<i64>,
    /// The inclusive upper timestamp bound (`--tle`), in Unix epoch milliseconds.
    pub end_timestamp: Option<i64>,
    pub ignore_case: bool,
}
```
`Deserialize` is required because the `#[task]` macro folds every non-`ctx` parameter into a generated serde `Deserialize` params struct. `File` stays a **unit** variant: Python derives its path from `worker_config.stream_output.get_directory() / job_id / archive_id` (`fs_search_task.py:211-217`), and `SpiderTaskExecutorConfig` has no `stream_output` field at all — so the path is not merely unimplemented, it is *unresolvable* from the current config.
### 5.2 The revised task signature
```rust
#[task(name = "query::clp_s_query_to_results_cache")]
pub(crate) fn clp_s_query_to_results_cache_task(
    ctx: TaskContext,
    query_job_id: i32,
    clp_s_query_option: ClpSQueryOption,
    output_handle: OutputHandle,
    dataset: Option<NonEmptyString>,
    archive_id: String,
) -> Result<(), TdlError>
```
### 5.3 `dataset: Option<…>` — how `None` is resolved
The Rust side owns the `None` → default mapping. The helper already exists:
```rust
// clp_rust_utils::dataset (dataset.rs:5-16)
pub const CLP_DEFAULT_DATASET_NAME: &str = "default";
pub fn resolve_dataset_name(dataset: Option<&str>) -> &str { dataset.unwrap_or(CLP_DEFAULT_DATASET_NAME) }
```
Resolve **once**, at the top of the worker, and pass the resolved `&str` everywhere after:
```rust
let dataset = resolve_dataset_name(dataset.as_ref().map(NonEmptyString::as_str));
```
Three correctness checks:
1. **The path is right.** `dataset_archive_storage_directory` (`config.rs:366-384`) already calls `resolve_dataset_name` internally, so `…(Some("default"))` and `…(None)` produce the identical path. Resolving early is exactly equivalent for path building, and makes the resolved name available for the flag.
2. **`default`**** is really where the archives are.** Compression maps `None` → `CLP_DEFAULT_DATASET_NAME` on both submit paths — `compress.py:197` (and `compress_from_s3.py:252`) on the Python side, `compression_job_submitter.rs:80` on the Rust side. So archives ingested without an explicit dataset land under `…/default/`. Resolving to `default` on read matches what was written.
3. **Always pass ****`--dataset <resolved>`****.** This is where we deliberately diverge from Python for the better. Python appends the flag only `if dataset is not None` (`fs_search_task.py:229-230`); omitting it makes clp-s default-construct the field and write `dataset: ""` into every document (`CommandLineArguments.hpp:39`). Passing the resolved name writes `dataset: "default"`, which is both truthful and what a caller who filters on `dataset` expects. Note also that Python's own `dataset: str | None` is only nominally optional here — `_make_core_clp_s_command_and_env_vars` would build `archives_dir / None` and raise if it were ever actually `None` for clp-s (`fs_search_task.py:111,131`).
**Why ****`Option<NonEmptyString>`**** and not ****`Option<String>`** (settled): an empty `String` is not `None`. It would resolve to `""`, producing the archive path `<archives_dir>/` (searching the dataset root instead of the dataset) and writing `dataset: ""` into every document. Both are silent wrong answers. `NonEmptyString` makes the case unrepresentable, is already a `clp-rust-utils` dependency, and is what the sibling `task_io/compression.rs` uses for non-empty wire fields.
### 5.4 `max_num_results: Option<NonZeroU32>` — how `None` is resolved
`None` means **no task-level limit**, and — for the same "Rust side owns the mapping" reason as §5.3 — the flag is still passed explicitly, with `u32::MAX`:
```rust
let max_num_results = clp_s_query_option.max_num_results.map_or(u32::MAX, NonZeroU32::get);
// … "--max-num-results", max_num_results.to_string()
```
The alternative — omitting the flag on `None` — is worse and would be a real bug: clp-s's own default is **1000** (`CommandLineArguments.hpp:41`), so "no limit" would silently become "1000", truncating results with no error. `u32::MAX` is unreachable for a single archive (clp-s's cap is a per-archive `std::priority_queue` bound, `OutputHandlerImpl.cpp:149-176`), so it is a faithful "unlimited" without widening the type. This also resolves the old open item about Python's `max_num_results == 0` sentinel (`query_scheduler.py:1032`), which `NonZeroU32` could not express: the coordinator now maps `0` → `None`.
### 5.5 Other items
**(6) The orphaned import is a hard CI failure**, not a warning: `query/mod.rs:4` imports `QueryTaskOutput` while the fn returns `Result<(), TdlError>`, and CI runs `cargo +nightly clippy --all-targets --all-features -- -D warnings` (`taskfiles/lint.yaml:824-826`). The `QueryTaskOutput` *type* stays in `clp-rust-utils` (it is `pub`, so no warning) — it keeps the door open for a future query-commit task.
**(7) The timestamp doc comments are wrong end-to-end.** `task_io/query.rs:17,20` say "microseconds"; the whole chain is milliseconds — `api-server/src/client.rs:223-224` assigns `time_range_begin_millisecs` straight into `begin_timestamp`, Python passes it through unconverted, and clp-s's help says `ms`. **Fix the comment; add no conversion.** An implementer who trusts the comment and divides by 1000 makes every time-bounded query silently return nothing.
**(8) Hoisting.** `clp_binary_path` (`compress.rs:593-595`) and `s3_credential_env` (`compress.rs:364-414`) are module-private inside a private module, and both are needed verbatim. They go in a new `src/task/clp_s.rs` rather than `common.rs`, because `lib.rs:3` declares `pub mod common;` (leaking them into the rlib's public API) while `lib.rs:4` declares `mod task;`.
---
## 6. Telemetry — OPTIONAL, deferred to a future PR
> **Not part of this implementation.** Nothing in §5 or §7 depends on this section, and the task will not pass `--enable-telemetry`. This is a standing proposal, researched and written down now so the future PR starts from a known state rather than re-deriving it. Two other pieces of that PR are handled separately: the worker telemetry env (§6.2, item 1) and, if the coordinator wants to carry a trace context, whatever it needs to pass down.
Python's search task samples telemetry per invocation (`fs_search_task.py:139-147`); we currently do not. Here is what clp-s offers, what is missing on the Rust side, and what the future PR would do.
### 6.1 What clp-s gives us
`--enable-telemetry` is a plain bool switch (`CommandLineArguments.cpp:749-753`). When set, clp-s builds an OTLP/HTTP trace exporter whose endpoint resolves as (`TelemetryContext.cpp:71-92`):
1. `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` if either is set (honoured natively by the exporter), else
2. `CLP_TELEMETRY_ENDPOINT` + `/v1/traces`, else
3. the opentelemetry default (`http://localhost:4318`).
Service name defaults to `clp-search` unless `OTEL_SERVICE_NAME` is set. The span picks up `CLP_QUERY_ID` and `CLP_TASK_ID` from the environment (`SearchTelemetry.cpp:247-253`) and records a **non-reversible hash** of the archive ID (`SearchTelemetry.cpp:48,230`) — so no archive identity leaves the process.
### 6.2 Three gaps to close in that PR
1. **The spider-worker container has no telemetry env at all.** `compose.clp-spider.yaml:6-9` sets only `CLP_CONFIG_PATH`, `CLP_DB_PASS`, `CLP_DB_USER`; it does not merge the `x-clp-telemetry-env-defaults` anchor (`docker-compose-all.yaml:41-44`) that every other CLP service uses. Helm is the same: spider is a subchart, and `values.yaml:395-398` (`spider.spiderConfig.worker.extra_envs`) carries those same three variables, never `clp.telemetryEnv` (`_helpers.tpl:136-146`). **Consequence: as things stand, ****`--enable-telemetry`**** would fall through to case (3) above and export to ****`localhost:4318`**** inside the worker container — a silent no-op.** This is being handled in a separate PR; §6.3's last paragraph records exactly what it needs. Until then, enabling the flag would be pointless anyway, which is the second reason this whole section is deferred.
2. **No sampling knob on the executor config.** Python samples per task from `worker_config.query_worker.query_trace_sampling_probability` (`fs_search_task.py:139-147`, default `0.01`, `clp_config.py:397-399`, `values.yaml:271`). `SpiderTaskExecutorConfig` has no `query_worker` field.
3. **No shared kill switch helper.** Python has `is_telemetry_disabled_by_env()` reading `CLP_DISABLE_TELEMETRY` / `DO_NOT_TRACK` (`telemetry_config.py:5-13`). Rust has the identical check, but **inlined** inside `init_telemetry` (`clp-rust-utils/src/telemetry.rs:61-69`, values at `:88`), so the task cannot reuse it without duplicating it.
### 6.3 Proposed shape
**Config (****`clp-rust-utils`****).** Both additions would be pure mirrors of fields that *already exist in the YAML* the worker reads — `SpiderTaskExecutorConfig` is explicitly "partially defined: unused fields are omitted and discarded through deserialization" (`config.rs:242-244`), so this is filling in, not inventing:
```rust
pub struct SpiderTaskExecutorConfig {
    pub package: Package,
    pub archive_output: ArchiveOutput,
    pub tmp_directory: String,
    pub database: Database,
    pub query_worker: QueryWorker,  // new
    pub telemetry: Telemetry,       // new; the type already exists at config.rs:463-478
}

/// Mirror of `clp_py_utils.clp_config.QueryWorker` (partially defined).
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct QueryWorker {
    pub query_trace_sampling_probability: f64,  // default 0.01
}
```
Note `QueryWorker` holds an `f64`, so `SpiderTaskExecutorConfig` loses its `Eq` derive and keeps `PartialEq`. That is the one ripple; it needs a check across existing `assert_eq!` uses in the config tests (they keep working — `assert_eq!` needs only `PartialEq`).
**Helper (****`clp-rust-utils`****).** Extract the inlined check into `pub fn is_telemetry_disabled_by_env() -> bool` and have `init_telemetry` call it. Mirrors the Python helper name, keeps one definition of the two variable names.
**Task.** Enable when all three agree:
```rust
let enable_telemetry = !config.telemetry.disable
    && !is_telemetry_disabled_by_env()
    && sample(ctx.task_instance_id, config.query_worker.query_trace_sampling_probability);
```
When enabled: append `--enable-telemetry` to the args (before the query positional, with the other option flags), and add `CLP_QUERY_ID = query_job_id` and `CLP_TASK_ID = ctx.task_instance_id` to the child env alongside the AWS credentials.
**Why the caller samples at all.** clp-s cannot. `--enable-telemetry` is a bool switch, and `TelemetryContext` builds its provider with `TracerProviderFactory::Create(processor, resource)` — **no sampler argument** (`TelemetryContext.cpp:114-121`) — so it gets the SDK default `AlwaysOn`. `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` are not wired either. Each invocation therefore traces always or never, and the decision has to be made upstream, exactly as Python does (`fs_search_task.py:139-147`). This placement is also cheaper: the flag gates process-level work — an OTLP HTTP exporter and batch span processor at startup plus a `Shutdown` with a 1s timeout on teardown (`clp-s.cpp:437-440`, `TelemetryContext.cpp:138-146`) — so un-sampled tasks pay none of it. Teaching clp-s a `TraceIdRatioBasedSampler` would move the decision inward but still construct and tear down the exporter on every invocation just to drop the span; a bad trade for a short-lived per-archive process, and a clp-s change besides.
**Sampling with ****`rand`****.** Promote `rand` to a workspace dependency (one line; it is already in `Cargo.lock` transitively, so no new downloads) and keep the draw to one readable line:
```rust
fn should_sample(probability: f64) -> bool { rand::random::<f64>() < probability }
```
An earlier revision of this plan proposed hashing `ctx.task_instance_id` with `DefaultHasher` instead, purely to preserve a "no new dependency" property. That was rejected on review: it is far more machinery than a coin flip warrants, and its claimed benefit — determinism, hence testability — does not hold, because `DefaultHasher`'s algorithm is explicitly unspecified across Rust releases. A test pinning a particular `task_instance_id` to `true` would be stable within a build but could break on a toolchain bump.
**Deployment.** Add `<<: *clp_telemetry_env_defaults` to the `spider-worker` service in `compose.clp-spider.yaml`, and append `{{- include "clp.telemetryEnv" . | nindent … }}` (or the two literal env entries) to `spider.spiderConfig.worker.extra_envs` in `values.yaml`. Optionally set `OTEL_SERVICE_NAME: "spider-worker"` to match the other services. This is being handled separately; it is recorded here only so the whole picture stays in one place.
---
## 7. Implementation steps
Ordered so each step compiles on top of the last.
1. **`clp-rust-utils/src/task_io/query.rs`** — add `OutputHandle`; change `max_num_results` to `Option<NonZeroU32>`; add `deny_unknown_fields` to `ClpSQueryOption`, `QueryTaskOutput`, and `OutputHandle`; fix the two doc comments; extend the msgpack round-trip tests to cover both `OutputHandle` variants and the `max_num_results: None` case, in the existing style at `:41-95`. Self-contained; compiles alone.
2. **Hoist** `clp_binary_path` + `s3_credential_env` into new `src/task/clp_s.rs`; add `pub(crate) mod clp_s;` to `task/mod.rs`; update `compress.rs` imports and move the `s3_credential_env` test. **Keep as its own commit** — it touches the compression path, so the query change stays reviewable in isolation.
3. **Hoist** `create_archive_s3_key` (`compress.rs:623-632`) into `ArchiveOutput::dataset_archive_object_key` (§3.2), next to `dataset_archive_storage_directory` (`config.rs:366-384`). Add a unit test pinning the `key_prefix`-ends-in-slash behaviour. Own commit.
4. **`search.rs`****: ****`build_clp_s_search_args`** — the pure builder, plus unit tests. Order mirrors `fs_search_task.py:106-137` + `:178-231`: subcommand, archive path, archive selector/auth, query positional, match flags, then the `results-cache` subcommand last. `--batch-size` is deliberately omitted (clp-s defaults it to 1000), exactly as Python does. Tests assert the full `Vec<OsString>` in order: one FS case with both timestamps and `ignore_case: true`, one S3 case with neither, one `dataset: None` case, one `max_num_results: None` case.
5. **`search.rs`****: ****`resolve_archive_input`** — match on `ArchiveOutputStorage`. FS arm: `abs_archive_output_staging(clp_home).join(dataset)` + `--archive-id`, **empty** credential vec. S3 arm: `dataset_archive_object_key` → `generate_s3_url` → positional URL + `--auth s3`, with `s3_credential_env(&runtime(), region, &s3_config.aws_authentication)`.
6. **`search.rs`****: ****`run_clp_s_search`** — modelled on `run_log_converter`: `stdout(Stdio::null())`, `stderr(Stdio::piped())`, drain stderr, check exit status, log stderr on failure. Blocking `std::process::Command` is correct — the task runs on a Spider blocking worker thread, same as compression. Needs a `# Panics` doc section for the `.expect` on piped stderr.
7. **`search.rs`****: ****`pub(super) fn search`** — resolve the dataset (§5.3); destructure `OutputHandle::ResultsCache { uri }` with a `let … else { anyhow::bail!(…) }` rejecting `File`; bail if `config.package.storage_engine` is not `ClpS` (it defaults to `Clp`, `config.rs:251-256`, and Python guards this explicitly at `fs_search_task.py:161-173`); log start/finish with the `job_id`/`task_id`/`task_instance_id` tracing fields used in `compress.rs`. Parameter count stays at 7 so `clippy::too_many_arguments` does not fire.
8. **`query/mod.rs`** — apply the §5.2 signature, add `mod search;`, delegate to `search::search(…)`, `.map_err(|e| TdlError::ExecutionError(format!("{e:#}")))` per `compression/mod.rs:27`, and delete the `QueryTaskOutput` import. `lib.rs` needs no change; the task is already registered at `lib.rs:34`.
9. **README + lint.** Add a `### Query` section. Then `task lint:fix-rust-format`, `task lint:check-rust-static`, `cargo test -p clp-rust-utils -p clp-tdl-package`. The workspace denies `clippy::all`, `nursery`, and `pedantic` (`.cargo/config.toml:1-7`).
Error handling throughout follows the house pattern: `anyhow::Result` with `.context(…)` in the worker, flattened to `TdlError::ExecutionError` at the `#[task]` boundary. **No new error variants are needed** in either `clp_rust_utils::Error` or `TdlError`.
---
## 8. Testing
### 8.1 What is committed, and what is not
<table header-row="true">
<tr>
<td></td>
<td>Committed to the PR</td>
<td>Scratch, local only</td>
</tr>
<tr>
<td>**What**</td>
<td>In-crate `#[cfg(test)]` unit tests (§8.2)</td>
<td>The container-backed harness and every integration case (§8.3-§8.5)</td>
</tr>
<tr>
<td>**Runs**</td>
<td>`cargo nextest` / `task tests:rust-all`, no Docker</td>
<td>By hand, before the PR</td>
</tr>
<tr>
<td>**Purpose**</td>
<td>Permanent regression cover</td>
<td>One-time correctness evidence; results summarized in the PR description</td>
</tr>
</table>
The integration harness exists to prove the task actually works before the end-to-end system is wired up. It is deliberately not committed: it would be the repo's first `clp-s s` binary test and its first MinIO usage, and standing that up properly (task-runner targets, container scripts, CI wiring) is its own piece of work, not a rider on this PR.
### 8.2 Committed unit tests
These follow the established `compress.rs` idiom: every subprocess invocation is built by a pure `build_*_args` function returning `Vec<OsString>`, and the test asserts the exact argv (`compress.rs:996-1107`). They use only the crate's existing dependencies, so `clp-tdl-package` still needs **no ****`[dev-dependencies]`**.
**U1 ****`build_clp_s_search_args`**** — the argv matrix** (`search.rs`)
<table header-row="true">
<tr>
<td>#</td>
<td>Case</td>
<td>Asserts</td>
</tr>
<tr>
<td>a</td>
<td>FS, both timestamps, `ignore_case: true`</td>
<td>full argv in order: `s`, `<dir>/<ds>`, `--archive-id`, `<id>`, `<query>`, `--tge`, `--tle`, `--ignore-case`, `results-cache`, `--uri`, `--collection`, `--max-num-results`, `--dataset`</td>
</tr>
<tr>
<td>b</td>
<td>FS, no timestamps, `ignore_case: false`</td>
<td>`--tge`/`--tle`/`--ignore-case` all absent</td>
</tr>
<tr>
<td>c</td>
<td>S3</td>
<td>positional URL + `--auth s3`; **no** `--archive-id`</td>
</tr>
<tr>
<td>d</td>
<td>`dataset: None`</td>
<td>path ends `/default`, flag is `--dataset default`</td>
</tr>
<tr>
<td>e</td>
<td>`dataset: Some("ds1")`</td>
<td>path ends `/ds1`, flag is `--dataset ds1`</td>
</tr>
<tr>
<td>f</td>
<td>`max_num_results: Some(7)`</td>
<td>`--max-num-results 7`</td>
</tr>
<tr>
<td>g</td>
<td>`max_num_results: None`</td>
<td>`--max-num-results 4294967295`</td>
</tr>
<tr>
<td>h</td>
<td>any</td>
<td>`--batch-size` never appears (clp-s defaults it to 1000, as Python does)</td>
</tr>
<tr>
<td>i</td>
<td>timestamps</td>
<td>values passed through **unconverted** — guards the µs/ms doc bug in §5.5(7)</td>
</tr>
</table>
**U2 ****`resolve_archive_input`** (`search.rs`)
<table header-row="true">
<tr>
<td>#</td>
<td>Case</td>
<td>Asserts</td>
</tr>
<tr>
<td>a</td>
<td>`Fs { directory }`</td>
<td>`abs_archive_output_staging(clp_home).join(dataset)`; credential vec is **empty**</td>
</tr>
<tr>
<td>b</td>
<td>`S3 { s3_config, staging_directory }`</td>
<td>object key is `{key_prefix}{dataset}/{archive_id}`; URL from `generate_s3_url`; `--auth s3`; credential env populated</td>
</tr>
<tr>
<td>c</td>
<td>**S3 ignores ****`staging_directory`**</td>
<td>give it a value that would be wrong and assert it appears nowhere in the argv — the `abs_archive_output_staging` trap (§2)</td>
</tr>
<tr>
<td>d</td>
<td>S3 with `endpoint_url: Some`, `region_code: None`</td>
<td>path-style URL `http://host:port/bucket/key`</td>
</tr>
</table>
**U3 rejection paths** (`search.rs`, calling `search()` directly — these fail before anything spawns)
<table header-row="true">
<tr>
<td>#</td>
<td>Case</td>
<td>Asserts</td>
</tr>
<tr>
<td>a</td>
<td>`OutputHandle::File`</td>
<td>`Err`, message names the unsupported handler; no process spawned</td>
</tr>
<tr>
<td>b</td>
<td>`package.storage_engine: Clp`</td>
<td>`Err` — it defaults to `Clp` (`config.rs:251-256`), so this guard matters</td>
</tr>
</table>
**U4 ****`ArchiveOutput::dataset_archive_object_key`** (`clp-rust-utils`)
<table header-row="true">
<tr>
<td>#</td>
<td>Case</td>
<td>Asserts</td>
</tr>
<tr>
<td>a</td>
<td>S3, `dataset: None`</td>
<td>`{key_prefix}default/{id}`</td>
</tr>
<tr>
<td>b</td>
<td>S3, `dataset: Some`</td>
<td>`{key_prefix}{ds}/{id}`</td>
</tr>
<tr>
<td>c</td>
<td>`key_prefix` ending in `/`</td>
<td>exactly one separator — parity with Python's `f"{key_prefix}{dataset}/{archive_id}"` (`clp_config.py:598-605`)</td>
</tr>
</table>
Wire-format round-trip tests for `OutputHandle` and `ClpSQueryOption` are **not** listed separately: they are already covered by §7 step 1, which extends the existing msgpack tests in `task_io/query.rs:41-95`.
### 8.3 The scratch harness
**Driving the task.** `clp-tdl-package` is built as `cdylib` **and ****`rlib`**, and `register_tdl_package!` expands to `pub extern "C"` items at crate root (`spider-tdl/src/register.rs:112-175`). A local test can link the rlib and call the same three symbols the real task executor resolves through `dlopen` (`spider-task-executor/src/manager.rs:253-270`):
<table header-row="true">
<tr>
<td>Step</td>
<td>Real executor</td>
<td>Harness</td>
</tr>
<tr>
<td>Load package</td>
<td>`dlopen`  • `dlsym`</td>
<td>link the rlib</td>
</tr>
<tr>
<td>Init</td>
<td>`__spider_tdl_package_init()`</td>
<td>same, after setting `CLP_CONFIG_PATH` / `CLP_HOME`</td>
</tr>
<tr>
<td>Encode context</td>
<td>`rmp_serde::to_vec(&TaskContext)` (`process_pool.rs:422`)</td>
<td>same</td>
</tr>
<tr>
<td>Encode params</td>
<td>`TaskInputsSerializer`  • one `rmp_serde::to_vec` per param (`compression_job_submitter/spider.rs:87-89`)</td>
<td>same</td>
</tr>
<tr>
<td>Dispatch</td>
<td>`__spider_tdl_package_execute(name, ctx, inputs)`</td>
<td>same</td>
</tr>
</table>
This is worth the FFI plumbing because it exercises, for free, four things a direct call to `search()` would not: the env-var config load, the `#[task]` macro's generated params struct, the **compact** msgpack encoding (the encoding that actually breaks internally-tagged enums, §4), and the `TdlError` msgpack error path. It is also the only external door — `lib.rs:4` declares `mod task;` privately, so `search()` is unreachable from `tests/`.
**Services**, both in Docker containers, started outside the test process:
<table header-row="true">
<tr>
<td>Service</td>
<td>Image</td>
<td>Provisioning</td>
</tr>
<tr>
<td>MongoDB</td>
<td>`mongo:8.0.21` (same pin as `docker-compose-all.yaml:193`)</td>
<td>none; collections are created implicitly by `insert_many`</td>
</tr>
<tr>
<td>MinIO</td>
<td>latest; no pin needed for throwaway use</td>
<td>create one lowercase bucket</td>
</tr>
</table>
**Setup per run:**
1. Build a shim `CLP_HOME`: a temp dir with `bin/clp-s` symlinked to the built binary, plus `var/tmp/`.
2. Write a `clp-config.yaml` with `package.storage_engine: clp-s` and the branch-appropriate `archive_output.storage`; point `CLP_CONFIG_PATH` at it.
3. Build the fixture archive with `clp-s c … --print-archive-stats`, parsing the archive ID off stdout exactly as `compress.rs:416-432` does.
4. For S3: upload `<archives_dir>/<archive_id>` to `s3://<bucket>/<key_prefix><dataset>/<archive_id>`.
5. `__spider_tdl_package_init()` once per binary; a fresh `query_job_id` per case so each gets its own MongoDB collection.
**Five constraints that shape the harness**, each verified against source:
1. **`clp-s`**** lives at ****`build/core/clp-s`****, not ****`$CLP_HOME/bin/clp-s`****.** CMake puts it at the build-dir root (`components/core/src/clp_s/CMakeLists.txt:521-547`, `taskfile.yaml:36,219-234`); the `$CLP_HOME/bin/` layout exists only inside the Docker images (`tools/docker-images/clp-package/Dockerfile:37`). Hence the symlink shim.
2. **The executor config is a ****`OnceLock`** (`common.rs:32-47`) — one config per process. FS-backed and S3-backed runs therefore **cannot share a test binary**; they need two.
3. **A network archive is unconditionally read as single-file.** `ArchiveReaderAdaptor.cpp:28-40` sets `m_single_file_archive = true` whenever the source is not `Filesystem`, so the S3 fixture must be compressed with `--single-file-archive` while the FS fixture should be a multi-file directory — which is what each deployment actually produces (`compression_task.py:415`). The two branches genuinely need different fixtures.
4. **Path-style S3 URLs work, so MinIO is viable.** clp-s uses libcurl with a SigV4 *presigned URL* and no AWS SDK (`InputConfig.cpp:38-41,241-262`); `S3Url` accepts path-style with an optional `s3.` prefix and a port (`AwsAuthenticationSigner.cpp:173-216`, `aws/constants.hpp:11-14`), and a unit test already pins `http://localhost:4566/test-bucket/logs/system.log` (`test_AwsAuthenticationSigner.cpp:26-32`).
5. **The S3 config must set ****`region_code: None`****, and bucket/host must be lowercase.** With a custom endpoint *and* a region, `generate_s3_url` prepends the region as a hostname label — `http://us-east-1.minio:9000/…` (`s3/url.rs:54-78`, its own test at `:148-161`) — which will not resolve. clp-s derives the region from the URL alone, defaulting to `us-east-1` (`AwsAuthenticationSigner.cpp:207-215`), which is what MinIO expects. Both the bucket and endpoint regexes are `[a-z0-9.-]+`.
### 8.4 Integration case matrix
**Tier F — filesystem-backed** (MongoDB only; multi-file directory archive)
<table header-row="true">
<tr>
<td>#</td>
<td>Case</td>
<td>Asserts</td>
</tr>
<tr>
<td>**F1**</td>
<td>Happy path, `dataset: None`</td>
<td>task succeeds; the `<query_job_id>` collection holds the expected documents; every doc has `archive_id == <id>`, `dataset == "default"`, and the expected `message`</td>
</tr>
<tr>
<td>**F2**</td>
<td>Explicit `dataset: Some("json_multifile")`</td>
<td>results present; docs carry `dataset: "json_multifile"` — proves both the path join and the label</td>
</tr>
<tr>
<td>**F3**</td>
<td>`--tge` only, set between day 1 and day 2 of the fixture</td>
<td>strictly fewer documents than F1, and every returned `timestamp >= tge`. **The highest-value case here** — a µs/ms conversion bug returns zero rows</td>
</tr>
<tr>
<td>**F4**</td>
<td>`--tle` only, and `--tge`+`--tle` bracketing one day</td>
<td>exact expected event count</td>
</tr>
<tr>
<td>**F5**</td>
<td>`ignore_case: true` vs `false` on a case-mismatched query</td>
<td>case-insensitive returns hits, case-sensitive returns none</td>
</tr>
<tr>
<td>**F6**</td>
<td>`max_num_results: Some(1)`</td>
<td>exactly one document, and it is the newest by timestamp (clp-s keeps top-N by timestamp, `OutputHandlerImpl.hpp:159-166`)</td>
</tr>
<tr>
<td>**F7**</td>
<td>`max_num_results: None` against the \>1000-event fixture (§8.5)</td>
<td>**all** matching events returned, not 1000 — the end-to-end proof that "no limit" is not silently capped by clp-s's default</td>
</tr>
<tr>
<td>**F8**</td>
<td>Query matching nothing</td>
<td>task **succeeds**; collection is empty. Distinguishes "no results" from "failed"</td>
</tr>
<tr>
<td>**F9**</td>
<td>Two tasks, same `query_job_id`, different archives</td>
<td>both result sets present, neither clobbers the other — mirrors a real multi-archive job</td>
</tr>
</table>
**Tier S — S3-backed** (MongoDB + MinIO; single-file archive)
<table header-row="true">
<tr>
<td>#</td>
<td>Case</td>
<td>Asserts</td>
</tr>
<tr>
<td>**S1**</td>
<td>Happy path</td>
<td>results present; `archive_id` in every document equals the archive ID — which on the S3 path clp-s derives from **the last component of the object key** (`InputConfig.cpp:141-151`), so this also pins our key builder</td>
</tr>
<tr>
<td>**S2**</td>
<td>`staging_directory` points at an **empty** temp dir</td>
<td>results still present. The trap test: an implementation using `abs_archive_output_staging` unconditionally would search an empty local dir and return **zero rows with exit code 0** — a silent wrong answer no other case catches</td>
</tr>
<tr>
<td>**S3**</td>
<td>`dataset: Some("ds1")`, object at `{key_prefix}ds1/{id}`</td>
<td>results present; docs carry `dataset: "ds1"`</td>
</tr>
<tr>
<td>**S4**</td>
<td>Correct vs. deliberately wrong credentials</td>
<td>success vs. failure — proves `s3_credential_env` is actually applied to the child env, not merely built</td>
</tr>
<tr>
<td>**S5**</td>
<td>Path-style URL shape</td>
<td>argv URL matches `http://<host>:<port>/<bucket>/<key>` — guards against a future `region_code` regression</td>
</tr>
</table>
**Tier E — failure paths**
<table header-row="true">
<tr>
<td>#</td>
<td>Case</td>
<td>Asserts</td>
</tr>
<tr>
<td>**E1**</td>
<td>Nonexistent `archive_id`</td>
<td>`TdlError::ExecutionError`; **the message contains clp-s's stderr** — the diagnostic contract, since the task has no other channel</td>
</tr>
<tr>
<td>**E2**</td>
<td>Malformed KQL</td>
<td>task errors; stderr surfaced</td>
</tr>
<tr>
<td>**E3**</td>
<td>Unreachable MongoDB URI (closed port)</td>
<td>task errors rather than reporting success</td>
</tr>
<tr>
<td>**E4**</td>
<td>`OutputHandle::File` over the real FFI path</td>
<td>`TdlError::ExecutionError`; nothing written to MongoDB</td>
</tr>
</table>
**Tier W — wire contract, over the real FFI entry point**
<table header-row="true">
<tr>
<td>#</td>
<td>Case</td>
<td>Asserts</td>
</tr>
<tr>
<td>**W1**</td>
<td>Params encoded exactly as the coordinator will</td>
<td>implicit in every F/S case, and the reason to drive via FFI at all</td>
</tr>
<tr>
<td>**W2**</td>
<td>Extra unknown field in the encoded `OutputHandle`</td>
<td>`TdlError::DeserializationError` — `deny_unknown_fields` over the real wire</td>
</tr>
<tr>
<td>**W3**</td>
<td>Unknown task name</td>
<td>`TdlError::TaskNotFound` — cheap proof the task is registered under the expected name</td>
</tr>
<tr>
<td>**W4**</td>
<td>Missing `CLP_CONFIG_PATH` at init</td>
<td>`__spider_tdl_package_init()` returns an error rather than panicking</td>
</tr>
</table>
### 8.5 Fixtures
- **`integration-tests/tests/data/json_multifile/`** — 5 files, **40 events**, real millisecond timestamps spanning `1310138944000` → `1311208074120` across 5 distinct days, `timestamp_key` of `timestamp`, and a documented `single_match_wildcard_query` (`metadata.json`). Small, deterministic, and the day boundaries make F3/F4 easy to write with exact expected counts.
- **A generated \>1000-event JSONL**, written to a temp dir by the harness, for **F7**. At 40 events the committed fixture cannot distinguish `max_num_results: None` from clp-s's built-in default of 1000 — both return everything. Generating \~1500 synthetic events is cheap and makes the "unlimited" mapping (§5.4) verifiable end-to-end rather than only in `U1(g)`.
- `components/core/src/clp_s/tests/test_log_files/test_search.jsonl` — tiny; useful only as a second archive for F9.
### 8.6 One CI note
`components/clp-tdl-package/**` is **absent** from `monitored_paths` in `.github/workflows/clp-rust-checks.yaml:5-16`. A PR touching only this crate therefore runs **no Rust CI at all** today — the unit tests in §8.2 included. Adding the path is a one-line change and worth doing in this PR.
### 8.7 Not covered
Aggregation and the `reducer`/`network` handlers (§9); telemetry (§6); cancellation/SIGTERM, which needs a real Spider executor; the coordinator's collection and index creation (§1.2c); and real AWS S3 — MinIO is the S3 surface under test.
---
## 9. Deliberate divergences from the Python task
All are per-task behaviour we drop on purpose; each should be named in the PR description.
- **Aggregation**, and the `reducer` / `network` / `file` output handlers (`fs_search_task.py:188-220`). The coordinator must reject jobs carrying an `aggregation_config`, a `network_address`, or `write_to_file: true` *before* dispatch. Note this is reachable: `SearchJobConfig::write_to_file` is set from `!value.buffer_results_in_mongodb` (`api-server/src/client.rs:226`).
- **MySQL ****`query_tasks`**** status/duration reporting** (`update_query_task_metadata`), the per-task stderr log at `<CLP_LOGS_DIR>/<job_id>/<task_id>-clo.log`, and the SIGTERM/`killpg` cancellation handler. If Spider does not provide equivalent cancellation, a long-running clp-s child may outlive a cancelled job — worth confirming during testing.
- **Job-level concerns that belong to the coordinator, not this task**: creating the per-job collection and its `timestamp`-descending index (`query_scheduler.py:362-385` — see §1.2c; this is where the coordinator's `mongodb` client is needed), `found_max_num_latest_results` early termination, and job-status aggregation.
- **`--dataset`**** is always passed**, where Python omits it when the dataset is `None` (§5.3.3). This is a divergence in our favour.
- **Telemetry** — `--enable-telemetry` with `CLP_QUERY_ID`/`CLP_TASK_ID` (`fs_search_task.py:139-147`). Deferred to a future PR; §6 is the standing proposal for it.
---
## 10. Settled decisions
<table header-row="true">
<tr>
<td>Item</td>
<td>Decision</td>
</tr>
<tr>
<td>`output_handle` as a task parameter</td>
<td>Approved.</td>
</tr>
<tr>
<td>`OutputHandle::ResultsCache` shape</td>
<td>Struct variant, `{ uri: NonEmptyString }`.</td>
</tr>
<tr>
<td>Deployment mount gap for FS archives</td>
<td>Out of scope; another PR. Test S3 in stage 3.</td>
</tr>
<tr>
<td>`dataset`</td>
<td>`Option<NonEmptyString>`; the Rust side resolves `None` → `default` and always passes `--dataset`.</td>
</tr>
<tr>
<td>`deny_unknown_fields`</td>
<td>On every `task_io::query` type. Verified compatible with `tag = "type"` (§4).</td>
</tr>
<tr>
<td>Results-cache URI form</td>
<td>Includes `directConnection=true`; produced by the coordinator, per y-scope/clp#2505.</td>
</tr>
<tr>
<td>`max_num_results`</td>
<td>`Option<NonZeroU32>`; `None` → explicit `--max-num-results u32::MAX`.</td>
</tr>
<tr>
<td>Telemetry</td>
<td>**Out of scope.** Deferred to a future PR; §6 holds the proposal.</td>
</tr>
<tr>
<td>`create_archive_s3_key`</td>
<td>Moved to the shared `ArchiveOutput::dataset_archive_object_key`.</td>
</tr>
<tr>
<td>`mongodb` client</td>
<td>Not in the task; required in the coordinator for collection + index creation.</td>
</tr>
<tr>
<td>Test scope</td>
<td>Only the §8.2 unit tests are committed; the container harness is local verification, summarized in the PR description.</td>
</tr>
<tr>
<td>MinIO image</td>
<td>Unpinned — throwaway harness, never runs in CI.</td>
</tr>
<tr>
<td>`max_num_results: None` coverage</td>
<td>Harness generates a \>1000-event fixture so F7 proves it end-to-end.</td>
</tr>
</table>
Nothing is outstanding; implementation can start.
One note on the URI: the coordinator builds it, so the task needs no change for #2505 — but the `directConnection=true` form must be what the coordinator passes, matching `api-server/src/client.rs:263-266`. The config mounted into the worker is container-transformed, so `results_cache.host` is `results_cache`, not `localhost`.
---
## 11. Research provenance
Five parallel researchers (Python task, Rust compression task, `task_io` types, executor config/storage, results-cache + clp-s CLI), each adversarially audited by a second agent; one audit (`task-io-types`) was lost to a connection error. The load-bearing facts were then independently re-verified against source: the search CLI surface and handler options (`CommandLineArguments.cpp:714-780, 845-880, 1315-1324`), the AWS env var names (`InputConfig.hpp:14-24`), Python's argument ordering (`fs_search_task.py:95-231`), `SpiderTaskExecutorConfig`'s field set and `abs_archive_output_staging`'s S3 behaviour (`config.rs:55-110`), `dataset_archive_storage_directory` (`config.rs:366-384`), and the compression task's spawn/credential idioms (`compress.rs:364-414, 593-632, 742-881`).
Later revisions add first-hand verification of: the results-cache document shape and archive-ID derivation (`OutputHandlerImpl.cpp:89-176`, `search/Output.cpp:99`, `ArchiveReader.cpp:30`, `InputConfig.cpp:141-151`); the S3 key layout equivalence between Python and Rust (`fs_search_task.py:110-111`, `clp_config.py:598-605`, `compress.rs:623-632`); the dataset default mapping (`dataset.rs:5-16`, `compress.py:197`, `compression_job_submitter.rs:80`); the telemetry surface and its deployment gaps (`TelemetryContext.cpp:71-92`, `SearchTelemetry.cpp:48,230,247-253`, `telemetry.rs:61-88`, `compose.clp-spider.yaml:6-9`, `values.yaml:395-398`, `_helpers.tpl:136-146`); the task-input wire encoding (`spider.rs:87-89`, `io.rs:297-301`), confirmed by compiling and running a throwaway `deny_unknown_fields` + `tag = "type"` round-trip test in `clp-rust-utils`, since removed; the consumers of the result documents' `dataset` field (`LogViewerLink.tsx:44-46` is the only one that branches on it); and the test infrastructure the harness builds on (`taskfiles/tests/main.yaml:8-37`, `tools/scripts/localstack/`, `log-ingestor/tests/`, `CMakeLists.txt:521-547`, `ArchiveWriter.cpp:99-162`, `ArchiveReaderAdaptor.cpp:28-40`, `AwsAuthenticationSigner.cpp:173-216`).


