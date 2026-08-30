# Integration plan: `yscope-clp-core-beta` → `main` (C++ core only)

Scope: the C++ core changes only. The Python (`python-wheels/yscope-clp-core`) work on the
branch is explicitly out of scope and excluded from this plan.

## Branch state

- `yscope-clp-core-beta..main` is **empty**: `main` is a strict ancestor of the branch. A
  merge into `main` is conflict-free / fast-forward-capable — no divergence to reconcile.
- The branch history is messy: ~60 commits including many `Merge branch 'main' into …`
  commits, WIP/fixup commits (`lint fix`, `Revert temp lint changed`), and two **temporary
  hacks** that must not land on `main`:
  - `taskfiles/lint.yaml` — comments out Python-project linting (unrelated to the C++ work).
  - `taskfiles/tests/integration.yaml` — comments out `deps: ["::core"]`.
- Recommendation: **squash-merge** (or a curated rebase) rather than a plain
  merge/fast-forward, so `main` gets one clean commit and the temporary hacks are dropped or
  reverted.

## What the C++ diff actually does

The work is one coherent feature set — "make `ClpArchiveReader` decode/search-capable and
make telemetry optional" — split into four groups:

### 1. Optional telemetry (build-system refactor)
- `options.cmake`: new option `CLP_BUILD_CLP_S_SEARCH_TELEMETRY` (default **ON**, so default
  builds are unchanged).
- `set_clp_s_search_dependencies`: OpenTelemetry + xxHash are now required **only** when
  telemetry is on (previously unconditional).
- `search/CMakeLists.txt`: links `opentelemetry-cpp::*` + `xxHash` only under
  `CLP_BUILD_CLP_S_SEARCH_TELEMETRY`, and defines the macro on the target.
- `SearchTelemetry.cpp` / `TelemetryContext.cpp`: all OTel/xxHash code is
  `#ifdef CLP_BUILD_CLP_S_SEARCH_TELEMETRY`-guarded, with **no-op `Impl` stubs** when
  disabled (so the public API stays linkable without OTel).

### 2. SFA reader gains decode + search
- `ffi/sfa/ClpArchiveReader.{hpp,cpp}`: new methods `select_file`, `decode`, `decode_all`,
  `decode_range`, `search`, plus `find_file_info`, `get_active_event_count`,
  `get_uncompressed_size`, `get_selected_file_info`. Adds a `DecodeState` machine, a
  decoded-event cache, and `SchemaReader` tables. Constructor now also takes `archive_path`.
- `ffi/sfa/LogEvent.hpp` (new): `LogEvent{log_event_idx, timestamp, message}` (note: index
  type changed to `int64_t`).
- `ffi/sfa/SfaErrorCode.{hpp,cpp}`: new enums — `MalformedRangeIndex`, `DecodeRangeOutOfBounds`,
  `InvalidQuery`, `SearchFailure`, `LogEventIndexUnavailable`, `FileNotFound`,
  `FileSelectionAfterDecode`.
- `ffi/CMakeLists.txt`: `clp_s_ffi_sfa` now links `clp_s::search` + `clp_s::search::kql`
  (this is what enables `search()`).
- `validate_clp_s_ffi_sfa_dependencies` now also requires `CLP_BUILD_CLP_S_SEARCH` +
  `CLP_BUILD_CLP_S_SEARCH_KQL`.

### 3. InputConfig libarchive gating
- `InputConfig.cpp`: libarchive includes now guarded behind the existing
  `CLP_BUILD_CLP_S_ENABLE_LIBARCHIVE` option (the option itself already exists on `main`).

### 4. Test updates
- `test-clp_s-ffi_sfa_reader.cpp`: adds `assert_decoded_log_event_idx_matches_index`,
  exercising `decode_all()` and verifying global index ordering.

## Integration plan (step-by-step)

1. **Prepare a clean integration branch off `main`:**
   ```bash
   git fetch origin
   git checkout -b integrate/clp-s-ffi-decode-search origin/main
   ```
2. **Squash the branch's C++ work onto it**, excluding the two taskfile hacks. The cleanest
   route is a squash merge followed by reverting the hacks:
   ```bash
   git merge --squash yscope-clp-core-beta
   # revert the temporary hacks from the staged tree:
   git checkout origin/main -- taskfiles/lint.yaml taskfiles/tests/integration.yaml
   ```
   Then craft a single descriptive commit message summarizing the four groups above.
3. **Sanity-check the staged diff** is exactly the C++ feature set:
   ```bash
   git diff --cached origin/main --stat   # should be the 9 C++/cmake/test files only
   ```
4. **Build verification** (default config — telemetry ON, behavior unchanged):
   ```bash
   cmake -S components/core -B build/core -DCLP_BUILD_CLP_S_SEARCH=ON \
         -DCLP_BUILD_CLP_S_FFI_SFA=ON ...
   cmake --build build/core
   ```
5. **Build verification (telemetry OFF)** — the new gating path:
   ```bash
   cmake -S components/core -B build/core-notel -DCLP_BUILD_CLP_S_SEARCH_TELEMETRY=OFF ...
   cmake --build build/core-notel   # confirms no-op stubs link without OTel
   ```
6. **Run the SFA reader tests** (the updated `test-clp_s-ffi_sfa_reader.cpp`):
   ```bash
   ctest --test-dir build/core -R clp_s-ffi-sfa-reader --output-on-failure
   ```
7. **Lint/format the C++** (clang-tidy + clang-format via the repo's `task lint` target) — the
   branch had a "lint fix" + "Revert temp lint changed" cycle, so verify no formatting
   regressions slipped in.
8. **Open PR `integrate/clp-s-ffi-decode-search` → `main`**, noting in the description:
   - new public C++ API on `ClpArchiveReader` (decode/search/select_file),
   - the new `CLP_BUILD_CLP_S_SEARCH_TELEMETRY` option (default ON, no behavior change),
   - the libarchive include gating,
   - that `LogEvent` index type is now `int64_t`.
9. **Post-merge cleanup**: delete `yscope-clp-core-beta` once CI is green, and track the
   excluded Python work + the two taskfile hacks as separate follow-ups.

## Risks / things to watch

- **ABI/API change for SFA consumers**: `ClpArchiveReader`'s constructor signature changed
  (added `archive_path` param), and `LogEvent` index type went `uint64_t → int64_t`. Any
  in-tree caller of the private constructor or direct user of `LogEvent` needs to move with
  the branch — the test file already does.
- **`clp_s_ffi_sfa` now hard-depends on `clp_s::search` + `clp_s::search::kql`**. Any
  downstream that builds `CLP_BUILD_CLP_S_FFI_SFA=ON` but `CLP_BUILD_CLP_S_SEARCH=OFF` will
  now fail at CMake validation (`validate_clp_s_ffi_sfa_dependencies`) — verify this matches
  intended constraints; document it in the option's docstring/PR.
- **Telemetry-off path is new and untested by default CI** — step 5 is the one non-default
  config worth running manually before merge; if CI only builds the default, the no-op stubs
  may have a latent link error nobody catches.
- **The two taskfile hacks must not merge** — re-enabling Python linting/deps is out of scope
  here, but if they accidentally land, the repo's lint and integration-test targets silently
  skip the Python projects.