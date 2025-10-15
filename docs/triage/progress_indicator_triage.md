# Issue Triage: Progress indicators for long-running queries

## 1. Issue Analysis
- **Summary:** Users running long DataFusion queries through the Python bindings do not receive any execution feedback, which hurts UX in terminals and Jupyter notebooks. The community is already prototyping a CLI progress bar, and the feature request asks how similar feedback could be surfaced in Python, ideally via a user-supplied callable for custom visualizations.
- **Actual vs. expected:** `DataFrame.collect()` simply delegates to the Rust binding without emitting progress information, so users see nothing until the entire query finishes. Expected behavior is a progress reporting hook (e.g., built-in progress bar or callback) that surfaces intermediate execution status while the query is running.

## 2. Codebase Scope
- **Key modules:**
  - `python/datafusion/dataframe.py` exposes `DataFrame.collect()` and `collect_partitioned()`, both of which synchronously call into the Rust layer with no instrumentation.【F:python/datafusion/dataframe.py†L682-L712】
  - `src/dataframe.rs` implements the PyO3 bindings for collection; it calls into DataFusion and converts the resulting `RecordBatch`es to PyArrow objects, again without emitting progress events.【F:src/dataframe.rs†L518-L545】
  - `src/utils.rs` provides `wait_for_future`, which periodically checks for Python interrupts while awaiting Rust futures. This is where a cooperative progress hook could be injected while the GIL is released.【F:src/utils.rs†L68-L93】
- **Dependencies:** Collection ultimately invokes `datafusion::dataframe::DataFrame::collect`, so any progress solution must either leverage core DataFusion task metrics or poll execution state via the runtime environment exposed through the bindings.【F:src/dataframe.rs†L521-L526】【F:src/utils.rs†L68-L93】
- **Recent activity:** No recent commits in this repository add progress reporting; current functionality dates back to the existing synchronous collection path noted above.

## 3. Classification
- **Type:** Feature
- **Severity:** Minor (lack of progress updates degrades UX but does not break functionality)
- **Scope:** Single (limited to query execution UX in Python bindings)
- **Priority:** Medium (useful quality-of-life improvement; aligns with ongoing CLI work)

## 4. Resolution Plan
1. Investigate DataFusion core support for progress listeners (e.g., session task manager metrics used by the CLI PR) and document the Rust APIs available from version 50.
2. Prototype a Rust-side progress bridge that accepts an optional Python callback (probably stored as a `PyObject` guarded by the GIL) and invokes it from `wait_for_future` or a spawned task while `collect` executes.
3. Extend the Python API (possibly via `SessionConfig` or an argument to `collect`/`show`) to register the callback or select a default progress indicator implementation for terminals vs. notebooks.
4. Add user documentation and examples demonstrating both the default progress output and how to plug in a custom callable.
5. Write integration tests (likely asynchronous or using mocked callbacks) to ensure callbacks fire and do not block execution, plus manual verification in a notebook.

## 5. Estimation of Fix Size
Estimated effort: Medium — 2–3 days; Files: 4–6; Tests: 2–3; Risk: Medium (requires careful cross-language callback handling). Deployment: Requires release.

## 6. Next Steps
- **Recommendation:** Defer to future release after API design consensus (needs design alignment with ongoing CLI progress work and Python callback ergonomics).

## 7. Fix Location
- **Fix in this repo:** The Python bindings must expose and manage the progress hook locally, though it may depend on upstream DataFusion metrics infrastructure.
