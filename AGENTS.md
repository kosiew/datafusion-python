# AGENTS Instructions

This repository contains Python bindings for Rust's DataFusion.

## Project structure
- Root split: Rust implementation in `src/` and Python wrappers in `python/datafusion/`.
- Examples live in `examples/`; use `examples/datafusion-ffi-example/` as a reference for FFI idioms and UDF/UDAF examples.

## Build / dev / test workflow (essential)
- Ensure git submodules are initialized: `git submodule update --init`.
- Build the Rust→Python extension with maturin (prefer `uv` tooling):
  - Local dev build: `uv run --no-project maturin develop --uv` (or `maturin develop --uv` inside a venv)
- Run tests after building:
  - `uv --no-project pytest .` or `python -m pytest`

## Project-specific conventions & patterns
- Use the `maturin` + `pyo3` workflow for building wheels/develop installs; repository `pyproject.toml` contains maturin configuration.
- Many Python-only helpers and higher-level APIs live in `python/datafusion/` (for example `io.py`, `user_defined.py`, `dataframe_formatter.py`); prefer these helper modules when changing Python surface area.
- For Rust ↔ Python interop, prefer Arrow C Data Interface / PyCapsule patterns (see `src/pyarrow_util.rs`, `python/datafusion/context.py`, and `docs/source/contributor-guide/ffi.rst`).
- Place typing-only imports under `if TYPE_CHECKING:` guards (Ruff rule `TCH001` is enforced).
- In Rust examples/interop glue, prefer raw C string literals like `cr"..."` for small constants over allocating a `CString`.

## Linting and formatting
- Use pre-commit for linting/formatting.
- Run hooks for changed files before committing:
  - `pre-commit run --files <files>`
  - or `pre-commit run --all-files`
- Hooks enforce:
  - Python linting/formatting via Ruff
  - Rust formatting via `cargo fmt`
  - Rust linting via `cargo clippy`
- Ruff rules that frequently fail in this repo:
  - **Import sorting (`I001`)**: Keep import blocks sorted/grouped. Running `ruff check --select I --fix <files>` will repair order.
  - **Docstring spacing (`D202`, `D205`)**: The summary line must be separated from the body with exactly one blank line, and there must be no blank line immediately after the closing triple quotes.
  - **Ternary suggestions (`SIM108`)**: Prefer single-line ternary expressions when Ruff requests them over multi-line `if`/`else` assignments.

## Notes
- The repository mixes Python and Rust; ensure changes build for both languages.
- If adding new dependencies, update `pyproject.toml` and run `uv sync --dev --no-install-package datafusion`.

## Rust insights

Use these as quick mental models when reviewing or editing Rust code in this repo:

- Ownership/borrowing: model values and references as compile-time capability flow; keep borrows as short as practical.
- Traits: prefer composable capability contracts over inheritance-style thinking.
- `Result`/`Option`: treat them as explicit control-flow data; use combinators and `?` for clear pipelines.
- Lifetimes: express valid coexistence windows for references, not just syntax to satisfy the compiler.
- Pattern matching: model state with enums and exhaustive `match` handling.


## Refactoring opportunities
  - Avoid using private or low-level APIs when a stable, public helper exists. For example,
    automated refactors should spot and replace uses: 

    ```python
    # Before (uses private/low-level API) 
    # PyArrow example
    reader = pa.RecordBatchReader._import_from_c_capsule(
            df.__arrow_c_stream__()
    )

    # After (use public API)
    reader = pa.RecordBatchReader.from_stream(df)
    ```

    Look for call chains that invoke `_import_from_c_capsule` with `__arrow_c_stream__()`
    and prefer `from_stream(df)` instead. This improves readability and avoids
    relying on private PyArrow internals that may change.

## Helper Functions

## Commenting guidance

Use comments intentionally. Prefer three kinds of comments depending on purpose:

- Implementation Comments
  - Explains non-obvious choices and tricky implementations
  - Serves as breadcrumbs for future developers

- Documentation Comments
  - Describes functions, classes, and modules
  - Acts as public interface documentation

- Contextual Comments
  - Documents assumptions, preconditions, and non-obvious requirements

Keep comments concise and up-to-date; prefer clear code over comments when
possible, and move long-form design notes into the repository docs or an
appropriate design file.

- `python/datafusion/io.py` offers global context readers:
  - `read_parquet`
  - `read_json`
  - `read_csv`
  - `read_avro`
- `python/datafusion/user_defined.py` exports convenience creators for user-defined functions:
  - `udf` (scalar)
  - `udaf` (aggregate)
  - `udwf` (window)
  - `udtf` (table)
- `python/datafusion/col.py` exposes the `Col` helper with `col` and `column` instances for building column expressions using attribute access.
- `python/datafusion/catalog.py` provides Python-based catalog and schema providers.
- `python/datafusion/object_store.py` exposes object store connectors: `AmazonS3`, `GoogleCloud`, `MicrosoftAzure`, `LocalFileSystem`, and `Http`.
- `python/datafusion/unparser.py` converts logical plans back to SQL via the `Dialect` and `Unparser` classes.
- `python/datafusion/dataframe_formatter.py` offers configurable HTML and string formatting for DataFrames (replaces the deprecated `html_formatter.py`).
- `python/tests/generic.py` includes utilities for test data generation:
  - `data`
  - `data_with_nans`
  - `data_datetime`
  - `data_date32`
  - `data_timedelta`
  - `data_binary_other`
  - `write_parquet`
- `python/tests/conftest.py` defines reusable pytest fixtures:
  - `ctx` creates a `SessionContext`.
  - `database` registers a sample CSV dataset.
- `src/dataframe.rs` provides the `collect_record_batches_to_display` helper to fetch the first non-empty record batch and flag if more are available.
