# AGENTS Instructions

This repository contains Python bindings for Rust's DataFusion.

## Development workflow
- Ensure git submodules are initialized: `git submodule update --init`.
- Build the Rust extension before running tests:
  - `uv run --no-project maturin develop --uv`
- Run tests with pytest:
  - `uv --no-project pytest .`

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
  - **Type-checking guards (`TCH001`)**: Place imports that are only needed for typing (e.g., `AggregateUDF`, `ScalarUDF`, `TableFunction`, `WindowUDF`, `NullTreatment`, `DataFrame`) inside a `if TYPE_CHECKING:` block.
  - **Docstring spacing (`D202`, `D205`)**: The summary line must be separated from the body with exactly one blank line, and there must be no blank line immediately after the closing triple quotes.
  - **Ternary suggestions (`SIM108`)**: Prefer single-line ternary expressions when Ruff requests them over multi-line `if`/`else` assignments.

## Notes
- The repository mixes Python and Rust; ensure changes build for both languages.
- If adding new dependencies, update `pyproject.toml` and run `uv sync --dev --no-install-package datafusion`.

## Rust insights

Below are a set of concise mental-model shifts and expert insights about Rust that are helpful when developing and reviewing the Rust parts of this repository. They emphasize how to think in terms of compile-time guarantees, capabilities, and algebraic composition rather than just language ergonomics.

1. Ownership → Compile-Time Resource Graph

> Stop seeing ownership as “who frees memory.”
> See it as a **compile-time dataflow graph of resource control**.

Every `let`, `move`, or `borrow` defines an edge in a graph the compiler statically verifies — ensuring linear usage of scarce resources (files, sockets, locks) **without a runtime GC**. Once you see lifetimes as edges, not annotations, you’re designing **proofs of safety**, not code that merely compiles.

2. Borrowing → Capability Leasing

> Stop thinking of borrowing as “taking a reference.”
> It’s **temporary permission to mutate or observe**, granted by the compiler’s capability system.

`&mut` isn’t a pointer — it’s a **lease with exclusive rights**, enforced at compile time. Expert code treats borrows as contracts:

* If you can shorten them, you increase parallelism.
* If you lengthen them, you increase safety scope.

3. Traits → Behavioral Algebra

> Stop viewing traits as “interfaces.”
> They’re **algebraic building blocks** that define composable laws of behavior.

A `Trait` isn't just a promise of methods; it’s a **contract that can be combined, derived, or blanket-implemented**. Once you realize traits form a behavioral lattice, you stop subclassing and start composing — expressing polymorphism as **capabilities, not hierarchies**.

4. `Result` → Explicit Control Flow as Data

> Stop using `Result` as an error type.
> It’s **control flow reified as data**.

The `?` operator turns sequential logic into a **monadic pipeline** — your `Result` chain isn’t linear code; it’s a dependency graph of partial successes. Experts design their APIs so every recoverable branch is an **encoded decision**, not a runtime exception.

5. Lifetimes → Static Borrow Slices

> Stop fearing lifetimes as compiler noise.
> They’re **proofs of local consistency** — mini type-level theorems.

Each `'a` parameter expresses that two pieces of data **coexist safely** within a bounded region of time. Experts deliberately model relationships through lifetime parameters to **eliminate entire classes of runtime checks**.

6. Pattern Matching → Declarative Exhaustiveness

> Stop thinking of `match` as a fancy switch.
> It’s a **total function over variants**, verified at compile time.

Once you realize `match` isn’t branching but **structural enumeration**, you start writing exhaustive domain models where every possible state is named, and every transition is **type-checked**.

Stop seeing `Option` as “value or no value.” Instead, see it as a lazy computation pipeline that only executes when meaningful. These combinators turn error-handling into data flow: once you think of absence as a first-class transformation, you can write algorithms that never mention control flow explicitly—and yet, they’re 100% safe and analyzable by the compiler.


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
