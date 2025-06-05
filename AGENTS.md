# Repository instructions for datafusion-python

## Style and Linting
- Python formatting and linting is handled by **ruff**.
- Rust code must pass `cargo fmt`, `cargo clippy`, and `cargo tomlfmt`.
- Install the pre-commit hooks with `pre-commit install` and run them with
  `pre-commit run --files <file1> <file2>` or `pre-commit run --all-files`.
- The same checks can be run manually using the scripts in `ci/scripts`:
  - `./ci/scripts/python_lint.sh`
  - `./ci/scripts/rust_fmt.sh`
  - `./ci/scripts/rust_clippy.sh`
  - `./ci/scripts/rust_toml_fmt.sh`

## Running Tests
1. Ensure submodules are initialized:
   ```bash
   git submodule update --init
   ```
2. Create a development environment and install dependencies:
   ```bash
   uv sync --dev --no-install-package datafusion
   ```
3. Build the Python extension:
   ```bash
   uv run --no-project maturin develop --uv
   ```
4. Execute the test suite:
   ```bash
   uv run --no-project pytest -v .
   ```

## Building Documentation
- Documentation dependencies can be installed with the `docs` group:
  ```bash
  uv sync --dev --group docs --no-install-package datafusion
  ```
- Build the docs with:
  ```bash
  uv run --no-project maturin develop --uv
  uv run --no-project docs/build.sh
  ```
- The generated HTML appears under `docs/build/html`.
