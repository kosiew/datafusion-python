# Repository instructions for datafusion-python

## Style and Linting
- Python formatting and linting is handled by **ruff**.
- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) and these key
  ruff rules:
  - Use double quotes for string literals
  - Use explicit relative imports such as `from .module import Class`
  - Limit lines to 88 characters
  - Provide type hints for parameters and return values
  - Avoid unused imports
  - Prefer f-strings over other string formatting
  - Avoid `else` blocks after `return`
  - Use `isinstance()` instead of direct type comparison
  - Keep docstrings concise in Google format
  - Assign exception messages to variables before raising
- All modules, classes, and functions should include docstrings.
- Tests must use descriptive function names, pytest style assertions, and be
  grouped in classes when appropriate.
- Rust code must pass `cargo fmt`, `cargo clippy`, and `cargo tomlfmt`.
- Install the pre-commit hooks with `pre-commit install` and run them with
  `pre-commit run --files <file1> <file2>` or `pre-commit run --all-files`.

Install the required Rust components before running pre-commit:

```bash
rustup component add rustfmt clippy
```

If installation is blocked by a proxy, see the [offline installation guide](https://rust-lang.github.io/rustup/installation/other.html).

- The same checks can be run manually using the scripts in `ci/scripts`:
  - `./ci/scripts/python_lint.sh`
  - `./ci/scripts/rust_fmt.sh`
  - `./ci/scripts/rust_clippy.sh`
  - `./ci/scripts/rust_toml_fmt.sh`

## Rust Code Style

When generating Rust code for DataFusion, follow these guidelines:

1. Do not add unnecessary parentheses around `if` conditions:
   - Correct: `if some_condition`
   - Incorrect: `if (some_condition)`

2. Do not use `expect()` or `panic!()` except in tests - use proper error handling with `Result` types

3. Follow the standard Rust style conventions from rustfmt

## Code Organization
- Keep functions focused and under about 50 lines.
- Break complex tasks into well-named helper functions and reuse existing
  helpers when possible.
- Prefer adding parameters to existing functions rather than creating new
  versions with similar behavior.
- Avoid unnecessary abstractions and follow established patterns in the
  codebase.

## Comments
- Add meaningful comments for complex logic and avoid obvious comments.
- Start inline comments with a capital letter.

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
