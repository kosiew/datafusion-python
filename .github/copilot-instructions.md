# Copilot Instructions

## Python Code Style

When generating Python code for the DataFusion Python project, adhere to the following style guidelines:

1. Follow the [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide
2. Comply with Ruff lint rules, specifically:

   - Use double quotes for string literals
   - Use explicit relative imports (e.g., `from .module import Class`)
   - Limit line length to 88 characters (Black formatter standard) (prevent E501, W505)
   - Use type hints for function parameters and return values
   - Avoid unused imports
   - Use f-strings instead of `.format()` or `%` formatting
   - Avoid unnecessary `else` statements after `return`
   - Use `isinstance()` instead of comparing types directly
   - Keep docstrings concise and follow Google-style docstring format
   - Assign exception message strings to variables before raising (prevent EM101)

     ```python
     # Correct:
     msg = "Invalid input value"
     raise ValueError(msg)

     # Incorrect:
     raise ValueError("Invalid input value")
     ```

3. Include comprehensive docstrings for:

   - Modules
   - Classes
   - Functions/methods

4. For tests:
   - Use meaningful test function names that describe what is being tested
   - Group related tests in classes
   - Use pytest style assertions instead of unittest style

## Rust Code Style

When generating Rust code for DataFusion, follow these guidelines:

1. Do not add unnecessary parentheses around `if` conditions:

   - Correct: `if some_condition`
   - Incorrect: `if (some_condition)`

2. Follow the standard Rust style conventions from rustfmt

### Code Organization

- Keep functions concise and focused on a single task
- Aim for functions under 40-50 lines of code
- Break long or complex operations into smaller, well-named helper functions
- Before creating new utility functions, check if existing helper functions in the codebase can be reused or extended
- Consider adding parameters to existing functions rather than creating similar parallel implementations
- Don't overengineer; avoid creating abstractions that are not needed
- Look for similar patterns in the codebase and follow established conventions

## Comments

- Add meaningful comments for complex logic
- Avoid obvious comments
- Start inline comments with a capital letter

## Example Style

```python
from typing import Optional, List

def process_data(data: List[str], max_length: Optional[int] = None) -> List[str]:
    """Process a list of string data.

    Args:
        data: List of strings to process
        max_length: Optional maximum length for each string

    Returns:
        List of processed strings

    Raises:
        ValueError: If invalid data is provided
    """
    if not data:
        raise ValueError("Empty data list provided")

    result = []
    for item in data:
        # Skip empty items
        if not item.strip():
            continue

        processed = item.strip().lower()
        if max_length is not None and len(processed) > max_length:
            processed = processed[:max_length]

        result.append(processed)

    return result
```
