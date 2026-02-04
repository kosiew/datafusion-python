# Code Review: DataFrame Display Memory Limit Fix

## Review Date
February 4, 2026

## Commits Reviewed
- `fa9f2573`: Update DataFrameHtmlFormatter to enforce min_rows_display constraint and adjust default values
- `0563f6ca`: Refactor DataFrame formatter to replace repr_rows with max_rows and update related validations

## Summary
This PR addresses the issue where the memory size limit was not being respected when displaying large DataFrames. The root cause was a logical inconsistency where `repr_rows` (default: 10) was compared against `min_rows` (default: 20), which made the condition `rows_so_far < min_rows` always evaluate to True, preventing the memory limit from being enforced. The fix involves adjusting defaults, renaming for clarity, and enforcing proper validation constraints.

---

## Strengths ✅

### 1. **Addresses Root Cause Effectively**
The fundamental issue is fixed by:
- Aligning defaults: both `min_rows_display` and `max_rows` now default to 10
- Adding validation to ensure `min_rows_display <= max_rows`
- This prevents the logical trap where minimum always exceeded maximum

### 2. **Thoughtful API Renaming**
- `repr_rows` → `max_rows` is semantically clearer
- Better reflects the parameter's actual role (upper bound, not just "representation rows")
- Improves discoverability and documentation clarity

### 3. **Backward Compatibility Preservation**
The refactor maintains a deprecation path:
```python
if repr_rows is not None and repr_rows != max_rows:
    raise ValueError("Specify only max_rows (repr_rows is deprecated)")
```
- Allows gradual migration for existing code
- Clear error message guides users to new parameter

### 4. **Comprehensive Testing**
- Added validation tests for the new constraint
- Updated existing tests to use new naming
- Tests cover both direct instantiation and `configure_formatter()` path
- Memory limit tests present (though see concerns below)

### 5. **Cross-Language Consistency**
Changes are applied consistently across:
- Python formatter class (`dataframe_formatter.py`)
- Rust FFI layer (`dataframe.rs`)
- Documentation (`rendering.rst`)

### 6. **Clear Documentation Updates**
- Docstrings updated with new parameter names
- Constraint relationships documented ("must be <= max_rows")
- Comments updated in examples

---

## Issues & Suggestions 📝

### Severity: MEDIUM

#### 1. **Deprecation Strategy Could Be Stricter**
**Location**: `dataframe_formatter.py` lines 207-210

**Current behavior**:
```python
if repr_rows is not None and repr_rows != max_rows:
    msg = "Specify only max_rows (repr_rows is deprecated)"
    raise ValueError(msg)
```

**Concern**: This allows silently accepting `repr_rows` if it equals `max_rows`. While this maintains compatibility, it could hide usage of deprecated parameter.

**Suggestion**: 
- Consider warning vs error trade-off
- Document in CHANGELOG that `repr_rows` parameter will be removed in version X.Y
- Example improved approach:
```python
if repr_rows is not None:
    import warnings
    warnings.warn(
        "repr_rows parameter is deprecated, use max_rows instead",
        DeprecationWarning,
        stacklevel=2
    )
    if repr_rows != max_rows:
        msg = "Cannot specify both repr_rows and max_rows; use max_rows only"
        raise ValueError(msg)
    max_rows = repr_rows
```

---

#### 2. **Rust FFI Bridge Logic Could Fail Silently** 
**Location**: `src/dataframe.rs` lines 156-161

**Current code**:
```rust
let max_rows = get_attr(formatter, "max_rows", default_config.max_rows);
let repr_rows = get_attr(formatter, "repr_rows", max_rows);
let max_rows = if repr_rows != max_rows {
    repr_rows
} else {
    max_rows
};
```

**Concerns**:
1. **Variable shadowing**: `max_rows` is assigned twice, reducing clarity
2. **Silent override**: If both attributes exist with different values, no error/warning is raised to Rust side
3. **Inconsistent with Python**: Python raises ValueError when both differ; Rust silently picks `repr_rows`
4. **Documentation gap**: The `get_attr` fallback behavior isn't explained

**Suggestions**:
- Extract to a helper function for clarity:
```rust
fn resolve_max_rows(formatter: &Bound<'_, PyAny>, default: usize) -> PyResult<usize> {
    let max_rows = get_attr(formatter, "max_rows", default);
    let repr_rows = get_attr(formatter, "repr_rows", default);
    
    if repr_rows != max_rows && repr_rows != default {
        // User provided explicit repr_rows value
        return Ok(repr_rows);
    }
    Ok(max_rows)
}
```
- Add a validation check in Rust or ensure Python-side validation is sufficient
- Document the fallback precedence

---

#### 3. **Collection Condition Logic Warrants Verification**
**Location**: `src/dataframe.rs` line 1366

**Current condition**:
```rust
while (size_estimate_so_far < max_bytes && rows_so_far < max_rows) 
      || rows_so_far < min_rows {
```

**Assessment**: Logic appears correct post-fix. The condition ensures:
1. If memory/row limits not hit AND haven't reached max: continue
2. OR if haven't reached minimum: continue
3. This allows minimum rows to override both memory and row limits ✓

**Minor improvement suggestion**:
Add an explanatory comment about the semantics:
```rust
// Collect rows until EITHER:
// (a) we hit a limit (memory or max_rows), OR
// (b) we reach the guaranteed minimum
// This ensures min_rows always display, even if memory/row limits would prevent it
while (size_estimate_so_far < max_bytes && rows_so_far < max_rows) 
      || rows_so_far < min_rows {
```

---

#### 4. **Edge Case: Memory Calculation Under Investigation** 
**Location**: `src/dataframe.rs` lines 1376-1386

**Current memory scaling logic**:
```rust
if size_estimate_so_far > max_bytes {
    let ratio = max_bytes as f32 / size_estimate_so_far as f32;
    let total_rows = rows_in_rb + rows_so_far;
    let mut reduced_row_num = (total_rows as f32 * ratio).round() as usize;
    if reduced_row_num < min_rows {
        reduced_row_num = min_rows.min(total_rows);
    }
```

**Concern**: Floating-point rounding could result in:
- Very small dataframes: aggressive rounding down, then min_rows override might show fewer rows than requested
- Very large memory requests: ratio-based calculation might not distribute row reductions fairly across batches

**Suggestions for future improvement** (not blocking):
1. Add test cases for boundary conditions (DataFrame at exactly max_bytes, just under, just over)
2. Consider deterministic rounding strategy instead of `f32::round()`
3. Document expected behavior when memory limit is very close to actual usage

---

### Severity: LOW (Polish/Documentation)

#### 5. **Backward-Compatibility Alias Could Be Marked @deprecated** 
**Location**: `dataframe_formatter.py` lines 241-242

```python
self.max_rows = max_rows
# Backwards-compatible alias
self.repr_rows = max_rows
```

**Suggestion**: Consider adding property with deprecation warning:
```python
@property
def repr_rows(self) -> int:
    """Deprecated: use max_rows instead."""
    return self.max_rows

@repr_rows.setter
def repr_rows(self, value: int) -> None:
    warnings.warn("repr_rows is deprecated, use max_rows", DeprecationWarning)
    self.max_rows = value
```
This would surface deprecation to direct attribute access as well.

---

#### 6. **Test Coverage Gap: Deprecated repr_rows Path**
**Location**: `python/tests/test_dataframe.py`

**Observation**: Tests thoroughly cover the new `max_rows` parameter but don't explicitly test that deprecated `repr_rows` still works (even in backward-compatibility mode).

**Suggestion**: Add a test:
```python
def test_repr_rows_backward_compatibility(clean_formatter_state):
    """Verify that repr_rows parameter still works as deprecated alias."""
    # Should work when not conflicting with max_rows
    formatter = DataFrameHtmlFormatter(repr_rows=5)
    assert formatter.max_rows == 5
    assert formatter.repr_rows == 5
    
    # Should fail when conflicting
    with pytest.raises(ValueError, match="Specify only max_rows"):
        DataFrameHtmlFormatter(repr_rows=5, max_rows=10)
```

---

#### 7. **Documentation Could Be Clearer on Default Relationship**
**Location**: `docs/source/user-guide/dataframe/rendering.rst`

The documentation mentions setting `min_rows_display=20, max_rows=20` to get exactly 20 rows, but doesn't explain:
- What happens if DataFrame has < min_rows_display rows
- How memory limit interacts with both parameters
- Precedence order: Which limit triggers first?

**Suggestion**: Add a "Parameters Interaction" section:
```
The display respects these constraints in the following order:
1. Memory limit (max_memory_bytes) applies first
2. Row limit (max_rows) applies second  
3. Minimum guarantee (min_rows_display) overrides both if needed

To display exactly N rows: set min_rows_display=N and max_rows=N with sufficient memory.
```

---

#### 8. **Consider Helper Function for Configuration Validation**
**Location**: Both `dataframe_formatter.py` and `src/dataframe.rs`

**Observation**: Parameter validation logic is duplicated:
- Python: checks in `__init__` method
- Rust: checks in `validate()` method

**Suggestion for future refactoring** (not blocking for this PR):
Extract to shared validation logic or document the validation contract more explicitly to reduce duplication risk during maintenance.

---

## Behavior Verification

### ✅ The Bug is Fixed
**Before**: `min_rows=20 > repr_rows=10` → condition always true → memory limit ignored
**After**: `min_rows_display=10 <= max_rows=10` → memory limit can trigger → behavior correct

### ✅ Integration Points
- Python ↔ Rust: FFI properly marshals new parameter names
- Direct instantiation ↔ `configure_formatter()`: Both paths validated
- Attribute access: `formatter.repr_rows` maintains backward compatibility

---

## Decision

### ✅ **APPROVE WITH SUGGESTIONS**

**Reasoning**: 
- The PR solves the core problem (memory limit now respected)
- Implementation is solid with good test coverage
- Naming is improved for clarity
- Backward compatibility is maintained
- Non-blocking suggestions focus on polish and documentation

**Blocking Issues**: None

**Recommended Follow-ups** (for future work, not this PR):
1. Add deprecation warnings for `repr_rows` parameter
2. Improve Rust FFI logic clarity with helper function
3. Expand documentation on parameter interactions
4. Add test coverage for deprecated parameter path
5. Validate floating-point rounding behavior in edge cases

---

## Inline Comments by File

### `python/datafusion/dataframe_formatter.py`

| Line | Suggestion |
|------|-----------|
| 207-210 | Consider adding deprecation warning alongside ValueError |
| 241-242 | Could use @property decorator with warning for `repr_rows` |
| 235-242 | Extract validation logic to separate method for testability |

### `src/dataframe.rs`

| Line | Suggestion |
|------|-----------|
| 156-161 | Extract max_rows resolution to helper function (reduce shadowing) |
| 1366 | Add comment explaining min_rows override semantics |
| 1376-1386 | Consider test for boundary conditions (exact max_bytes size) |

### `python/tests/test_dataframe.py`

| Line | Suggestion |
|------|-----------|
| 1467-1475 | Add test for deprecated repr_rows backward compatibility |
| 1481-1511 | Add test for memory boundary conditions |

### `docs/source/user-guide/dataframe/rendering.rst`

| Line | Suggestion |
|------|-----------|
| 58-65 | Add "Parameters Interaction" section explaining precedence |
| 191+ | Document behavior when DataFrame rows < min_rows_display |

---

## Questions for Author

1. Was the choice to silently accept matching `repr_rows` values intentional, or should this always raise a deprecation warning?
2. Is the Rust-side precedence (repr_rows overrides max_rows if both exist) intentional to match Python behavior?
3. Are there any known issues with floating-point rounding in edge cases with very small or very large DataFrames?

