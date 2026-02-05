# PR Response to Review Comments

## Overview

Thank you @timsaucer for the detailed review! Your comments have identified several important improvements needed to make the deprecation path smoother and the implementation more robust. Below are responses and action plans for each comment.

---

## Documentation: `docs/source/user-guide/dataframe/rendering.rst`

### Comment 1: Lines 60-61 - Inconsistent defaults and naming

**Issue:**
```python
min_rows_display = 20,   # Minimum number of rows to display
repr_rows = 10,          # Number of rows to display in __repr__
```

**Comment:** 
> It looks like the default here has `min_rows > max_rows`. Also should we have consistent naming of the two?
> Either `min_rows` and `max_rows` or `min_rows_display` and `max_rows_display`?
> I think the `_display` was differentiating what happens during a `display()` call vs `__repr__`, but I think these values get used during both calls.

**Response & Action Plan:**

You're absolutely right on both points:

1. **Default values issue**: Having `min_rows_display=20` and `max_rows=10` creates an invalid configuration. Both parameters use the same rendering logic path, so this is incorrect.

2. **Naming consistency**: The `_display` suffix is misleading since both parameters apply to all rendering contexts (both `__repr__` and explicit `display()` calls).

**Action items:**
- [ ] Change defaults in documentation to `min_rows_display=10, max_rows=10` (or similar valid configuration like `min_rows_display=5, max_rows=10`)
- [ ] Consider renaming to either:
  - Option A: `min_rows` and `max_rows` (simpler, clearer)
  - Option B: `min_rows_display` and `max_rows_display` (more verbose but consistent)
- [ ] Update all examples in documentation to use consistent naming
- [ ] Ensure validation logic enforces `min_rows_display <= max_rows`

**Recommendation:** I lean toward Option A (`min_rows`/`max_rows`) for simplicity unless there's a strong reason for the `_display` suffix.

---

### Comment 2: Lines 193-194 - Same naming/default issue

**Issue:**
```python
min_rows_display = 50,   # Always show at least 50 rows
max_rows = 20            # Show 20 rows in __repr__ output
```

**Comment:** 
> Same as above, difference between `_display` and without the trailer and also we have here `min_rows > max_rows`.

**Response & Action Plan:**

Same issues as above in a different example.

**Action items:**
- [ ] Fix the example to use valid configuration: `min_rows_display=20, max_rows=50` (or similar)
- [ ] Apply consistent naming per the decision in Comment 1
- [ ] Review all other documentation examples for similar issues

---

## Python: `python/datafusion/dataframe_formatter.py`

### Comment 3: Lines 167-169/256 - Duplicate type/default information

**Issue:**
```python
min_rows_display : int, default 10
    Minimum number of rows to display.
repr_rows : int, default 10
    Default number of rows to display in repr output.
```

**Comment:** 
> It's not about this PR per se, but maybe this is an opportunity to tighten up the comments here. We're repeating ourselves with the types and defaults. Those are already in the type hints.
> I think it's becoming customary to not duplicate that information and the argument line is the preferred place to keep it. That way we don't have to worry about maintaining the values in two places.

**Response & Action Plan:**

Excellent point! This is indeed Python best practice. Since type hints and default values are already in the signature, the docstring should focus on **what** the parameter does and **why** you'd use it, not repeat information that's already in the code.

**Action items:**
- [ ] Refactor docstring to follow NumPy/pandas style without type/default duplication:
  ```python
  Parameters
  ----------
  max_cell_length
      Maximum length of cell content before truncation.
  max_width
      Maximum width of the displayed table in pixels.
  max_memory_bytes
      Maximum memory in bytes for rendered data. Helps prevent performance
      issues with large datasets.
  min_rows_display
      Minimum number of rows to display even if memory limit is reached.
      Must not exceed max_rows.
  max_rows
      Maximum number of rows to display. Takes precedence over memory limits
      when fewer rows are requested.
  ```
- [ ] Apply this pattern consistently across all docstrings in the file
- [ ] Keep detailed explanation of behavior/constraints in the description section

---

### Comment 4 & 5: Lines 335-338, 351-354 - Why add accessors for deprecated property?

**Issue:**
```python
@property
def repr_rows(self) -> int:
    """Get the maximum number of rows (deprecated name)..."""
    return self._max_rows

@repr_rows.setter
def repr_rows(self, value: int) -> None:
    """Set the maximum number of rows using deprecated name..."""
    warnings.warn(...)
    self._max_rows = value
```

**Comment:** 
> If `repr_rows` is being deprecated, why add an accessor?
> Same, why add for deprecated?

**Response & Action Plan:**

This is a great question about deprecation strategy. The accessors are being added for **backward compatibility** during the deprecation period:

**Rationale:**
1. **User code may directly access the property**: Code like `formatter.repr_rows = 20` needs to continue working during the deprecation period
2. **Graceful migration path**: Users get a warning but their code doesn't break
3. **Custom formatter implementations**: External code that inherits from the formatter and accesses `repr_rows` directly will continue to work

However, I see your point - if users are unlikely to access these properties directly (most use `configure_formatter()`), then these accessors may be overkill.

**Questions for consideration:**
- Do we know of any user code that directly accesses `formatter.repr_rows`?
- What's our deprecation timeline? (1 release cycle, 2 release cycles?)
- Should we follow a "hard break" vs "soft deprecation" approach?

**Proposed approach:**
- [ ] Keep the accessors for at least 1-2 release cycles to allow users to migrate
- [ ] Add prominent deprecation warnings in release notes
- [ ] Remove in a future major version (e.g., next major version bump)
- [ ] Alternative: If direct property access is rare, we could remove accessors and only handle `repr_rows` in `configure_formatter()`

**Recommendation:** Keep the accessors for now with clear deprecation warnings, plan removal in next major version.

---

## Tests: `python/tests/test_dataframe.py`

### Comment 6: Line 1460-1462 - Use `large_df` instead of `df`

**Issue:**
```python
def test_html_formatter_memory_boundary_conditions(df, clean_formatter_state):
```

**Comment:** 
> Maybe switch to `large_df` instead of `df` here?

**Response & Action Plan:**

Excellent suggestion! The `large_df` fixture (100,000 rows) is much better suited for testing memory boundary conditions than the standard `df` fixture (3 rows).

**Action items:**
- [ ] Change test signature to use `large_df` fixture
- [ ] Update test to work with larger dataset
- [ ] Adjust assertions to account for the larger data size

---

### Comment 7: Lines 1468-1471 - Adjust max_rows for large_df

**Issue:**
```python
configure_formatter(max_memory_bytes=10 * MB, min_rows_display=1, max_rows=100)
```

**Comment:** 
> If you do switch to `large_df` then I think it may go above the 100 limit you have.

**Response & Action Plan:**

Good catch! With `large_df` containing 100,000 rows, the `max_rows=100` limit would be hit before we get useful memory limit testing.

**Action items:**
- [ ] Increase `max_rows` to a value well above what we expect to collect (e.g., `max_rows=100000`)
- [ ] Ensure we're actually testing memory limits, not row limits
- [ ] Add explicit comments about why we set these values

---

### Comment 8: Lines 1473-1476 - Test actual early termination of stream

**Issue:**
```python
# Test 1: Very small memory limit should still respect min_rows
configure_formatter(max_memory_bytes=10, min_rows_display=1)
```

**Comment:** 
> I think a better test is one where we *do* hit the memory limit well before we hit the min number of rows, hence the recommendation to switch to `large_df`.
> Actually, maybe we want a different dataframe, one where we know we have multiple batches instead of a single batch. The thing this isn't doing is verifying we've ended the stream early, but I think that would have to be a rust test instead of a pytest.

**Response & Action Plan:**

This is an excellent observation about the limitations of the current test. You're identifying two key testing gaps:

1. **Multi-batch streaming**: The current test may use a single-batch DataFrame, so we never actually test stream termination
2. **Rust-level verification**: Python tests can't verify that the stream was actually closed early vs. all data being collected and then truncated

**Action items:**

**Python test improvements:**
- [ ] Create a test DataFrame that definitely spans multiple record batches
  ```python
  # Create a multi-batch DataFrame explicitly
  batches = [pa.record_batch({"a": range(10000), "b": [f"str_{i}" for i in range(10000)]}) 
             for _ in range(10)]
  large_multi_batch_df = ctx.from_arrow(batches)
  ```
- [ ] Test with memory limit that would be exceeded by 2-3 batches but not 1 batch
- [ ] Verify behavior: partial data + truncation message + respects min_rows

**Rust test considerations:**
- [ ] Add Rust unit test in `src/dataframe.rs` that:
  - Creates a mock stream with known batch sizes
  - Sets a memory limit that should trigger after N batches
  - Verifies that only N batches were consumed (stream was closed early)
  - Verifies `has_more_rows` flag is set correctly

**Example Rust test structure:**
```rust
#[test]
fn test_stream_early_termination() {
    // Create a stream with known batches
    // Set memory limit to stop after 2 batches
    // Verify only 2 batches were consumed
    // Verify has_more_rows = true
}
```

**Note:** The Rust test would be more robust but also more complex. We should discuss if it's worth the effort vs. relying on integration testing.

---

## Rust: `src/dataframe.rs`

### Comment 9: Lines 150-156 - Backward-compatible attribute lookup

**Issue:**
```rust
let max_rows = get_attr(formatter, "max_rows", default_config.max_rows);
```

**Comment:** 
> Since users may have provided their own implementation for the formatter, I think we need to first try getting `max_rows`.
> If that fails, try getting `repr_rows`. If that fails, take default.
> When we remove `repr_rows` entirely after it's been deprecated for a few releases, then we can revert to this simpler logic.

**Response & Action Plan:**

This is a **critical point** for backward compatibility! Users with custom formatter implementations won't have `max_rows` attribute yet, only `repr_rows`. The current code would break their formatters.

**Action items:**
- [ ] Implement fallback logic in Rust:
  ```rust
  // Try new name first, fall back to deprecated name, then default
  let max_rows = get_attr(formatter, "max_rows", 0)
      .or_else(|| get_attr(formatter, "repr_rows", 0))
      .unwrap_or(default_config.max_rows);
  ```
  
  Or more explicitly:
  ```rust
  let max_rows = if let Ok(value) = formatter.getattr("max_rows") {
      value.extract::<usize>().unwrap_or(default_config.max_rows)
  } else if let Ok(value) = formatter.getattr("repr_rows") {
      value.extract::<usize>().unwrap_or(default_config.max_rows)
  } else {
      default_config.max_rows
  };
  ```

- [ ] Add a code comment explaining the fallback chain:
  ```rust
  // Try max_rows first (new name), fall back to repr_rows (deprecated),
  // then use default. This ensures backward compatibility with custom
  // formatter implementations during the deprecation period.
  ```

- [ ] Add test case for custom formatter with only `repr_rows` attribute
- [ ] Document deprecation timeline for when we can remove the fallback

**Timeline suggestion:**
- Release N: Add `max_rows`, deprecate `repr_rows` with warnings
- Release N+1: Keep both supported
- Release N+2: Remove `repr_rows` fallback in Rust
- Release N+3: Remove `repr_rows` property entirely

---

## Summary of Action Items

### High Priority (Breaks backward compatibility if not addressed)
1. ✅ **Rust fallback logic** (Comment 9) - Must be fixed to support custom formatters
2. ✅ **Documentation default values** (Comments 1, 2) - Current examples show invalid configurations

### Medium Priority (Quality & correctness improvements)
3. ✅ **Test improvements** (Comments 6, 7, 8) - Use `large_df`, test actual streaming behavior
4. ✅ **Naming consistency** (Comments 1, 2) - Decide on `min_rows`/`max_rows` vs `min_rows_display`/`max_rows_display`

### Low Priority (Code quality & maintainability)
5. ✅ **Docstring improvements** (Comment 3) - Remove duplicate type/default info
6. ✅ **Deprecation accessor strategy** (Comments 4, 5) - Document rationale or remove

### Additional Considerations
- Document deprecation timeline clearly in CHANGELOG
- Add migration guide for users with custom formatters
- Consider adding Rust-level tests for stream termination verification

---

## Questions for Discussion

1. **Naming convention**: Should we standardize on `min_rows`/`max_rows` or keep `min_rows_display`/`max_rows_display`? (My vote: simpler names)

2. **Deprecation timeline**: How many releases should we support `repr_rows` before removal? (My suggestion: 2-3 releases)

3. **Rust tests**: Should we invest in Rust unit tests for stream termination, or are the Python integration tests sufficient?

4. **Default values**: What should the actual defaults be? Current suggestion: `min_rows_display=10, max_rows=10` or `min_rows_display=5, max_rows=10`?

---

## Implementation Plan

If the above responses look good, I can proceed with implementing the fixes in this order:

1. **Critical fix**: Implement Rust fallback logic for `repr_rows` → `max_rows` transition
2. **Documentation**: Fix all examples and naming consistency
3. **Tests**: Switch to `large_df`, add multi-batch test cases
4. **Docstrings**: Clean up redundant type/default information
5. **Polish**: Add code comments explaining deprecation strategy

Please let me know if you'd like me to proceed with these changes or if you have different preferences for any of the action items above!
