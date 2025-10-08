# PyCapsule Read Failure Analysis

## Symptom
Running `python examples/pycapsule_failure.py` terminates the interpreter with a segmentation fault.

## Regression Surface
Commit range `9b4f1442^..d629ced2` replaced the `Table` wrapper-based API with a generic constructor that accepts arbitrary Python objects and auto-discovers how to turn them into a `TableProvider`. The new `PyTable::new` implementation in Rust now attempts to coerce any object that exposes `__datafusion_table_provider__` into an FFI provider. 【F:src/table.rs†L54-L77】

## Root Cause
`table_provider_from_pycapsule` only validates the capsule name before transmuting its pointer into an `FFI_TableProvider`. 【F:src/utils.rs†L127-L141】 The helper assumes the capsule contains a valid `FFI_TableProvider` allocation created by our bindings. The regression example fabricates a capsule with the correct name but with an arbitrary pointer (`ctypes.create_string_buffer`). 【F:examples/pycapsule_failure.py†L8-L24】 Because the new constructor now reaches this path for any `read_table` call, the bogus pointer is dereferenced immediately, corrupting memory and crashing the interpreter.

Prior to the refactor, callers could not pass arbitrary capsule-bearing objects to `SessionContext.read_table`; they first had to wrap them in `Table`/`RawTable`, which were only constructible through safe helpers that produced trusted capsules. The new auto-coercion path therefore widened the attack surface to unvalidated capsules, exposing the latent unsafety.

## Suggested Fixes
See the Suggested Tasks in the PR review comment for concrete follow-up work.
