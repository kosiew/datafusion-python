# PyCapsule Read Failure Analysis

## Symptom
Running `python examples/pycapsule_failure.py` terminates the interpreter with a segmentation fault.

## Regression Surface
Commit range `9b4f1442^..d629ced2` replaced the `Table` wrapper-based API with a generic constructor that accepts arbitrary Python objects and auto-discovers how to turn them into a `TableProvider`. The new `PyTable::new` implementation in Rust now attempts to coerce any object that exposes `__datafusion_table_provider__` into an FFI provider. 【F:src/table.rs†L54-L77】

## Root Cause
`table_provider_from_pycapsule` only validates the capsule name before transmuting its pointer into an `FFI_TableProvider`. 【F:src/utils.rs†L127-L141】 The helper assumes the capsule contains a valid `FFI_TableProvider` allocation created by our bindings. The regression example fabricates a capsule with the correct name but with an arbitrary pointer (`ctypes.create_string_buffer`). 【F:examples/pycapsule_failure.py†L8-L24】 Because the new constructor now reaches this path for any `read_table` call, the bogus pointer is dereferenced immediately, corrupting memory and crashing the interpreter.

Prior to the refactor, callers could not pass arbitrary capsule-bearing objects to `SessionContext.read_table`; they first had to wrap them in `Table`/`RawTable`, which were only constructible through safe helpers that produced trusted capsules. The new auto-coercion path therefore widened the attack surface to unvalidated capsules, exposing the latent unsafety.

## Runtime failure after 91b90f44
Commit 91b90f44 changed :meth:`SessionContext.read_table` so that any object
exposing ``__datafusion_table_provider__`` is normalized through
``Table.from_table_provider_capsule`` before delegating to the Rust context.
【F:python/datafusion/context.py†L1189-L1198】 That helper now calls into the
private binding ``df_internal.catalog.RawTable.from_table_provider_capsule`` to
wrap the capsule, but the ``RawTable`` type exported from
``datafusion._internal`` does not currently expose such a constructor.
【F:python/datafusion/catalog.py†L176-L187】 At runtime the lookup therefore
raises ``AttributeError`` and prevents `examples/pycapsule_failure.py` from
running, regressing the original reproducer from a segfault into a hard failure.

## Suggested Tasks
1. Export a ``RawTable.from_table_provider_capsule`` constructor from the Rust
   bindings and ensure it becomes available through
   ``datafusion._internal.catalog`` during the wheel build so that the Python
   shim can locate it.
2. Add an integration test that imports ``datafusion._internal`` and asserts
   ``hasattr(df_internal.catalog.RawTable, "from_table_provider_capsule")``
   before exercising ``SessionContext.read_table`` with a raw capsule to catch
   regressions.
3. Consider extending ``table_provider_from_pycapsule`` so that
   ``RawTable.__new__`` can directly accept capsule instances (without going
   through the static helper) to reduce the surface area for Python/Rust API
   skew in the future.
