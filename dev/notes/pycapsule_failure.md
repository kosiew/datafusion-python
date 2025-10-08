# PyCapsule Read Failure Analysis

## Symptom
Running `python examples/pycapsule_failure.py` now fails immediately with
``ValueError: Table provider capsule is missing a destructor`` when
``SessionContext.read_table`` attempts to coerce the fabricated capsule.

## Regression Surface
Commit range `9b4f1442^..6e449da5` teaches
``table_provider_from_pycapsule`` to recognize raw ``PyCapsule`` instances
before checking for a ``__datafusion_table_provider__`` attribute.
【F:src/utils.rs†L205-L222】 As soon as ``SessionContext.read_table`` sees the
dummy capsule exposed by the example it now calls straight into the capsule
conversion path without first wrapping it in ``Table``.

## Root Cause
The new fast-path reuses ``table_provider_from_capsule``, which enforces that
the capsule has a destructor originating from ``datafusion_ffi``’s helper.
【F:src/utils.rs†L193-L205】 The regression example still fabricates its capsule
with ``PyCapsule_New`` and passes ``NULL`` for the destructor.
【F:examples/pycapsule_failure.py†L10-L24】 Because no release callback is
registered, the validation now fails with the ``missing a destructor`` error.
Prior to the change, the example first wrapped the capsule in ``Table`` which
never triggered the destructor check, masking the issue.

## Runtime failure after 91b90f44
## Suggested Tasks
1. Provide a public helper (Python or Rust) that fabricates a minimal, but
   valid, table-provider capsule so that examples/tests no longer rely on
   ``PyCapsule_New`` with a ``NULL`` destructor.
2. Document the requirement that all externally supplied table-provider
   capsules must originate from ``datafusion_ffi`` helpers so the release hook
   is present; include guidance in ``examples/pycapsule_failure.py``.
3. Add a regression test that round-trips a capsule produced by the new helper
   through ``SessionContext.read_table`` to ensure the destructor validation and
   success path both work as intended.
