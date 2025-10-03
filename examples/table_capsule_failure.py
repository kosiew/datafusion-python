"""Demonstrate how missing __datafusion_table_provider__ breaks table UDTFs.

Run with ``python examples/table_capsule_failure.py``.

This example mirrors how advanced integrations unwrap ``Table`` instances via the
``__datafusion_table_provider__`` PyCapsule. The refactor that removed this method
means user-defined table functions returning ``Table`` now raise ``NotImplementedError``
and the script prints the resulting error message instead of crashing.
"""

from __future__ import annotations

from datafusion import SessionContext, Table, udtf


def main() -> None:
    """Register a Python table UDTF that returns a ``Table`` and trigger it."""

    ctx = SessionContext()
    failing_table = Table(ctx.sql("SELECT 1 AS value"))

    @udtf("capsule_dependent")
    def capsule_dependent_udtf() -> Table:
        """Return a ``Table`` so DataFusion unwraps it via the FFI capsule."""

        # Prior to the refactor the wrapper exposed ``__datafusion_table_provider__``
        # so this conversion succeeded. Without it the runtime raises a
        # ``NotImplementedError`` complaining about the missing attribute.
        return failing_table

    ctx.register_udtf(capsule_dependent_udtf)

    # Executing the UDTF now fails because ``Table`` no longer exposes the
    # ``__datafusion_table_provider__`` helper that PyTableFunction expects.
    try:
        ctx.sql("SELECT * FROM capsule_dependent()").collect()
    except NotImplementedError as err:
        # Document the regression by surfacing the missing capsule attribute
        # instead of crashing with a panic inside the execution engine.
        print(
            "capsule_dependent() failed due to missing __datafusion_table_provider__: "
            f"{err}"
        )


if __name__ == "__main__":
    main()
