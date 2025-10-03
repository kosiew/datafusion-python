"""Regression example highlighting missing table provider capsule behavior.
"""

from __future__ import annotations

from datafusion import SessionContext, Table, udtf


def main() -> None:
    """Demonstrate current failure modes around table provider capsules."""

    ctx = SessionContext()

    @udtf("table_from_sql")
    def table_from_sql_udtf() -> Table:
        """Return a DataFusion Table constructed from a SQL query."""

        return Table(ctx.sql("SELECT 1 AS value"))

    ctx.register_udtf(table_from_sql_udtf)

    result = ctx.sql("SELECT * FROM table_from_sql()").collect()
    as_pydict = [batch.to_pydict() for batch in result]
    print("table_from_sql() returned:", as_pydict)
    assert as_pydict == [{"value": [1]}]

    ctx.register_table("numbers", Table(ctx.sql("SELECT 1 AS value")))

    numbers = ctx.catalog().schema("public").table("numbers")

    try:
        getattr(numbers, "__datafusion_table_provider__")
    except AttributeError as err:
        print("Accessing __datafusion_table_provider__ on catalog table failed:", err)


if __name__ == "__main__":
    main()
