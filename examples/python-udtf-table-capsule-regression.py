"""Regression example highlighting missing table provider capsule behavior.
"""

from __future__ import annotations

from datafusion import SessionContext, Table, udtf


def main() -> None:
    """Demonstrate current failure modes around table provider capsules."""

    ctx = SessionContext()
    capsule_source = Table(ctx.sql("SELECT 1 AS value"))

    @udtf("table_from_sql")
    def table_from_sql_udtf() -> Table:
        """Return a DataFusion Table constructed from a SQL query."""

        return capsule_source

    ctx.register_udtf(table_from_sql_udtf)

    try:
        ctx.sql("SELECT * FROM table_from_sql()").collect()
    except NotImplementedError as err:
        print(
            "Collecting from table_from_sql() failed because the table provider capsule is missing:",
            err,
        )

    ctx.register_table("numbers", Table(ctx.sql("SELECT 1 AS value")))

    numbers = ctx.catalog().schema("public").table("numbers")

    try:
        getattr(numbers, "__datafusion_table_provider__")
    except AttributeError as err:
        print(
            "Accessing __datafusion_table_provider__ on catalog table failed because the capsule attribute is missing:",
            err,
        )


if __name__ == "__main__":
    main()
