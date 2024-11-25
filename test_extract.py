import pyarrow as pa
import pandas as pd
from datafusion import SessionContext


def main():
    # Create a SessionContext
    ctx = SessionContext()

    # Create a RecordBatch with event_time values
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3, 4, 5, 6, 7], pa.int32()),
            pa.array(["a", "a", "a", "a", "a", "b", "b"], pa.string()),
            pa.array(
                [
                    pd.Timestamp("2020-10-05"),
                    pd.Timestamp("2020-10-05"),
                    pd.Timestamp("2020-10-05"),
                    pd.Timestamp("2020-10-06"),
                    pd.Timestamp("2020-10-06"),
                    pd.Timestamp("2020-10-06"),
                    pd.Timestamp("2020-10-07"),
                ],
                pa.timestamp("s"),
            ),
        ],
        names=["id", "grp", "event_time"],
    )

    # Register the RecordBatch as a table named "events"
    ctx.register_record_batches("events", [[batch]])

    # Run the EXTRACT query
    result = ctx.sql(
        "SELECT EXTRACT(MONTH FROM event_time) AS month, COUNT(*) AS event_count "
        "FROM events "
        "GROUP BY EXTRACT(MONTH FROM event_time)"
    ).collect()

    # Convert the result to a PyArrow Table
    result = pa.Table.from_batches(result)

    # Convert the result to a dictionary
    rd = result.to_pydict()

    # Print the result
    print(rd)

    # Verify the result
    assert dict(zip(rd["month"], rd["event_count"])) == {10: 7}


if __name__ == "__main__":
    main()
