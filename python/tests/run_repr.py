import pyarrow as pa
from datafusion import (
    SessionContext,
)
import time

RUNS = 1000


def run_dataframe_repr_long() -> None:
    ctx = SessionContext()
    # Create a DataFrame with more than 10 rows
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(list(range(15))),
            pa.array([x * 2 for x in range(15)]),
            pa.array([x * 3 for x in range(15)]),
        ],
        names=["a", "b", "c"],
    )
    df = ctx.create_dataframe([[batch]])

    output = repr(df)


def average_runtime(func, runs=RUNS):
    total_time = 0
    for _ in range(runs):
        start_time = time.time()
        func()
        end_time = time.time()
        total_time += end_time - start_time
    return total_time / runs


average_time = average_runtime(run_dataframe_repr_long)
print(f"Average runtime over {RUNS} runs: {average_time:.6f} seconds")
