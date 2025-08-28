import time

import pyarrow as pa
from datafusion import SessionContext


def run(n_batches: int = 8, batch_size: int = 1_000_000) -> None:
    ctx = SessionContext()
    batches = []
    for i in range(n_batches):
        start = i * batch_size
        arr = pa.array(range(start, start + batch_size))
        batches.append(pa.record_batch([arr], names=["a"]))

    df = ctx.create_dataframe([batches])

    start = time.perf_counter()
    df.collect()
    duration = time.perf_counter() - start
    print(f"{n_batches} batches collected in {duration:.3f}s")


if __name__ == "__main__":
    run()
