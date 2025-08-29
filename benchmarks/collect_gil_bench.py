# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import math
import time

import pyarrow as pa
from datafusion import SessionContext, col
from datafusion import functions as f


def run(
    n_batches: int = 8,
    batch_size: int = 1_000_000,
    n_partitions: int | None = None,
) -> None:
    """Aggregate column 'a' across partitions and report runtime."""
    ctx = SessionContext()
    batches = []
    for i in range(n_batches):
        start = i * batch_size
        arr = pa.array(range(start, start + batch_size))
        batches.append(pa.record_batch([arr], names=["a"]))

    if n_partitions is None:
        n_partitions = n_batches
    n_partitions = max(1, min(n_partitions, n_batches))
    partition_size = math.ceil(len(batches) / n_partitions)
    partitions = [
        batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
    ]
    df = ctx.create_dataframe(partitions)

    start = time.perf_counter()
    df.aggregate([], [f.sum(col("a"))]).collect()
    duration = time.perf_counter() - start
    print(f"{n_batches} batches aggregated in {duration:.3f}s")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--batches",
        type=int,
        default=8,
        help="number of input batches to generate",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1_000_000,
        help="number of rows per batch",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=None,
        help="number of partitions to create (defaults to one per batch)",
    )
    args = parser.parse_args()
    run(
        n_batches=args.batches,
        batch_size=args.batch_size,
        n_partitions=args.partitions,
    )
