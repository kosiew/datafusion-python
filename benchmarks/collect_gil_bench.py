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


def run_simple_aggregation(
    n_batches: int = 8,
    batch_size: int = 1_000_000,
    n_partitions: int | None = None,
) -> None:
    """Simple aggregation benchmark (original)."""
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
    print(f"Simple aggregation: {n_batches} batches in {duration:.3f}s")


def run_complex_computations(
    n_batches: int = 8,
    batch_size: int = 1_000_000,
    n_partitions: int | None = None,
) -> None:
    """CPU-intensive computations with multiple columns."""
    ctx = SessionContext()
    batches = []
    for i in range(n_batches):
        start = i * batch_size
        # Create multiple columns with different data types
        arr_a = pa.array(range(start, start + batch_size))
        arr_b = pa.array([x * 2.5 + 1.0 for x in range(start, start + batch_size)])
        arr_c = pa.array([x % 1000 for x in range(start, start + batch_size)])
        batches.append(pa.record_batch([arr_a, arr_b, arr_c], names=["a", "b", "c"]))

    if n_partitions is None:
        n_partitions = n_batches
    n_partitions = max(1, min(n_partitions, n_batches))
    partition_size = math.ceil(len(batches) / n_partitions)
    partitions = [
        batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
    ]
    df = ctx.create_dataframe(partitions)

    # CPU-intensive transformations
    df = df.select(
        col("a"),
        col("b"),
        col("c"),
        # Complex mathematical operations
        (col("a") * col("b") + col("c") * col("c")).alias("poly1"),
        (col("a") * col("a") * col("a") + col("b") * col("b")).alias("poly2"),
        (col("a") / (col("b") + 1.0) * col("c")).alias("ratio"),
        # More expensive operations
        f.sqrt(col("a") + col("b")).alias("sqrt_sum"),
        (col("a") * col("a")).alias("power2"),
        (col("b") * col("b") * col("b")).alias("power3"),
    )
    
    # Multiple filtering operations
    df = df.filter(col("a") % 100 < 50)
    df = df.filter(col("poly1") > 1000)
    df = df.filter(col("ratio") < 10000)

    # Group by with multiple aggregations
    df = df.aggregate(
        [col("c") % 10],
        [
            f.sum(col("poly1")).alias("sum_poly1"),
            f.avg(col("poly2")).alias("avg_poly2"),
            f.max(col("ratio")).alias("max_ratio"),
            f.min(col("sqrt_sum")).alias("min_sqrt"),
            f.count(col("a")).alias("count_rows"),
        ]
    )

    start = time.perf_counter()
    result = df.collect()
    duration = time.perf_counter() - start
    print(f"Complex computations: {n_batches} batches in {duration:.3f}s, {len(result)} result rows")


def run_string_processing(
    n_batches: int = 8,
    batch_size: int = 500_000,  # Smaller batches for string operations
    n_partitions: int | None = None,
) -> None:
    """CPU-intensive string processing operations."""
    ctx = SessionContext()
    batches = []
    for i in range(n_batches):
        start = i * batch_size
        # Create string data
        arr_id = pa.array([f"user_{x:08d}" for x in range(start, start + batch_size)])
        arr_email = pa.array([f"user{x}@example{x%10}.com" for x in range(start, start + batch_size)])
        arr_category = pa.array([f"category_{x%100:03d}" for x in range(start, start + batch_size)])
        arr_value = pa.array([x * 1.5 for x in range(start, start + batch_size)])
        
        batches.append(pa.record_batch(
            [arr_id, arr_email, arr_category, arr_value], 
            names=["id", "email", "category", "value"]
        ))

    if n_partitions is None:
        n_partitions = n_batches
    n_partitions = max(1, min(n_partitions, n_batches))
    partition_size = math.ceil(len(batches) / n_partitions)
    partitions = [
        batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
    ]
    df = ctx.create_dataframe(partitions)

    # String processing operations
    df = df.select(
        col("id"),
        col("email"),
        col("category"),
        col("value"),
        # String manipulations (CPU intensive)
        f.length(col("email")).alias("email_length"),
        f.upper(col("category")).alias("category_upper"),
        f.lower(col("email")).alias("email_lower"),
        f.length(col("id")).alias("id_length"),
    )
    
    # String-based filtering
    df = df.filter(f.length(col("email")) > 15)
    df = df.filter(f.length(col("category_upper")) > 10)
    df = df.filter(col("email_length") < 50)

    # Group by operations with string processing
    df = df.aggregate(
        [col("category")],  # Group by full category
        [
            f.sum(col("value")).alias("total_value"),
            f.avg(col("email_length")).alias("avg_email_len"),
            f.max(col("id_length")).alias("max_id_len"),
            f.count(col("id")).alias("count_users"),
        ]
    )

    start = time.perf_counter()
    result = df.collect()
    duration = time.perf_counter() - start
    print(f"String processing: {n_batches} batches in {duration:.3f}s, {len(result)} result rows")


def run_window_functions(
    n_batches: int = 8,
    batch_size: int = 1_000_000,
    n_partitions: int | None = None,
) -> None:
    """CPU-intensive window function operations."""
    ctx = SessionContext()
    batches = []
    for i in range(n_batches):
        start = i * batch_size
        arr_a = pa.array(range(start, start + batch_size))
        arr_group = pa.array([x % 1000 for x in range(start, start + batch_size)])
        arr_value = pa.array([x * 2.5 + (x % 100) for x in range(start, start + batch_size)])
        
        batches.append(pa.record_batch(
            [arr_a, arr_group, arr_value], 
            names=["a", "group_id", "value"]
        ))

    if n_partitions is None:
        n_partitions = n_batches
    n_partitions = max(1, min(n_partitions, n_batches))
    partition_size = math.ceil(len(batches) / n_partitions)
    partitions = [
        batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
    ]
    df = ctx.create_dataframe(partitions)

    # Note: Window functions in DataFusion Python may have limited support
    # Using group-by operations that require sorting and complex aggregations
    df = df.filter(col("value") > 100)
    df = df.select(
        col("group_id"),
        col("value"),
        (col("value") * col("value")).alias("value_squared"),
        f.sqrt(col("value")).alias("value_sqrt"),
    )

    # Multiple aggregations per group (CPU intensive)
    df = df.aggregate(
        [col("group_id")],
        [
            f.sum(col("value")).alias("sum_value"),
            f.avg(col("value_squared")).alias("avg_squared"),
            f.max(col("value_sqrt")).alias("max_sqrt"),
            f.min(col("value")).alias("min_value"),
            f.count(col("value")).alias("count_rows"),
        ]
    )

    start = time.perf_counter()
    result = df.collect()
    duration = time.perf_counter() - start
    print(f"Window/groupby operations: {n_batches} batches in {duration:.3f}s, {len(result)} result rows")


def run(
    n_batches: int = 8,
    batch_size: int = 1_000_000,
    n_partitions: int | None = None,
    workload: str = "all",
) -> None:
    """Run the specified workload(s)."""
    if workload == "simple" or workload == "all":
        run_simple_aggregation(n_batches, batch_size, n_partitions)
    
    if workload == "complex" or workload == "all":
        run_complex_computations(n_batches, batch_size, n_partitions)
    
    if workload == "strings" or workload == "all":
        run_string_processing(n_batches, batch_size // 2, n_partitions)  # Use smaller batches for strings
    
    if workload == "groupby" or workload == "all":
        run_window_functions(n_batches, batch_size, n_partitions)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="CPU-intensive benchmarks to demonstrate multi-threading benefits"
    )
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
    parser.add_argument(
        "--workload",
        type=str,
        default="all",
        choices=["simple", "complex", "strings", "groupby", "all"],
        help="type of workload to run: simple (basic aggregation), complex (mathematical operations), strings (string processing), groupby (group-by operations), or all",
    )
    args = parser.parse_args()
    
    print(f"\n\nRunning benchmark with {args.batches} batches, {args.batch_size} rows per batch")
    print(f"Partitions: {args.partitions or args.batches}, Workload: {args.workload}")
    print("-" * 60)
    
    run(
        n_batches=args.batches,
        batch_size=args.batch_size,
        n_partitions=args.partitions,
        workload=args.workload,
    )
