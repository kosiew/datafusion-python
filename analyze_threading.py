#!/usr/bin/env python3

import math
import time
import pyarrow as pa
from datafusion import SessionContext, col
from datafusion import functions as f

def analyze_execution_plan(n_batches=100, batch_size=1_000_000, n_partitions=8):
    """Analyze the execution plan to understand parallelization."""
    ctx = SessionContext()
    batches = []
    for i in range(n_batches):
        start = i * batch_size
        arr = pa.array(range(start, start + batch_size))
        batches.append(pa.record_batch([arr], names=["a"]))

    partition_size = math.ceil(len(batches) / n_partitions)
    partitions = [
        batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
    ]
    df = ctx.create_dataframe(partitions)
    df_agg = df.aggregate([], [f.sum(col("a"))])
    
    print(f"Configuration: {n_batches} batches, {n_partitions} partitions")
    print(f"Partition sizes: {[len(p) for p in partitions]}")
    print("Execution plan:")
    print(df_agg.explain())
    print("-" * 80)

def test_different_partition_configs():
    """Test different partitioning strategies."""
    print("=== Testing Different Partition Configurations ===")
    
    # Test 1: Many partitions (1 batch per partition)
    analyze_execution_plan(n_batches=100, n_partitions=100)
    
    # Test 2: Few partitions (many batches per partition)  
    analyze_execution_plan(n_batches=100, n_partitions=8)
    
    # Test 3: Medium partitions
    analyze_execution_plan(n_batches=100, n_partitions=25)

def benchmark_partition_strategies():
    """Benchmark different partitioning strategies."""
    print("=== Benchmarking Different Strategies ===")
    
    configs = [
        (100, 100, "1 batch per partition"),
        (100, 25, "4 batches per partition"), 
        (100, 8, "12-13 batches per partition"),
        (100, 4, "25 batches per partition"),
        (100, 1, "All batches in 1 partition")
    ]
    
    for n_batches, n_partitions, description in configs:
        ctx = SessionContext()
        batches = []
        for i in range(n_batches):
            start = i * 1_000_000
            arr = pa.array(range(start, start + 1_000_000))
            batches.append(pa.record_batch([arr], names=["a"]))

        partition_size = math.ceil(len(batches) / n_partitions)
        partitions = [
            batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
        ]
        df = ctx.create_dataframe(partitions)
        
        start = time.perf_counter()
        df.aggregate([], [f.sum(col("a"))]).collect()
        duration = time.perf_counter() - start
        
        print(f"{description}: {duration:.3f}s")

if __name__ == "__main__":
    test_different_partition_configs()
    benchmark_partition_strategies()
