#!/usr/bin/env python3

import math
import time
import pyarrow as pa
from datafusion import SessionContext, col, SessionConfig
from datafusion import functions as f

def benchmark_with_different_configs():
    """Benchmark with different target_partitions configurations."""
    print("=== Benchmarking Different target_partitions Settings ===")
    
    n_batches = 100
    batch_size = 1_000_000
    
    # Test different target_partitions settings
    configs = [
        (1, "target_partitions=1"),
        (4, "target_partitions=4"), 
        (8, "target_partitions=8"),
        (16, "target_partitions=16"),
        (None, "default (10)")
    ]
    
    for target_partitions, description in configs:
        print(f"\n--- {description} ---")
        
        if target_partitions is None:
            ctx = SessionContext()
        else:
            config = SessionConfig({"datafusion.execution.target_partitions": str(target_partitions)})
            ctx = SessionContext(config)
        
        # Create data
        batches = []
        for i in range(n_batches):
            start = i * batch_size
            arr = pa.array(range(start, start + batch_size))
            batches.append(pa.record_batch([arr], names=["a"]))

        # Test with 8 input partitions
        partition_size = math.ceil(len(batches) / 8)
        partitions = [
            batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
        ]
        df = ctx.create_dataframe(partitions)
        df_agg = df.aggregate([], [f.sum(col("a"))])
        
        # Show execution plan
        plan_str = str(df_agg.explain())
        # Extract the DataSourceExec line
        for line in plan_str.split('\n'):
            if 'DataSourceExec:' in line:
                print(f"  {line.strip()}")
                break
        
        # Benchmark
        start = time.perf_counter()
        result = df_agg.collect()
        duration = time.perf_counter() - start
        print(f"  Time: {duration:.3f}s")

def test_no_repartitioning():
    """Test performance when avoiding repartitioning."""
    print("\n=== Testing Without Repartitioning ===")
    
    # Use target_partitions that matches our input partitions
    config = SessionConfig({"datafusion.execution.target_partitions": "8"})
    ctx = SessionContext(config)
    
    n_batches = 100
    batch_size = 1_000_000
    
    # Create data with exactly 8 partitions 
    batches = []
    for i in range(n_batches):
        start = i * batch_size
        arr = pa.array(range(start, start + batch_size))
        batches.append(pa.record_batch([arr], names=["a"]))

    partition_size = math.ceil(len(batches) / 8)
    partitions = [
        batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
    ]
    df = ctx.create_dataframe(partitions)
    df_agg = df.aggregate([], [f.sum(col("a"))])
    
    print("Execution plan:")
    print(df_agg.explain())
    
    # Benchmark multiple times
    times = []
    for i in range(3):
        start = time.perf_counter()
        result = df_agg.collect()
        duration = time.perf_counter() - start
        times.append(duration)
        print(f"Run {i+1}: {duration:.3f}s")
    
    avg_time = sum(times) / len(times)
    print(f"Average: {avg_time:.3f}s")

if __name__ == "__main__":
    benchmark_with_different_configs()
    test_no_repartitioning()
