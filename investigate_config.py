#!/usr/bin/env python3

import math
import time
import pyarrow as pa
from datafusion import SessionContext, col, SessionConfig
from datafusion import functions as f

def check_target_partitions():
    """Check the default target_partitions setting."""
    print("=== DataFusion Configuration ===")
    ctx = SessionContext()
    
    # Create a simple test to see how many target partitions are configured
    batches = []
    for i in range(8):
        arr = pa.array(range(i*1000, (i+1)*1000))
        batches.append(pa.record_batch([arr], names=["a"]))
    
    # Test with different partitioning
    partitions = [[batch] for batch in batches]  # 8 partitions
    df = ctx.create_dataframe(partitions)
    df_agg = df.aggregate([], [f.sum(col("a"))])
    
    print("Plan with 8 input partitions:")
    print(df_agg.explain())
    print("-" * 80)

def test_manual_config():
    """Test with manually configured target_partitions."""
    print("=== Testing Manual Configuration ===")
    
    # Create config with specific target_partitions
    config = SessionConfig({"datafusion.execution.target_partitions": "8"})
    ctx = SessionContext(config)
    
    batches = []
    for i in range(100):
        start = i * 1_000_000
        arr = pa.array(range(start, start + 1_000_000))
        batches.append(pa.record_batch([arr], names=["a"]))

    # Create 8 partitions
    partition_size = math.ceil(len(batches) / 8)
    partitions = [
        batches[i : i + partition_size] for i in range(0, len(batches), partition_size)
    ]
    df = ctx.create_dataframe(partitions)
    df_agg = df.aggregate([], [f.sum(col("a"))])
    
    print("Plan with manual target_partitions=8:")
    print(df_agg.explain())
    print("-" * 80)

def benchmark_with_repartitioning():
    """Test performance with explicit repartitioning."""
    print("=== Benchmarking With Repartitioning ===")
    
    ctx = SessionContext()
    batches = []
    for i in range(100):
        start = i * 1_000_000
        arr = pa.array(range(start, start + 1_000_000))
        batches.append(pa.record_batch([arr], names=["a"]))

    # Create fewer partitions, let DataFusion repartition
    partitions = [batches]  # All in one partition
    df = ctx.create_dataframe(partitions)
    
    # Add an operation that might trigger repartitioning
    df_processed = df.select(col("a"), (col("a") * 2).alias("a2"))
    df_agg = df_processed.aggregate([], [f.sum(col("a")), f.sum(col("a2"))])
    
    print("Plan with repartitioning:")
    print(df_agg.explain())
    
    start = time.perf_counter()
    result = df_agg.collect()
    duration = time.perf_counter() - start
    print(f"Time with repartitioning: {duration:.3f}s")
    print("-" * 80)

if __name__ == "__main__":
    check_target_partitions()
    test_manual_config()
    benchmark_with_repartitioning()
