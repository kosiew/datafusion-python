# RecordBatch conversion and the GIL

Profiling `DataFrame.collect` showed that converting each `RecordBatch` to
PyArrow via `rb.to_pyarrow(py)` spent considerable time holding the Python GIL.
For queries that return many batches this limited CPU utilisation because only
one conversion could run at a time.

The implementation has been updated to release the GIL and convert batches in
parallel using Rayon. This allows the CPU intensive portions of the conversion
to run concurrently.

A simple benchmark is provided in `benchmarks/collect_gil_bench.py`.
Run it twice to compare serial and parallel conversions:

```bash
RAYON_NUM_THREADS=1 python benchmarks/collect_gil_bench.py   # serial
python benchmarks/collect_gil_bench.py                      # parallel
```

On this container, collecting 128 1 M‑row batches took around 0.72 s
serially versus 1.53 s with the default thread pool, illustrating the
conversion cost and the overhead of parallel execution.
