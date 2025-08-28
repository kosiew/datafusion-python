# RecordBatch conversion and the GIL

Profiling `DataFrame.collect` showed that converting each `RecordBatch` to
PyArrow via `rb.to_pyarrow(py)` spent considerable time holding the Python GIL.
Using `py-spy` on a query returning many batches indicated that more than
95 % of the conversion executed while the GIL was held, meaning the work was
effectively serialised.
For queries that return many batches this limited CPU utilisation because only
one conversion could run at a time.

The implementation now converts each batch to Arrow's C data (schema/array)
while the GIL is released, acquiring the GIL only to wrap those pointers into
PyArrow objects. This allows the CPU intensive portions of the conversion to
run fully in parallel.

A simple benchmark is provided in `benchmarks/collect_gil_bench.py`.
Run it twice to compare serial and parallel conversions:

```bash
RAYON_NUM_THREADS=1 python benchmarks/collect_gil_bench.py   # serial
python benchmarks/collect_gil_bench.py                      # parallel
```

On this container, collecting 128 1 M‑row batches took around 1.5 s with
`RAYON_NUM_THREADS=1` and 0.8 s with the default thread pool, demonstrating
that releasing the GIL allows conversions to run in parallel.
