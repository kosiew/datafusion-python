<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

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
RAYON_NUM_THREADS=1 python benchmarks/collect_gil_bench.py --batches 20 --partitions 8 --workload all # serial
RAYON_NUM_THREADS=8 python benchmarks/collect_gil_bench.py --batches 20 --partitions 8 --workload all # parallel
```
