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
