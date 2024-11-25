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

import pytest
from datafusion import SessionContext
import pyarrow as pa
from pyarrow.csv import write_csv

import pandas as pd


@pytest.fixture
def ctx():
    ctx = SessionContext()
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3, 4, 5, 6, 7], pa.int32()),
            pa.array(["a", "a", "a", "a", "a", "b", "b"], pa.string()),
            pa.array(
                [
                    pd.Timestamp("2020-10-05"),
                    pd.Timestamp("2020-10-05"),
                    pd.Timestamp("2020-10-05"),
                    pd.Timestamp("2020-10-06"),
                    pd.Timestamp("2020-10-06"),
                    pd.Timestamp("2020-10-06"),
                    pd.Timestamp("2020-10-07"),
                ],
                pa.timestamp("s"),
            ),
        ],
        names=["id", "grp", "event_time"],
    )
    ctx.register_record_batches("events", [[batch]])
    return ctx


@pytest.fixture
def database(ctx, tmp_path):
    path = tmp_path / "test.csv"

    table = pa.Table.from_arrays(
        [
            [1, 2, 3, 4],
            ["a", "b", "c", "d"],
            [1.1, 2.2, 3.3, 4.4],
        ],
        names=["int", "str", "float"],
    )
    write_csv(table, path)

    ctx.register_csv("csv", path)
    ctx.register_csv("csv1", str(path))
    ctx.register_csv(
        "csv2",
        path,
        has_header=True,
        delimiter=",",
        schema_infer_max_records=10,
    )
