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


def test_record_batch_stream_next(ctx):
    stream = ctx.sql("SELECT 1 as a").execute_stream()
    batch = next(stream)
    assert batch.to_pyarrow().num_rows == 1
    with pytest.raises(StopIteration):
        next(stream)


@pytest.mark.asyncio
async def test_record_batch_stream_anext(ctx):
    stream = ctx.sql("SELECT 1 as a").execute_stream()
    batch = await stream.__anext__()
    assert batch.to_pyarrow().num_rows == 1
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()
