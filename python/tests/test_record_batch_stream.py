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
