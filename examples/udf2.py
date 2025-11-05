
import pyarrow as pa
from datafusion import SessionContext, udf, column

uuid_type = pa.uuid()
chunks = [
    uuid_type.wrap_array(pa.array([], type=uuid_type.storage_type)),
    uuid_type.wrap_array(
        pa.array([b'\x00' * 16], type=uuid_type.storage_type)
    ),
]
chunked_ext = pa.chunked_array(chunks, type=uuid_type)

def combine_if_uuid(x: pa.ChunkedArray) -> pa.Array:
    # Diff 2 will deliver a broken type here → no extension metadata
    if x.type != uuid_type:
        raise TypeError(f"Expected uuid type, got {x.type}")

    return x.combine_chunks()

combine_udf = udf(
    combine_if_uuid,
    input_types=[pa.field("v", uuid_type)],
    return_type=pa.field("v", uuid_type),
    volatility="immutable",
)

ctx = SessionContext()
df = ctx.from_pydict({"uuid_col": chunked_ext})

# ❌ Breaks under Diff 2 (but works under Diff 1)
res = df.select(combine_udf(column("uuid_col")))
print(res.collect())
