# udf_diff2_failure_example.py

import pyarrow as pa
from datafusion import SessionContext, udf, col

# Define UUID extension type and source array
uuid_type = pa.uuid()
storage = pa.array(
    [b"\x00" * 16, b"\x01" * 16],
    type=uuid_type.storage_type
)
uuid_array = uuid_type.wrap_array(storage)

# Correct decorator pattern for Diff 2
@udf(
    input_types=[pa.field("v", uuid_type)],
    return_type=pa.field("v", uuid_type),
    volatility="immutable",
    name="ident",
)
def ident(x):
    if not isinstance(x, pa.ExtensionArray):
        # This is where Diff 2 breaks: x is a FixedSizeBinaryArray
        raise TypeError(f"Expected ExtensionArray, got {type(x)}")
    return x

ctx = SessionContext()
df = ctx.from_pydict({"uuid_col": uuid_array})

# This should trigger the failure due to missing extension type handling in Diff 2
try:
    result = df.select(ident(col("uuid_col"))).collect()
    print(result)
except Exception as e:
    print(f"❌ UDF failed: {e}")
