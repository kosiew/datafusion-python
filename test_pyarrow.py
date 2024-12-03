import datafusion as df
import pyarrow as pa
import pyarrow.parquet as pq

FILENAME = "/tmp/fixed_array_example_pyarrow.parquet"
ctx = df.SessionContext()

array = pa.array([[1.0, 2.0], [3.0, 4.0]], type=pa.list_(pa.float32(), 2))
table = pa.Table.from_pydict({"array": array})

print("original schema:")
print(table.schema)

pq.write_table(table, FILENAME)
print("roundtrip schema:")
print(ctx.read_parquet(FILENAME).schema())
