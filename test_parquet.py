import datafusion as df
import pyarrow as pa

FILENAME = "/tmp/fixed_array_example.parquet"
ctx = df.SessionContext()

array = pa.array([[1.0, 2.0], [3.0, 4.0]], type=pa.list_(pa.float32(), 2))
table = pa.Table.from_pydict({"array": array})
df_table = ctx.from_arrow(table)
print("original schema:")
print(df_table.schema())

df_table.write_parquet(FILENAME)
print("roundtrip schema:")
print(ctx.read_parquet(FILENAME).schema())
