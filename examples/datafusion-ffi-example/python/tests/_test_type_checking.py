from datafusion import SessionContext, udf, udaf
from geodatafusion import native
ctx = SessionContext()
ctx.register_udaf(udaf(native.Extent()))
