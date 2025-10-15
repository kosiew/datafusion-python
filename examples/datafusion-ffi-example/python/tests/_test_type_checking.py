from datafusion import SessionContext, udaf
from geodatafusion import native


def test_udaf_accepts_capsule() -> None:
    ctx = SessionContext()
    ctx.register_udaf(udaf(native.Extent()))
