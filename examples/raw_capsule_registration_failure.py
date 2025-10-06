"""Demonstrate how passing a raw PyCapsule triggers the dataset fallback.

Run with ``python examples/raw_capsule_registration_failure.py``.

This mirrors integrations that construct a :class:`datafusion.Table` directly
from the FFI PyCapsule returned by ``__datafusion_table_provider__``. After the
refactor that routes all inputs through ``RawTable`` the capsule is no longer
recognized, so the constructor falls back to the PyArrow dataset path and raises
``ValueError: dataset argument must be a pyarrow.dataset.Dataset object``.
"""

from __future__ import annotations

import ctypes

from datafusion import SessionContext, Table


def make_table_provider_capsule() -> object:
    """Create a dummy PyCapsule with the expected table provider name."""

    pycapsule_new = ctypes.pythonapi.PyCapsule_New
    pycapsule_new.restype = ctypes.py_object
    pycapsule_new.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    return pycapsule_new(None, b"datafusion_table_provider", None)


def main() -> None:
    """Attempt to use the capsule the same way existing callers do."""

    ctx = SessionContext()
    capsule = make_table_provider_capsule()

    try:
        Table(capsule)
    except ValueError as err:
        print("Constructing Table(capsule) failed:", err)

    try:
        ctx.register_table("capsule", capsule)
    except ValueError as err:
        print("Registering capsule with SessionContext failed:", err)


if __name__ == "__main__":
    main()
