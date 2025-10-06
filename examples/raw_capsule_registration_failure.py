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


# Keep the backing memory alive for the lifetime of the module so the capsule
# always wraps a valid (non-null) pointer. The capsule content is irrelevant for
# this regression example—we only need a non-null address.
_DUMMY_CAPSULE_BYTES = ctypes.create_string_buffer(b"x")


def make_table_provider_capsule() -> object:
    """Create a dummy PyCapsule with the expected table provider name."""

    pycapsule_new = ctypes.pythonapi.PyCapsule_New
    pycapsule_new.restype = ctypes.py_object
    pycapsule_new.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    dummy_ptr = ctypes.cast(_DUMMY_CAPSULE_BYTES, ctypes.c_void_p)
    return pycapsule_new(dummy_ptr, b"__datafusion_table_provider__", None)


def main() -> None:
    """Attempt to use the capsule the same way existing callers do."""

    ctx = SessionContext()
    try:
        capsule = make_table_provider_capsule()
    except Exception as err:
        print("Creating the PyCapsule failed:", err)
        return

    ctx.read_table(capsule)


if __name__ == "__main__":
    main()
