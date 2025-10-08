"""Demonstrate the destructor requirement for table-provider PyCapsules.

DataFusion rejects table-provider capsules that are fabricated directly via
``PyCapsule_New`` because they lack the release hook installed by the
``datafusion_ffi`` helpers. Use :func:`datafusion.catalog.make_table_provider_capsule`
or the Rust-side constructors exposed by :mod:`datafusion_ffi` to obtain a valid
capsule instead of constructing one manually.
"""

from __future__ import annotations

import ctypes

from datafusion import SessionContext, Table
from datafusion.catalog import make_table_provider_capsule

_PYCAPSULE_NEW = ctypes.pythonapi.PyCapsule_New
_PYCAPSULE_NEW.restype = ctypes.py_object
_PYCAPSULE_NEW.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]


# Keep the backing memory alive for the lifetime of the module so the capsule
# always wraps a valid (non-null) pointer. The capsule content is irrelevant for
# this regression example—we only need a non-null address.
_DUMMY_CAPSULE_BYTES = ctypes.create_string_buffer(b"x")


def make_invalid_capsule() -> object:
    """Create an intentionally invalid PyCapsule without a destructor."""

    dummy_ptr = ctypes.cast(_DUMMY_CAPSULE_BYTES, ctypes.c_void_p)
    return _PYCAPSULE_NEW(dummy_ptr, b"datafusion_table_provider", None)


def main() -> None:
    """Showcase both the failure and the supported code paths."""

    ctx = SessionContext()

    try:
        ctx.read_table(make_invalid_capsule())
    except ValueError as err:
        print("Creating the PyCapsule failed, as expected:", err)

    valid_capsule = make_table_provider_capsule()
    ctx.read_table(valid_capsule)

    valid_capsule_for_table = make_table_provider_capsule()
    Table.from_table_provider_capsule(valid_capsule_for_table)


if __name__ == "__main__":
    main()
