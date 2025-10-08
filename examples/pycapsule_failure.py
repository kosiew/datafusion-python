from __future__ import annotations

import ctypes

from datafusion import SessionContext, Table


# Keep the backing memory alive for the lifetime of the module so the capsule
# always wraps a valid (non-null) pointer. The capsule content is irrelevant for
# this regression example—we only need a non-null address.
_DUMMY_CAPSULE_BYTES = ctypes.create_string_buffer(b"x")

class CapsuleContainer:
    def __init__(self):
        self.__datafusion_table_provider__ = make_table_provider_capsule

def make_table_provider_capsule() -> object:
    """Create a dummy PyCapsule with the expected table provider name."""

    pycapsule_new = ctypes.pythonapi.PyCapsule_New
    pycapsule_new.restype = ctypes.py_object
    pycapsule_new.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    dummy_ptr = ctypes.cast(_DUMMY_CAPSULE_BYTES, ctypes.c_void_p)
    return pycapsule_new(dummy_ptr, b"datafusion_table_provider", None)


def main() -> None:
    """Attempt to use the capsule the same way existing callers do."""

    ctx = SessionContext()
    try:
        capsule = CapsuleContainer()
    except Exception as err:
        print("Creating the PyCapsule failed:", err)
        return


    ctx.read_table(capsule)
    

if __name__ == "__main__":
    main()