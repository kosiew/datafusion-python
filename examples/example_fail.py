from typing import TYPE_CHECKING, TypeGuard

if TYPE_CHECKING:
    from _typeshed import CapsuleType as _PyCapsule

def is_capsule(obj: object) -> TypeGuard[_PyCapsule]:
    return hasattr(obj, "__capsule__")

print("This will break soon...")
