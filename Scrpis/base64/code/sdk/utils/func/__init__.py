import types
from typing import Any


def extend_class_methods(obj: Any, **kwargs: Any) -> None:
    for k, v in list(kwargs.items()):
        setattr(obj, k, types.MethodType(v, obj))
