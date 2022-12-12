import json
import os
from typing import Any, Callable, Optional, Type, TypeVar

T = TypeVar("T")


class OSEnv:
    @classmethod
    def general_loader(
        cls: Type["OSEnv"],
        name: str,
        loader: Callable[[str], T],
        default: T,
        description: Optional[str] = None,
    ) -> T:
        value_str: Optional[str] = os.getenv(name, None)
        value: T = loader(value_str) if value_str is not None else default
        cls.log(name=name, value=value, use_default=value_str is None, description=description)
        return value

    @staticmethod
    def log(
        name: str,
        value: Any,
        use_default: bool,
        description: Optional[str] = None,
    ) -> None:
        if type(value) == str:
            value = f'"{value}"'
        value = f"{value} ({type(value).__name__})"
        print(f"Loaded {name.ljust(48)} from {'default' if use_default else 'env    '} -> {value}")

    @classmethod
    def int(
        cls: Type["OSEnv"],
        name: str,
        default: int = 0,
        description: Optional[str] = None,
    ) -> int:
        return cls.general_loader(
            name=name,
            loader=lambda v: int(v),
            default=default,
            description=description,
        )

    @classmethod
    def float(
        cls: Type["OSEnv"],
        name: str,
        default: float = 0.0,
        description: Optional[str] = None,
    ) -> float:
        return cls.general_loader(
            name=name,
            loader=lambda v: float(v),
            default=default,
            description=description,
        )

    @classmethod
    def bool(
        cls: Type["OSEnv"],
        name: str,
        default: bool = False,
        description: Optional[str] = None,
    ) -> bool:
        return cls.general_loader(
            name=name,
            loader=lambda v: v.lower() in ["true", "1", "yes"],
            default=default,
            description=description,
        )

    @classmethod
    def json(
        cls: Type["OSEnv"],
        name: str,
        default: Optional[Any] = None,
        description: Optional[str] = None,
    ) -> Any:
        return cls.general_loader(
            name=name,
            loader=lambda v: json.loads(v),
            default=default,
            description=description,
        )

    # move this to bottom
    @classmethod
    def str(
        cls: Type["OSEnv"],
        name: str,
        default: str = "",
        description: Optional[str] = None,
    ) -> str:
        return cls.general_loader(
            name=name,
            loader=lambda v: str(v),
            default=default,
            description=description,
        )
