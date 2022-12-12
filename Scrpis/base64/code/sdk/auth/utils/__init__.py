import os
from typing import Optional


def current_user() -> Optional[str]:
    return os.getenv("JPY_USER", None)
