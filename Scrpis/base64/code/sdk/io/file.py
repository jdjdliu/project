import os

from .settings import BASE_DATA_DIR


def data_path(*parts: str) -> str:
    s = BASE_DATA_DIR
    for p in parts:
        s = os.path.join(s, p)
    return s


def ensure_data_dir(*parts: str) -> str:
    s = data_path(*parts)
    if not os.path.exists(s):
        os.makedirs(s)
    return s


def ensure_data_dir_for_file(*parts: str) -> str:
    s = data_path(*parts)
    ensure_data_dir(os.path.dirname(s))
    return s
