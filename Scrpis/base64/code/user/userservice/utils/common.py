import os

from userservice.settings import BASE_DATA_DIR


def data_path(*parts):
    s = BASE_DATA_DIR
    for p in parts:
        s = os.path.join(s, p)
    return s


def ensure_data_dir(*parts):
    s = data_path(*parts)
    if not os.path.exists(s):
        os.makedirs(s)
    return s


def ensure_data_dir_for_file(*parts):
    s = data_path(*parts)
    ensure_data_dir(os.path.dirname(s))
    return s
