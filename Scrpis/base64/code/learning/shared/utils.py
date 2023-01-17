import datetime
import os
import types

BASE_DATA_DIR = '/var/app/data'


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


class Stopwatch:
    def __init__(self):
        self.start_time = datetime.datetime.now()

    @property
    def elapsed(self):
        return (datetime.datetime.now() - self.start_time).total_seconds()


def extend_methods(obj, **kwargs):
    for k, v in list(kwargs.items()):
        obj.__dict__[k] = types.MethodType(v, obj)
