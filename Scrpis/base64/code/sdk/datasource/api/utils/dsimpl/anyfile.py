import os

from . import utils
from sdk.datasource.extensions.bigshared.utils import ensure_data_dir_for_file


def open_file(id, version, writable, binary=False, **kwargs):
    if writable:
        path = utils.id_to_intermediate_path(id)
        ensure_data_dir_for_file(path)
        mode = "w"
        utils.chown_path_bigquant(os.path.dirname(path))
    else:
        meta = utils.on_visit(id, get_meta=True)
        _id = meta.get("id")
        path = utils.id_to_path(_id, version)
        file_type = meta.get("file_type")
        if "T" not in _id and "bigquant-" not in _id:
            if file_type == "pkl":
                path += "/all.pkl"
            elif file_type == "csv":
                path += "/all.csv"
            else:
                path += "/all.h5"
        mode = "r"

    if binary:
        mode += "b"
    return open(path, mode, **kwargs)


def close_file(handle, id, version, writable):
    handle.close()
    if writable:
        path = utils.id_to_intermediate_path(id)
        utils.chown_path_bigquant(path)
        utils.on_create(id, file_type=None, version=version)
