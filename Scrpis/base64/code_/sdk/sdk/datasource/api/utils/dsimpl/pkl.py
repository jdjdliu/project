import dill
import os
import pickle

from sdk.datasource.extensions.bigshared.utils import ensure_data_dir_for_file

from . import utils


def write_pickle_helper(obj, path, use_dill):
    ensure_data_dir_for_file(path)
    M = dill if use_dill else pickle
    with open(path, "wb") as writer:
        M.dump(obj, writer, protocol=4)  # protocol 可序列化超过4GB文件
    utils.chown_path_bigquant(path)


def write_pickle(obj, use_dill=False, use_cache=False, **kwargs):
    def _write(obj, use_dill, **kwargs):
        source_id = utils.gen_id_for_temp_ds()
        path = utils.id_to_path(source_id, "v3")
        write_pickle_helper(obj, path, use_dill)

        # utils.on_create(source_id, file_type=utils.FILE_TYPE_PICKLE)
        return source_id

    source_id = _write(obj=obj, use_dill=use_dill, **kwargs)
    return source_id


def read_pickle(id, version, use_dill=False, return_use_dill=False):
    utils.on_visit(id, get_meta=True)
    path = utils.id_to_path(id, "v3")
    if not os.path.exists(path):
        print(id, "not found.")
        return None
    M = dill if use_dill else pickle
    if "T" not in id and "bigquant-" not in id:
        path += "/all.pkl"
    with open(path, "rb") as reader:
        obj = M.load(reader)

    if return_use_dill:
        obj = (obj, True)

    return obj
