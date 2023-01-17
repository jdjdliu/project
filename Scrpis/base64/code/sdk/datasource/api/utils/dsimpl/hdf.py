import os
import shutil

import pandas as pd

from sdk.datasource.extensions.bigshared.utils import ensure_data_dir_for_file
from sdk.utils import BigLogger

from . import utils


log = BigLogger("dsimpl.hdf")


def write_df_helper(df, path, key=utils.STORAGE_KEY):

    ensure_data_dir_for_file(path)
    new_file = path + ".working"
    if os.path.exists(new_file):
        os.remove(new_file)
    try:
        df.to_hdf(new_file, key, mode="w", complevel=1, complib="blosc:lz4", format="fixed")
    except Exception:  # noqa
        df.to_hdf(new_file, key, mode="w", complevel=1, complib="blosc:lz4", format="table")
    shutil.move(new_file, path)
    # utils.chown_path_bigquant(path)


def change_category_str(df):
    """将df中的category列自动转为str"""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    for col, dtype in dict(df.dtypes).items():
        if str(dtype) == "category":
            log.debug("change {} dtype category->str".format(col))
            df[col] = df[col].astype("str")
    return df


def write_df(df, key=utils.STORAGE_KEY, use_cache=False, **kwargs):
    def _write(df, key, **kwargs):
        key = key or utils.STORAGE_KEY
        source_id = utils.gen_id_for_temp_ds()

        # path = utils.id_to_intermediate_path(source_id)
        path = utils.id_to_path(source_id, "v3")
        write_df_helper(df, path, key)
        # utils.on_create(source_id, file_type=utils.FILE_TYPE_H5)
        return source_id

    source_id = _write(df=df, key=key, **kwargs)
    return source_id


def read_df(id, version, key=None):
    # utils.on_visit(id, get_meta=True)

    path = utils.id_to_path(id, "v3")
    if "T" not in id and "bigquant-" not in id:
        path += "/all.h5"
    if os.path.exists(path):
        df = read_df_helper(path, key)
    else:
        print(id, "not found.")
        df = None
    return df


def read_df_helper(path, key=None):
    local_temp_path = utils.local_temp_path(path)
    if os.path.exists(local_temp_path):
        path = local_temp_path

    with pd.HDFStore(str(path), mode="r") as store:
        if key is not None:
            return store[key]
        df_list = []
        for key in sorted(store.keys()):
            df_list.append(pd.read_hdf(store, key))
        if not df_list:
            return pd.DataFrame()
        if len(df_list) > 1:
            df = pd.concat(df_list, copy=False)
        else:
            df = df_list[0]
    return df


def iter_df(id, version):
    #meta = utils.on_visit(id, get_meta=True)
    #_id = meta.get("id")
    path = utils.id_to_path(id, "v3")
    if "T" not in id and "bigquant-" not in id:
        path += "/all.h5"

    with pd.HDFStore(path, mode="r") as store:
        for key in sorted(store.keys()):
            yield key, pd.read_hdf(store, key)
