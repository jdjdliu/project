import pandas as pd
import os
from sdk.datasource.extensions.bigshared.utils import ensure_data_dir_for_file

from . import utils


def open_df_store(id, version, writable):
    path = utils.id_to_path(id, "v3")
    ensure_data_dir_for_file(path)
    if writable:
        mode = 'w'
    else:
        mode = 'r'
    if 'T' not in id and 'bigquant-' not in id:
        path += '/all.h5'
    df_store = pd.HDFStore(path, mode=mode, complevel=1, complib='blosc:lz4')
    return df_store


def close_df_store(df_store, id, version, writable):
    df_store.close()
    # if writable:
    #     path = utils.id_to_intermediate_path(id)
    #     utils.chown_path_bigquant(path)
    #     utils.on_create(id, file_type=utils.FILE_TYPE_H5, version=version)
