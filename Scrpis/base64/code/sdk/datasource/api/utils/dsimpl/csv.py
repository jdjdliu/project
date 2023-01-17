
import os

import pandas as pd

from . import utils
from sdk.datasource.extensions.bigshared.utils import ensure_data_dir_for_file


def write_csv_helper(df, path, **kwargs):
    ensure_data_dir_for_file(path)
    df.to_csv(path, **kwargs)
    utils.chown_path_bigquant(path)


def write_csv(df, use_cache=False, **kwargs):
    def _write(df, **kwargs):
        source_id = utils.gen_id_for_temp_ds()
        path = utils.id_to_intermediate_path(source_id)
        write_csv_helper(df, path, **kwargs)

        utils.on_create(source_id, file_type=utils.FILE_TYPE_CSV)
        return source_id

    source_id = _write(df=df, **kwargs)
    return source_id


def read_csv(id, version, index_col, **kwargs):
    utils.on_visit(id, get_meta=True)
    path = utils.id_to_path(id, version)
    if 'T' not in id and 'bigquant-' not in id:
        path += '/all.csv'
    if os.path.exists(path):
        data_df = pd.read_csv(path, index_col=index_col, **kwargs)
    else:
        print(id, 'not found.')
        data_df = None
    return data_df
