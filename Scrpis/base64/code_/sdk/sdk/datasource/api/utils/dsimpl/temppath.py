import os
import shutil

from sdk.datasource.extensions.bigshared.utils import ensure_data_dir_for_file

from . import utils


def open_temp_path(id, version, writable):
    if not writable:
        meta = utils.on_visit(id, get_meta=True)
        meta_id = meta.get("id")
        temp_path = '/tmp/bq_ds_' + meta_id

        path = utils.id_to_path(meta_id, version)
        if 'T' not in meta_id and 'bigquant-' not in meta_id:
            path += '/all.h5'
        if os.path.lexists(temp_path):
            os.remove(temp_path)
        os.symlink(path, temp_path)
    else:
        temp_path = '/tmp/bq_ds_' + id

    return temp_path


def close_temp_path(id, version, writable, temp_path):
    if writable:
        path = utils.id_to_intermediate_path(id)
        ensure_data_dir_for_file(path)

        shutil.move(temp_path, path)
        utils.chown_path_bigquant(path)
        utils.on_create(id, file_type=None, version=version)


def on_temp_path(id, version, writable, func):
    temp_path = open_temp_path(id, version, writable)
    try:
        return func(temp_path)
    finally:
        close_temp_path(id, version, writable, temp_path)
