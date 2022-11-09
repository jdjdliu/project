import os
import shutil

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.compute as pc
except:  # noqa
    pass
from . import dsimpl
from sdk.utils import BigLogger

log = BigLogger("bdb")


def write_bdb_helper(df, fn, chown_path_bigquant=True):
    """
    DataFrame写入bdb文件中

    Args:
        df pd.DataFrame: 数据df
        fn str: 文件路径
        chown_path_bigquant bool: 是否将路径权限修改为bigquant:bigquant, Defaults to True.
    """
    log.debug("write_bdb_helper: {} {}".format(fn, df.shape))
    tmp_file = fn + ".working"
    if os.path.exists(tmp_file):
        os.remove(tmp_file)
    bdb_table = pa.Table.from_pandas(df)
    with pa.OSFile(tmp_file, "wb") as sink:
        with pa.ipc.RecordBatchFileWriter(sink, bdb_table.schema) as writer:
            writer.write_table(bdb_table)
    shutil.move(tmp_file, fn)
    if chown_path_bigquant:
        dsimpl.chown_path_bigquant(fn)


def bdb_table_to_df(bdb_table):
    """bdb table 转为 df"""
    df = bdb_table.to_pandas(split_blocks=True, self_destruct=True)
    # TODO arrow 有的df数据类型不可变，不可进行操作 df['volume'] = df['volume'] / 100, 会改变数据类型
    # 可以使用 copy 可以修复, 继续调研优化， 报错 read-only
    # df = df.copy()
    return df


def concat_bdb_tables(table_list):
    """多个table拼接"""
    log.debug("concat_bdb_tables: {}".format(len(table_list)))
    table_list = [t for t in table_list if t]
    if not table_list:
        return None
    if len(table_list) == 1:
        table = table_list[0]
    else:
        table = pa.concat_tables(table_list, promote=True)
    return table


def read_bdb_helper(fn, bdb=False, memory_map=False):
    """
    读取bdb文件

    Args:
        fn str: 文件路径
        bdb bool: 默认为False, 是否返回DataFrame数据,  True: 返回数据为 bdb_table
        memory_map: bool  True使用memory_map, False 使用OSFile

    Returns:
        bdb_table or DataFrame
    """
    memory_map = memory_map or os.getenv("BDB_USE_MEMORY_MAP")
    log.debug("read_bdb_helper: {} {}, use_memory_map={}".format(fn, bdb, memory_map))
    reader = pa.memory_map if memory_map else pa.OSFile
    with reader(fn, "rb") as source:
        bdb_table = pa.ipc.RecordBatchFileReader(source).read_all()
    if bdb:
        return bdb_table
    return bdb_table_to_df(bdb_table)


def filter_bdb_table(bdb_table, instruments=None, product_codes=None, start_date=None, end_date=None, fields=None, date_field=None):
    if end_date and len(end_date) == 10:
        end_date = "{} 23:59:59".format(end_date)
    log.debug("filter bdb table {} {} {} {} {} {}".format(instruments, product_codes, start_date, end_date, fields, date_field))
    # 过滤数据
    mask_list = []
    all_column_names = bdb_table.column_names
    if fields:
        fields = set(fields).intersection(all_column_names)
        bdb_table = bdb_table.select(fields)

    if instruments and "instrument" in all_column_names:
        log.debug("filter condition add mask instruments={}".format(len(instruments)))
        mask_list.append(pc.is_in(bdb_table["instrument"], value_set=pa.array(instruments)))
    if product_codes and "product_code" in all_column_names:
        log.debug("filter condition add mask product_codes={}".format(len(product_codes)))
        mask_list.append(pc.is_in(bdb_table["product_code"], value_set=pa.array(product_codes)))
    if date_field and date_field in all_column_names:  # 判断 bdb_table中是否有date_field字段
        if start_date:
            log.debug("filter condition add mask start_date={}".format(start_date))
            mask_list.append(pc.greater_equal(bdb_table[date_field], pd.to_datetime(start_date)))
        if end_date:
            log.debug("filter condition add mask end_date={}".format(end_date))
            mask_list.append(pc.less_equal(bdb_table[date_field], pd.to_datetime(end_date)))
    mask = None
    for _m in mask_list:
        if mask:
            mask = pc.and_(mask, _m)
        else:
            mask = _m
    if mask:
        bdb_table = bdb_table.filter(mask)
    log.debug("filter bdb table ends ...")
    return bdb_table


def get_table_files(table, start_date=None, end_date=None, instruments=None, product_codes=None):
    """生成表文件路径"""

    import os
    import re
    import datetime
    from sdk.datasource.api.utils.bigschema import BigSchema, DatePartitioner
    from sdk.datasource.api.utils import gen_bdb_path, gen_table_path

    big_schema = BigSchema(table)
    partitioner = DatePartitioner(big_schema)
    base_path = gen_table_path(table=table)

    start_date = start_date or "2005-01-01"
    end_date = end_date or datetime.datetime.now().strftime("%Y-%m-%d")

    file_list = []
    if big_schema.partition_field:
        sub_dirs = []
        if big_schema.partition_field == "instrument" and instruments:
            sub_dirs = instruments
        elif big_schema.partition_field == "product_code":
            if instruments:
                product_codes = [re.findall(r"\D+", instrument) for instrument in instruments]
                product_codes = [p[0] for p in product_codes if p]
            if product_codes:
                sub_dirs = product_codes
        if not sub_dirs:
            sub_dirs = os.listdir(base_path) if os.path.exists(base_path) else []
        for filename in partitioner.on_read(start_date, end_date):
            for sub_dir in sub_dirs:
                fn = gen_bdb_path(table=table, filename=filename, sub_dir=sub_dir)
                if os.path.exists(fn):
                    file_list.append(fn)
    else:
        for filename in partitioner.on_read(start_date, end_date):
            fn = gen_bdb_path(table=table, filename=filename)
            if os.path.exists(fn):
                file_list.append(fn)
    return file_list


def delete_table_files(table):
    """删除数据文件"""
    all_files = get_table_files(table)
    for fn in all_files:
        os.remove(fn)
