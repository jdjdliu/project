
import os

import numpy as np

from sdk.datasource.settings import table_base_folder_map, USER_ALIAS_SUFFIX, USERPRIVATE_FOLDER, \
    BASE_PATH, DATA_BASE_DIR_NAME


# 默认文件目录名称
default_dir_name = 'bigquant'


def gen_table_path(table, sub_dir=None, auto_mkdir=False, base_dir=None):
    """
    生成数据目录

    Args:
        table: str 表名
        sub_dir: str 子目录名(可选，默认分字段存储，子目录名为字段名)
        auto_mkdir: bool 自动创建文件目录
        base_dir: str 目录名称

    Returns: str Path

    """
    # 文件目录, 特殊指定路径, 默认是bigquant
    base_folder = table_base_folder_map.get(table, default_dir_name)

    if table.endswith(USER_ALIAS_SUFFIX):
        base_folder = USERPRIVATE_FOLDER
    base_folder_path = os.path.join(os.path.join(BASE_PATH, base_folder), DATA_BASE_DIR_NAME)

    if base_dir:
        _base_dir = base_dir
    elif table.startswith("level"):
        _base_dir = 'high_freq'
    elif table.startswith('alpha'):
        _base_dir = 'alpha'

    elif 'CN_STOCK' in table:
        _base_dir = 'cn_stock'
    elif 'CN_FUTURE' in table:
        _base_dir = 'cn_future'
    elif 'CN_OPTION' in table:
        _base_dir = 'cn_option'
    elif 'CN_FUND' in table:
        _base_dir = 'cn_fund'
    elif 'CN_BOND' in table:
        _base_dir = 'cn_bond'
    elif 'CN_CONBOND' in table:
        _base_dir = 'cn_conbond'
    else:
        _base_dir = 'other_data'

    base_path = os.path.join(base_folder_path, _base_dir)
    path = os.path.join(base_path, table)
    if sub_dir:
        path = os.path.join(path, sub_dir)
    if not os.path.exists(path) and auto_mkdir:
        os.makedirs(path)
    return path


def gen_bdb_path(table, filename, sub_dir=None, auto_mkdir=False):
    # auto_mkdir：写入数据时设置为 True; 读取数据时设置为 False
    # TODO 全部转为bdb后缀后删除此兼容代码
    base_path = gen_table_path(table=table, sub_dir=sub_dir, auto_mkdir=auto_mkdir)
    fn = os.path.join(base_path, "{}.bdb".format(filename))
    # if not auto_mkdir and not os.path.exists(fn):
    #     fn = os.path.join(base_path, "{}.arrow".format(filename))
    return fn


def gen_pkl_path(table, sub_dir=None, auto_mkdir=False):
    base_path = gen_table_path(table, sub_dir=sub_dir, auto_mkdir=auto_mkdir, base_dir='pickle')
    fn = os.path.join(base_path, 'all.pkl')
    return fn


def gen_patch_path(table, auto_mkdir=False):
    """数据修改补丁目录"""
    base_path = os.path.join(BASE_PATH, "{}/{}".format(default_dir_name, DATA_BASE_DIR_NAME))
    patch_path = os.path.join(base_path, 'patch_data/{}'.format(table))
    if auto_mkdir and not os.path.exists(patch_path):
        os.makedirs(patch_path)
    return patch_path


def get_metadata(alias):
    from sdk.datasource import UpdateDataSource
    update_ds = UpdateDataSource()
    return update_ds.get_metadata(alias=alias)


def filter_df(df, start_date, end_date, big_schema, instruments=None, all_fields=None, product_codes=None):
    date_field = big_schema.date_field
    if end_date and len(end_date) == 10:
        end_date = "{} 23:59:59".format(end_date)
    if date_field and (start_date or end_date):
        df = df[df[date_field].between(start_date, end_date)]
    if instruments and 'instrument' in df.columns:
        df = df[df.instrument.isin(instruments)]
    if product_codes and 'product_code' in df.columns:
        df = df[df.product_code.isin(product_codes)]
    if all_fields:
        df = df[all_fields]
    return df


def compare_diff(df1, df2, primary_key=None):
    """
    在 df1 中存在，df2 中不存在的

    Args:
        df1 (DataFrame): df1
        df2 (DataFrame): df1
        primary_key (List, optional): 主键字段, 用于对比两个df差异时的主键. Defaults to None.

    Returns:
        only_in_df1, both_df
            only_in_df1: DataFrame   在 df1 中存在，df2 中不存在的
            both_df: DataFrame   两个都共有的df, 也就是从df1中删除的公共df
    """
    if not primary_key:
        primary_key = list(df1.columns)
    all_pk_list = df2[primary_key].apply(tuple, axis=1).to_list()
    condition = df1[primary_key].apply(tuple, axis=1).isin(all_pk_list)
    # 在 df1 中存在，df2 中不存在的
    only_in_df1 = df1[~condition]
    # 两个都共有的df, 也就是从df1中删除的公共df
    both_df = df1[condition]
    return only_in_df1, both_df


def ensure_dtypes(df, schema=None, table=None):
    """确保数据类型和schema中一致"""
    from sdk.datasource.api.utils.bigschema import BigSchema
    big_schema = BigSchema(table=table, schema=schema)
    df_columns = list(df.columns)
    if big_schema.date_field and big_schema.date_field not in df_columns:
        raise Exception("your data df lost date field [{}]!".format(big_schema.date_field))
    if big_schema.primary_key:
        # 更新数据前，先检查是否含有所有 schema 中的 primary_key 字段
        for primary_key in big_schema.primary_key:
            if primary_key not in df_columns:
                raise Exception("your data lost primary_key [{}] !".format(primary_key))
        df.drop_duplicates(list(big_schema.primary_key), keep='last', inplace=True)

    fields = big_schema.fields
    if not fields:
        raise Exception("Your schema lost key 'fields', schema={}".format(schema))
    for field, dtype in dict(df.dtypes).items():
        dtype = str(dtype)

        # 将指定的date_field字段强制转化为 datetime64 类型
        if field == big_schema.date_field:
            schema["fields"][field]['type'] = 'datetime64[ns]'

        if field not in fields:
            schema["fields"][field] = {"type": dtype, "desc": ""}
            continue
        else:
            schema_dtype = fields[field].get("type")

        if dtype != schema_dtype and "datetime" not in schema_dtype:
            try:
                if "float" in dtype and "int" in schema_dtype:
                    df[field] = df[field].replace(np.nan, 0)
                    df[field] = df[field].replace(np.inf, 0)
                df_field = df[field].astype(schema_dtype)
                df[field] = df_field
            except Exception as e:
                print("failed change data dtype: {} {} -> {}, {}".format(field, dtype, schema_dtype, e))
    return df, schema


def sorted_and_drop_dumplicate(df, primary_key=None, date_field=None):
    if primary_key:
        df = df.drop_duplicates(primary_key, keep='last')
    sort_columns = []
    if date_field:
        sort_columns = [date_field]
    if 'instrument' in df.columns:
        sort_columns.append("instrument")
    if sort_columns:
        df.sort_values(sort_columns, inplace=True)
    return df
