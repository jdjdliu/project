import pickle
import zlib
import base64

import requests
from sdk.datasource.settings import DATA_PROXY_URL, DATAPLATFORM_TOKEN, CURRENT_USER
from sdk.utils import BigLogger

log = BigLogger("data_proxy")

headers = {"Authorization": DATAPLATFORM_TOKEN}


def compress_df(df):
    """压缩df"""
    df = pickle.dumps(df)
    df_bytes = zlib.compress(df, level=4)
    df = base64.b64encode(df_bytes).decode("utf-8")
    return df


def decompress_df(df):
    """解压df"""
    if isinstance(df, str):
        df = base64.b64decode(df.encode("utf-8"))
    decom_bytes = zlib.decompress(df)
    df = pickle.loads(decom_bytes)
    return df


def read_proxy(username, table, start_date=None, end_date=None, instruments=None, fields=None, query=None):
    url = f"{DATA_PROXY_URL}/api/datasource/read"
    data = {
        "username": username,
        "table": table,
        "start_date": start_date,
        "end_date": end_date,
        "instruments": instruments,
        "fields": fields,
        "query": query,
    }
    log.debug(f"read_proxy {url} {data} ")
    response = requests.post(url, json=data, headers=headers, verify=False)

    if response.status_code != 200:
        log.warning(f"读取 DataSource('{table}').read(start_date='{start_date}', end_date='{end_date}') 数据失败：数据不存在")
        return None

    decom_bytes = zlib.decompress(response.content)
    df = pickle.loads(decom_bytes)
    return df


def update_proxy(data):
    """
    更新远程数据

    Args:
        source_id: str
        alias: str
        schema: dict
        **kwargs
            write_mode: str
            public: bool

    """
    remote_update_url = f"{DATA_PROXY_URL}/api/datasource/update"
    log.info("start update_proxy {} ... ".format(remote_update_url))
    response = requests.post(remote_update_url, json=data, verify=False, timeout=1200, headers=headers)
    if response.status_code == 200:
        log.info("update_proxy success!")
    else:
        log.error("-- request update_proxy failed ! {}".format(response.text))


def write_df_proxy(df, alias, owner=CURRENT_USER):
    fields  = {}
    for col in df.columns:
        fields[col] = {'desc': '', 'type': str(df[col].dtype)}

    schema = {
        'active': False,
        'date_field': None,
        'fields': fields,
        'public': True,
        'partition_date': None,
        'primary_key': list(df.columns)
    }

    data = {
        "alias": alias,
        "schema": schema,
        "df": compress_df(df),
        "owner": owner,
        "options": "update",
    }
    # 远程同步更新
    update_proxy(data)

