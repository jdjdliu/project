import json

import pandas as pd
import requests

from sdk.datasource.settings import (CURRENT_USER,
                                     DATAPLATFORM_CACHE_CREATE_URL,
                                     DATAPLATFORM_DATATASK_STATUS_URL,
                                     DATAPLATFORM_HOST,
                                     DATAPLATFORM_METADATA_URL,
                                     DATAPLATFORM_STATISTICS_URL,
                                     DATAPLATFORM_TOKEN,
                                     DATAPLATFORM_TOKEN_NAME,
                                     DATAPLATFORM_USER_TOKEN_NAME,
                                     default_public_tables,
                                     default_sysytem_user, is_paper_trading)
from sdk.utils import BigLogger

# 请求header, 鉴权
headers = {DATAPLATFORM_USER_TOKEN_NAME: CURRENT_USER, DATAPLATFORM_TOKEN_NAME: DATAPLATFORM_TOKEN}
log = BigLogger("dataplatform")


def request_dataplatform(url, data=None, method="GET", timeout=60):

    if not url.startswith(DATAPLATFORM_HOST):
        url = f"{DATAPLATFORM_HOST}{url}"
    response = None
    if method == "GET":
        response = requests.get(url, headers=headers, timeout=timeout, params=data, verify=False)
    elif method == "POST":
        response = requests.post(url, headers=headers, timeout=timeout, json=data, verify=False)
    elif method == "PUT":
        response = requests.put(url, headers=headers, timeout=timeout, params=data, verify=False)
    else:
        raise Exception(f"method {method} unknown!")
    if response.status_code != 200:
        log.debug(f"failed request data {response.text}")
    return response


def get_metadata(table, username=None, get_meta=False, update_stats=False):
    """获取表metadata和权限"""
    # 请求dataplatform接口请求metadata和权限
    username = username or CURRENT_USER
    metadata = {"active": True}
    if not get_meta and table in default_public_tables:
        return metadata

    data = {"username": username, "table": table, "update_stats": update_stats, "is_paper_trading": is_paper_trading}
    try:
        response = request_dataplatform(DATAPLATFORM_METADATA_URL, data=data)
        metadata = response.json()
    except Exception as e:  # noqa
        # log.warning(f"get permission failed {e}")
        metadata["active"] = True

    if not CURRENT_USER or (CURRENT_USER == default_sysytem_user) or (table in default_public_tables):
        metadata["active"] = True

    return metadata


def push_statistics_data(data, category="updatedatasource"):
    """推送统计指标到数据管理平台"""

    data["category"] = category
    response = request_dataplatform(DATAPLATFORM_STATISTICS_URL, data=data, method="POST")
    # if response.status_code != 200:
    #     print(f'push_statistics_data failed : {response.text}')
    return response.json()


def push_metadata(table, metadata, df, update_schema=False):

    data = metadata.get("schema")
    data["table_name"] = table

    category = data.get("category") or ""
    category1, category2 = "", ""
    if "/" in category:
        category1, category2 = category.split("/")
    data["category1"] = category1
    data["category2"] = category2
    data["public"] = metadata.get("public", True)
    data["owner"] = metadata.get("owner", "system")
    data["file_type"] = metadata.get("file_type", "bdb")
    data["update_schema"] = update_schema

    if isinstance(df, pd.DataFrame):
        # 日期格式优化
        for col in df.columns:
            if "datetime" in str(df[col].dtype):
                try:
                    df[col] = df[col].astype(str)
                except:  # noqa
                    pass
        demo_data = df.head().to_json(orient="records")
        data["demo_data"] = json.loads(demo_data)
    log.info(f"update dataplatform metadata: {DATAPLATFORM_METADATA_URL} {data}")
    response = request_dataplatform(DATAPLATFORM_METADATA_URL, data=data, method="POST")
    return response


def data_source_create(id, version="v3", file_type=None, data_base="user", schema_json=None):

    data = {
        "username": CURRENT_USER,
        "datasource_id": id,
        "version": version,
        "file_type": file_type,
        "data_base": data_base,
        "schema_data": schema_json,
        "is_paper_trading": is_paper_trading,
    }
    response = request_dataplatform(DATAPLATFORM_CACHE_CREATE_URL, data=data, method="POST")
    return response.json()
    # if response.status_code != 200:
    #     print(f'data_source_create failed : {response.text}')
    # print('data_source_create: ', response.json())


def data_source_visit(datasource_id, username=None):
    username = username or CURRENT_USER
    meta = get_metadata(datasource_id, username=username)
    return meta


def update_datatask_status(task_id=0, task_name=None, status="success", msg=""):
    """修改数据任务状态"""
    data = {"task_id": task_id, "task_name": task_name, "status": status, "msg": msg, "username": CURRENT_USER}
    response = request_dataplatform(DATAPLATFORM_DATATASK_STATUS_URL, data=data, method="PUT")
    if response and response.status_code == 200:
        print(response.json())
    else:
        print(f"请求失败 {response} !")
