import hashlib
import json
import os
from datetime import datetime


def make_data_alias(factor_column: str):
    time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    _encode_str = f"{factor_column}_{time_str}"
    _hash_code = hashlib.md5(_encode_str.encode("utf-8")).hexdigest()
    return f"alpha_{_hash_code[:8]}"


def check_factor_data(data: dict):
    """ 检查因子数据格式 """
    if not isinstance(data, dict):
        raise Exception(f"因子数据{type(data)}类型错误，请更正为<class 'dict'>")
    factor_need_colums = ["options", "metrics", "datasource", "column_name"]
    if "factor_need_colums" in os.environ:
        factor_need_colums = json.loads(os.environ["factor_need_colums"])
    input_data_keys = set(factor_need_colums) - set(data.keys())
    if input_data_keys:
        raise Exception(f"缺失因子信息{input_data_keys}")
    return True
