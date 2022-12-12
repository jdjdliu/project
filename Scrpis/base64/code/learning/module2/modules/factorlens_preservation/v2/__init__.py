import datetime
import json
import os
import uuid

import pandas as pd
from sdk.datasource import UpdateDataSource, get_metadata
from sdk.utils import BigLogger

import learning.module2.common.interface as I  # noqa
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import display_df, smart_dict

bigquant_public = True

bigquant_cacheable = False
bigquant_category = "因子研究"
bigquant_friendly_name = "保存因子"
bigquant_doc_url = "https://bigquant.com/docs/"
log = BigLogger(bigquant_friendly_name)

factor_run_mode = os.environ.get("RUN_MODE")
update_alpha_datasource = False
if factor_run_mode in ["factor_init", "factor_daily"]:
    update_alpha_datasource = True

DEFAULT_FACTOR_FIELDS = """# 定义因子名称
# {
#     "列名": {'name': "因子名", 'desc': "因子描述"},
#     "列名": {'name': "因子名", 'desc': "因子描述"},
#     ... 
# }
{}
"""


def check_factor_data(data: dict):
    """检查因子数据格式"""
    if not isinstance(data, dict):
        raise Exception(f"因子数据{type(data)}类型错误，请更正为<class 'dict'>")
    factor_need_colums = ["options", "metrics", "datasource", "column_name"]
    if "factor_need_colums" in os.environ:
        factor_need_colums = json.loads(os.environ["factor_need_colums"])
    input_data_keys = set(factor_need_colums) - set(data.keys())
    if input_data_keys:
        raise Exception(f"缺失因子信息{input_data_keys}")
    return True


def bigquant_run(
    factors_info: I.port("因子数据", specific_type_name="DataSource"),
    factor_fields: I.code(
        "因子描述, 每个因子及其对应的名称,因子描述信息", I.code_python, DEFAULT_FACTOR_FIELDS, specific_type_name="函数", auto_complete_type="python"
    ) = None,
    table: I.str("表名, 因子表名, 小写字母, 不能包含大写字母") = "",
) -> [I.port("数据", "data")]:
    """
    保存因子数据

    传入因子绩效数据:
        DataSource -> Dict
        factors_info = {
            "factor_1": {
                "options": {"start_date": "2021-01-01", "end_date": "2021-07-05"},
                "metrics": {"IC均值": 0.1, "IC_IR": 0.2, "近一日收益率": 0.01, "近一周收益率": -0.03, "近一月收益率": 0.12},
                "datasource": DataFrame,
                "column_name": "factor_1",
                "expr": ""
            }
        }
    传入因子数据:
        DataSource -> DataFrame

    """

    username = os.getenv("JPY_USER", "")
    data = factors_info.read()
    factor_fields = smart_dict(factor_fields)
    factors_info_data = {}
    parent_tables = []
    if table:
        metadata = get_metadata(table, username, get_meta=True)
        if metadata and metadata.get("owner") != username:
            msg = "table {} already exists! please change another one".format(table)
            log.error(msg)
            raise Exception(msg)
        if not table.islower():
            msg = "表名请使用小写字母! {}".format(table)
            log.error(msg)
            raise Exception(msg)
        parent_tables.append(table)

    if isinstance(data, dict):
        factors_info_data = {}
        for factor_name, factor_data in data.items():
            factors_info_data[factor_name] = {
                "options": factor_data["options"],
                "metrics": factor_data["metrics"],
                "datasource": factor_data["datasource"],
                "column_name": factor_name,
                "description": factor_data.get("description", ""),
                "expr": factor_data.get("expr", ""),
            }
    elif isinstance(data, pd.DataFrame):
        # 生成规定格式数据
        try:
            # 数据最新日期
            latest_date = data.date.max().strftime("%Y-%m-%d")
        except:  # noqa
            latest_date = datetime.datetime.now().strftime("%Y-%m-%d")
        primary_key = ["instrument", "date"]
        rename_columns = {}
        factor_name_set = set()
        for col in data.columns:
            if col in primary_key:
                continue
            name_desc = factor_fields.get(col)
            if isinstance(name_desc, dict):
                factor_name = name_desc.get("name", col)
                factor_desc = name_desc.get("desc", "")
            else:
                factor_name = col
                factor_desc = ""
            _df = data[primary_key + [col]]
            _df.rename(columns={col: factor_name}, inplace=True)
            if factor_name in factor_name_set:
                raise Exception("重复的因子名，请检查【因子描述】中的 name 字段是否重复")
            rename_columns[col] = factor_name
            factor_name_set.add(factor_name)
            factors_info_data[factor_name] = {
                "options": {"最新数据日期": latest_date, "表名": table},
                "metrics": {},
                "datasource": _df,
                "column_name": factor_name,
                "description": factor_desc,
                "expr": col,
            }
        df = data.rename(columns=rename_columns)
        display_df(df, bigquant_friendly_name)
    else:
        raise Exception("UnKnow data type!")

    save_data = []
    lost_factors = []
    log.info("开始检查因子数据 ...")
    for factor_name, factor_data in factors_info_data.items():
        if check_factor_data(factor_data):
            factor_df = factor_data["datasource"]
            column_name = factor_data["column_name"]
            factor_columns = factor_df.columns
            if column_name not in factor_columns:
                lost_factors.append(factor_name)
            if isinstance(factor_df, pd.DataFrame) and ("date" not in factor_columns or "instrument" not in factor_columns):
                raise Exception("factor data df must has columns [date, instrument]")
            if update_alpha_datasource:
                # 生成或更新因子值
                datasource_dict = os.getenv("DATASOURCE_DICT")
                if datasource_dict:
                    datasource_dict = json.loads(datasource_dict)
                    data_alias = datasource_dict[column_name]
                else:
                    data_alias = "alpha_{}".format(uuid.uuid4().hex[:8])
                update_ds = UpdateDataSource(owner=os.getenv("JPY_USER"), parent_tables=parent_tables)
                _schema = {
                    "active": False,
                    "friendly_name": "因子数据",
                    "desc": "因子数据: {}".format(column_name),
                    "fields": {
                        "date": {"desc": "日期", "type": "datetime64[ns]"},
                        "instrument": {"desc": "证券代码", "type": "str"},
                        column_name: {"desc": "因子", "type": str(factor_df[column_name].dtype)},
                    },
                    "file_type": "arrow",
                    "partition_date": "Y",
                    "date_field": "date",
                    "primary_key": ["date", "instrument"],
                }
                log.info("开始更新因子数据: {} {}".format(data_alias, parent_tables))
                update_ds.update(df=factor_df, alias=data_alias, schema=_schema)
                save_data.append((factor_name, data_alias))
    if lost_factors:
        raise Exception(f"因子数据中缺少需要保存的因子数据 {lost_factors}")

    if update_alpha_datasource:
        for factor_name, data_alias in save_data:
            factors_info_data[factor_name]["datasource"] = data_alias
            if "expr" not in factors_info_data[factor_name].keys():
                factors_info_data[factor_name]["expr"] = ""

        with open("/var/tmp/factor_data.json", "w", encoding="utf-8") as f:
            json.dump(factors_info_data, f, ensure_ascii=False)
        log.info("因子保存完成")
    else:
        log.info(f"检查因子通过, 请提交任务进行因子保存!")

    return Outputs(data=DataSource.write_pickle(factors_info_data), data_2=data)


def bigquant_postrun(outputs):
    return outputs
