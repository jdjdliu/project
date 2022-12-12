import json
import os
from sdk.utils import BigLogger

from sdk.datasource import UpdateDataSource

import learning.module2.common.interface as I  # noqa
from learning.module2.common.data import DataSource, Outputs

from .utils import check_factor_data, make_data_alias

bigquant_public = True

bigquant_cacheable = True
bigquant_category = "因子研究"
bigquant_friendly_name = "保存因子"
bigquant_doc_url = "https://bigquant.com/docs/"
log = BigLogger(bigquant_friendly_name)

# factors_info = {
#     "factor_1": {
#         "options": {"start_date": "2021-01-01", "end_date": "2021-07-05"},
#         "metrics": {"IC均值": 0.1, "IC_IR": 0.2, "近一日收益率": 0.01, "近一周收益率": -0.03, "近一月收益率": 0.12},
#         "datasource": "abcd1234",
#         "column_name": "factor_1",
#         "expr": ""
#     }
# }


def bigquant_run(
    factors_info: I.port("因子信息", specific_type_name="DataSource"),
) -> [I.port("数据", "data")]:
    """保存因子数据"""
    factors_info_data = factors_info.read()  # type: dict

    factor_run_mode = os.environ.get("RUN_MODE")

    save_data = []
    for factor_name, factor_data in factors_info_data.items():
        if check_factor_data(factor_data):
            log.info(f"开始保存因子: {factor_name}")
            factor_df = factor_data["datasource"]
            column_name = factor_data["column_name"]
            if column_name not in factor_df.columns:
                raise Exception(f"因子数据中未包含要保存的因子列{column_name}")
            log.info(f"{factor_name}数据格式无误，可以提交任务")
            if factor_run_mode == "factor_init" or factor_run_mode == "factor_daily":
                # 生成或更新因子值
                datasource_dict = os.getenv("DATASOURCE_DICT")
                if datasource_dict:
                    datasource_dict = json.loads(datasource_dict)
                    data_alias = datasource_dict[column_name]
                else:
                    data_alias = make_data_alias(column_name)
                update_ds = UpdateDataSource(current_node=None)
                update_ds.update_pkl(pkl=factor_df, alias=data_alias)
                save_data.append((factor_name, data_alias))

    if factor_run_mode == "factor_init" or factor_run_mode == "factor_daily":
        for factor_name, data_alias in save_data:
            factors_info_data[factor_name]["datasource"] = data_alias
            if "expr" not in factors_info_data[factor_name].keys():
                factors_info_data[factor_name]["expr"] = ""
            log.info(f"因子：{factor_name}处理完成")

        with open("/var/tmp/factor_data.json", "w") as f:
            json.dump(factors_info_data, f)
        log.info("因子保存完成")

    return Outputs(data=DataSource.write_pickle(factors_info_data))


def bigquant_postrun(outputs):
    return outputs
