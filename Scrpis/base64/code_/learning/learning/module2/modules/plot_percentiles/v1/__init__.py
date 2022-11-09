from sdk.utils import BigLogger
from learning.module2.common.data import Outputs, DataSource
import learning.module2.common.interface as I

bigquant_cacheable = True
# 模块接口定义
bigquant_category = "数据可视化"
bigquant_friendly_name = "分位数分布"
bigquant_doc_url = "https://bigquant.com/docs/"
log = BigLogger(bigquant_friendly_name)


def bigquant_run(
    input_data: I.port("输入数据源", specific_type_name="DataSource"),
    include_columns: I.str("选中列，多个列名用英文逗号分隔，默认所有列") = "",
    percentile_count: I.int("分位数数量", min=1) = 100,
    percentile_start: I.float("起始分位", 0.0, 1.0) = 0.0,
    percentile_end: I.float("结束起始分位", 0.0, 1.0) = 1.0,
) -> [I.port("数据", "data")]:
    """
    技术和绘制数据分位数分布。
    """
    from math import ceil
    import pandas as pd

    import learning.api.tools as T
    from learning.module2.common.utils import smart_list

    df = input_data.read()
    df = df.select_dtypes(include="number")

    include_columns = smart_list(include_columns, sep=",")
    if not include_columns:
        include_columns = set(df.columns)
    else:
        include_columns = set(include_columns) & set(df.columns)

    # percentiles
    delta = 1.0 / percentile_count
    percentiles = [(i + 1) * delta for i in range(0, percentile_count)]
    percentiles = [p for p in percentiles if percentile_start <= p <= percentile_end]
    percentile_locs = [ceil(p * len(df)) - 1 for p in percentiles]

    # calculate percentile_df
    percentile_df = pd.DataFrame({"percentile": percentiles})
    for col in include_columns:
        s = df[col].sort_values()
        percentile_df[col] = s.iloc[percentile_locs].values

    T.plot(percentile_df, x="percentile", title=["数据分位数分布"])

    return Outputs(data=DataSource.write_df(percentile_df))


def bigquant_postrun(outputs):
    return outputs
