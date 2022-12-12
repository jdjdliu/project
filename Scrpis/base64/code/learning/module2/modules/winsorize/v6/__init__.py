import numpy as np

from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_list
import learning.module2.common.interface as I  # noqa
from sdk.utils import BigLogger

# 是否自动缓存结果
bigquant_cacheable = True

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = False

bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = "数据处理"
bigquant_friendly_name = "去极值"
bigquant_doc_url = "https://bigquant.com/docs/"

log = BigLogger(bigquant_friendly_name)


def winsorize(df, columns, median_deviate):
    for factor in columns:
        median = df[factor].median()
        MAD = np.std(abs(df[factor] - median))
        # 剔除偏离中位数5倍以上的数据
        df.loc[df[factor] > median + median_deviate * MAD, factor] = median + median_deviate * MAD
        df.loc[df[factor] < median - median_deviate * MAD, factor] = median - median_deviate * MAD
    return df


def bigquant_run(
    input_data: I.port("输入数据", specific_type_name="DataSource"),
    features: I.port("因子列表", optional=True, specific_type_name="列表|DataSource") = None,
    columns_input: I.code("指定列", auto_complete_type="feature_fields,bigexpr_functions") = "",
    median_deviate: I.int("指定标准差倍数", max=100) = 3,
) -> [I.port("去极值数据", "data")]:
    """

    去极值，属于常见的数据处理模块，将剔除偏离中位数5倍以上的数据.

    """
    data = input_data.read_df()
    columns = []
    if features is None:
        if not columns_input:
            log.error("请输入去极值的列名或连接输入因子列表模块")
        else:
            columns = smart_list(columns_input)
    else:
        columns = features.read_pickle()

    # 截面数据去极值
    df = data.groupby("date").apply(winsorize, columns, median_deviate)
    ds = DataSource().write_df(df)
    return Outputs(data=ds)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):
    return outputs
