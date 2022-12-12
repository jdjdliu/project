from sdk.utils import BigLogger

import learning.module2.common.interface as I  # noqa
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_list

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
bigquant_friendly_name = "标准化处理"
bigquant_doc_url = "https://bigquant.com/docs/"

log = BigLogger(bigquant_friendly_name)


def standard(x):
    return (x - x.mean()) / x.std()


def bigquant_run(
    input_1: I.port("输入数据", specific_type_name="DataSource"),
    input_2: I.port("因子列表", optional=True, specific_type_name="列表|DataSource") = None,
    columns_input: I.code("指定列", auto_complete_type="feature_fields,bigexpr_functions") = "",
) -> [I.port("标准化数据", "data")]:
    """

    标准化处理，也可称为归一化处理，属于数据处理常见的一种方式.

    """
    # 示例代码如下。在这里编写您的代码
    import numpy as np

    df = input_1.read_df()
    df = df[~df.isin([np.nan, np.inf, -np.inf])]

    columns = []
    if input_2 is None:
        if not columns_input:
            log.error("请输入标准化的列名或连接输入因子列表模块")
        else:
            columns = smart_list(columns_input)
    else:
        columns = input_2.read_pickle()

    for fac in columns:
        # median = df[fac].median()
        # std = df[fac].std()
        df[fac] = df.groupby("date")[fac].apply(standard)

    ds = DataSource().write_df(df)
    return Outputs(data=ds)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):
    return outputs
