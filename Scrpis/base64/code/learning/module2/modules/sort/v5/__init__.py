from learning.module2.common.data import DataSource, Outputs
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
bigquant_friendly_name = "排序"
bigquant_doc_url = "https://bigquant.com/docs/"

log = BigLogger(bigquant_friendly_name)


def bigquant_run(
    input_ds: I.port("输入数据", specific_type_name="DataSource"),
    sort_by_ds: I.port("排序特征", optional=True, specific_type_name="DataSource") = None,
    sort_by: I.str("根据哪一列排序") = "--",
    group_by: I.str("根据哪些列group，用逗号分隔") = "date",
    keep_columns: I.str("保留哪些列") = "--",
    ascending: I.bool("升序") = True,
) -> [I.port("排序后数据", "sorted_data")]:
    """

    排序.

    """
    df = input_ds.read_df()
    group_by = group_by and group_by.strip()
    if sort_by_ds is not None:
        sort_by = sort_by_ds.read_pickle()
    else:
        sort_by = sort_by.strip().split(",")
    if not group_by or group_by == "--":
        df = df.sort_values(sort_by, ascending=ascending).reset_index(drop=True)
    else:
        group_by = group_by.split(",")
        df = df.groupby(group_by).apply(lambda x: x.sort_values(sort_by, ascending=ascending)).reset_index(drop=True)
    if keep_columns and keep_columns != "--":
        keep_columns = keep_columns.strip()
        keep_columns = keep_columns.split(",")
        df = df[keep_columns]
    data = DataSource.write_df(df)
    return Outputs(sorted_data=data)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):
    return outputs
