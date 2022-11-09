import numpy as np

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
bigquant_friendly_name = "股票池初选"
bigquant_doc_url = "https://bigquant.com/docs/"

log = BigLogger(bigquant_friendly_name)


def bigquant_run(
    input_1: I.port("输入数据", specific_type_name="DataSource"),
    self_instruments: I.code("自定义股票列表", I.code_python, default="""[]""", specific_type_name="列表") = [],
    input_concepts: I.code("指定概念板块", I.code_python, default="""[]""", specific_type_name="列表") = [],
    input_industrys: I.code("指定行业", I.code_python, default="""[]""", specific_type_name="列表") = [],
    input_indexs: I.code("指数范围", I.code_python, default="""['全A股']""", specific_type_name="列表") = ["全A股"],
    input_st: I.choice("过滤ST股", values=["过滤", "不过滤", "只包含"]) = "过滤",
    input_suspend: I.choice("过滤停牌股", values=["过滤", "不过滤"]) = "过滤",
) -> [I.port("输出数据", "data")]:
    """根据行业、指数、ST、停牌等过滤条件获取初步股票池。"""
    df = input_1.read_df()

    # 概念过滤函数
    def is_in_input_concepts(s):
        if len(input_concepts) == 0:
            return 1
        for item in s.split(";"):
            if item in input_concepts:
                return 1
        return 0

    # 行业过滤函数
    def is_in_input_industrys(s):
        if len(input_industrys) == 0:
            return 1
        if s in input_industrys:
            return 1
        return 0

    # st过滤函数
    def is_in_input_st(s):
        if input_st == "不过滤":
            return 1
        elif input_st == "只包含":
            return np.where(s != 0, 1, 0)
        elif input_st == "过滤":
            return np.where(s == 0, 1, 0)

    # 停牌过滤函数
    def is_in_input_suspend(s):
        if input_suspend == "不过滤":
            return 1
        elif input_suspend == "过滤":
            return np.where(s < 1, 1, 0)

    # 概念归属标签,1或0
    df["concept_matched"] = df["concept"].apply(is_in_input_concepts)
    del df["concept"]
    # 行业所属标签,1或0
    df["industry_matched"] = df["industry_sw_level1_0"].apply(is_in_input_industrys)
    del df["industry_sw_level1_0"]
    # 指数所属标签,1或0
    labels = {"沪深300": "in_csi300_0", "中证500": "in_csi500_0", "上证50": "in_sse50_0"}
    for k, v in labels.items():
        df[k] = df[v]
    df["上证A股"] = df["instrument"].apply(lambda x: 1 if x[0] == "6" else 0)
    df["深证A股"] = df["instrument"].apply(lambda x: 1 if x[0] != "6" else 0)
    df["全A股"] = 1
    df["中小板"] = df["instrument"].apply(lambda x: 1 if x[0] == "0" else 0)
    df["创业板"] = df["instrument"].apply(lambda x: 1 if x[0] == "3" else 0)
    df["自定义"] = df["instrument"].apply(lambda x: 1 if x in self_instruments else 0)
    df["index_matched"] = np.where(df[input_indexs].apply(lambda x: x.sum(), axis=1) > 0, 1, 0)  # 只要在任何一个股票池里面就保留
    df.drop(["沪深300", "中证500", "上证50", "上证A股", "深证A股", "全A股", "中小板", "创业板", "自定义", "in_csi300_0", "in_csi500_0", "in_sse50_0"], axis=1, inplace=True)
    # st标签
    df["st_matched"] = df["st_status_0"].apply(is_in_input_st)
    del df["st_status_0"]
    # 停牌标签
    df["suspend_matched"] = df["suspended"].apply(is_in_input_suspend)
    del df["suspended"]
    # 综合过滤标签,1或0
    df["filter"] = np.where(
        df[["concept_matched", "industry_matched", "index_matched", "st_matched", "suspend_matched"]].apply(lambda x: x.sum(), axis=1) == 5, 1, 0
    )
    df_filter = df[df["filter"] > 0]
    # 输出数据
    ds = DataSource.write_df(df_filter)
    return Outputs(data=ds)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):
    return outputs
