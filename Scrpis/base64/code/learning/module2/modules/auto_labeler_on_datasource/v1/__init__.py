from sdk.utils import BigLogger

import learning.api.tools as T
import learning.module2.common.interface as I
import numpy as np
import pandas as pd
from sdk.datasource import DataReaderV2
from sdk.utils.func import extend_class_methods

from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_list

D2 = DataReaderV2()
bigquant_cacheable = True


DEFAULT_LABEL_EXPR = """# #号开始的表示注释
# 0. 每行一个，顺序执行，从第二个开始，可以使用label字段
# 1. 可用数据字段见 https://bigquant.com/docs/develop/datasource/deprecated/history_data.html
# 2. 可用操作符和函数见 `表达式引擎 <https://bigquant.com/docs/develop/bigexpr/usage.html>`_

# 计算收益：5日收盘价(作为卖出价格)除以明日开盘价(作为买入价格)
shift(close, -5) / shift(open, -1)

# 极值处理：用1%和99%分位的值做clip
clip(label, all_quantile(label, 0.01), all_quantile(label, 0.99))

# 将分数映射到分类，这里使用20个分类
all_wbins(label, 20)

# 过滤掉一字涨停的情况 (设置label为NaN，在后续处理和训练中会忽略NaN的label)
where(shift(high, -1) == shift(low, -1), NaN, label)
"""


# 模块接口定义
bigquant_category = "数据标注"
bigquant_friendly_name = "自动标注(任意数据源)"
bigquant_doc_url = "https://bigquant.com/"
# log = logbook.Logger(bigquant_friendly_name)
log = BigLogger(bigquant_friendly_name)
bigquant_public = True


class BigQuantModule:
    def __init__(
        self,
        input_data: I.port("用来做标注的数据，一般是行情数据", specific_type_name="DataSource"),
        label_expr: I.code(
            "标注表达式，可以使用多个表达式，顺序执行，从第二个开始，可以使用label字段。[可用数据字段](https://bigquant.com/docs/develop/datasource/deprecated/history_data.html)。可用操作符和函数见[表达式引擎](https://bigquant.com/docs/develop/bigexpr/usage.html)",
            default=DEFAULT_LABEL_EXPR,
            specific_type_name="列表",
            auto_complete_type="history_data_fields,bigexpr_functions",
        ),
        drop_na_label: I.bool("删除无标注数据，是否删除没有标注的数据") = True,
        cast_label_int: I.bool("将标注转换为整数，一般用于分类学习") = True,
        date_col: I.str("日期列名，标明日期列，如果在表达式中用到切面相关函数时，比如 rank，会用到此列名") = "date",
        instrument_col: I.str("证券代码列名，标明证券代码列，如果在表达式中用到时间序列相关函数时，比如 shift，会用到此列名") = "instrument",
        user_functions: I.code(
            "自定义表达式函数，字典格式，例:{'user_rank':user_rank}，字典的key是方法名称，字符串类型，字典的value是方法的引用，参考文档[表达式引擎](https://bigquant.com/docs/develop/bigexpr/usage.html)",
            I.code_python,
            "{}",
            specific_type_name="字典",
        ) = {},
    ) -> [I.port("标注数据", "data")]:
        """
        可以使用表达式，对数据做任何标注。比如基于未来给定天数的收益/波动率等数据，来实现对数据做自动标注。标注后数据可作为预测对象或者离散化的特征。这里数据不局限于股票数据，期货、基金、期权、指数等数据均可标注。
        """

        self.__label_expr = smart_list(label_expr)
        self.__input_data = input_data
        self.__drop_na_label = drop_na_label
        self.__cast_label_int = cast_label_int
        self.__user_functions = user_functions
        self.__date_col = date_col
        self.__instrument_col = instrument_col

    def __load_data(self):
        import bigexpr

        general_features = []
        for expr in self.__label_expr:
            general_features += bigexpr.extract_variables(expr)
        if len(general_features) < 1:
            raise Exception("没有标注表达式")
        if "label" in general_features[0]:
            raise Exception("label变量不能使用在第一个表达式上")
        general_features = set(general_features)
        if "label" in general_features:
            general_features.remove("label")

        for col in [self.__date_col, self.__instrument_col]:
            general_features.add(col)
        data_df = self.__input_data.read_df()

        left_features = set(general_features) - set(data_df.columns)
        if len(left_features) > 0:
            raise Exception("字段: %s 没有在输入数据中" % ",".join(left_features))

        general_features = list(general_features)

        # history_data = history_data[history_data.amount > 0]
        return data_df

    def run(self):
        import bigexpr

        df = self.__load_data()
        log.info("开始标注 ..")

        for expr in self.__label_expr:
            df["label"] = bigexpr.evaluate(df, expr, self.__user_functions)

        if self.__drop_na_label:
            df = df.dropna(subset=["label"])

        if self.__cast_label_int:
            df["label"] = df["label"].astype(int)
            if df["label"].min() < 0:
                raise Exception("label必须为非负整数，错误数据 label=%s" % df["label"].min())

        df.reset_index(drop=True, inplace=True)
        df.columns = [col if col in [self.__date_col, self.__instrument_col, "label"] else "m:" + col for col in df.columns]

        data = DataSource.write_df(df)
        outputs = Outputs(data=data, cast_label_int=self.__cast_label_int)

        return outputs


def bigquant_postrun(
    outputs,
) -> [I.doc("绘制标注数据分布", "plot_label_counts"),]:
    def plot_label_counts(self):
        df = self.data.read_df()
        if self.cast_label_int:
            label_counts = sorted(df["label"].value_counts().to_dict().items())
            df = pd.DataFrame(label_counts, columns=["label", "count"]).set_index("label")
            T.plot(df, title="数据标注分布", double_precision=0, chart_type="column")
        else:
            bin_counts = np.histogram(df["label"], bins=20)
            label_counts = pd.DataFrame(data=list(bin_counts)).transpose()
            label_counts.columns = ["count", "label"]
            T.plot(
                label_counts,
                x="label",
                y=["count"],
                chart_type="column",
                title="label",
                options={"series": [{"pointPadding": 0, "groupPadding": 0, "pointPlacement": "between"}]},
            )

    extend_class_methods(outputs, plot_label_counts=plot_label_counts)

    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
