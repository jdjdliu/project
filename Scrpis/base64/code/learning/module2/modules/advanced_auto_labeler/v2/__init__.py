from sdk.datasource import DataReaderV2
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from sdk.utils.func import extend_class_methods
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

D2 = DataReaderV2()
bigquant_cacheable = True


DEFAULT_LABEL_EXPR = """# #号开始的表示注释
# 0. 每行一个，顺序执行，从第二个开始，可以使用label字段
# 1. 可用数据字段见 https://bigquant.com/docs/develop/datasource/deprecated/history_data.html
#   添加benchmark_前缀，可使用对应的benchmark数据
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
bigquant_friendly_name = "自动标注(股票)"
bigquant_doc_url = "https://bigquant.com/docs/develop/modules/advanced_auto_labeler.html"
# log = logbook.Logger(bigquant_friendly_name)
log = BigLogger(bigquant_friendly_name)


class BigQuantModule:
    def __init__(
        self,
        instruments: I.port("代码列表", specific_type_name="列表|DataSource"),
        label_expr: I.code(
            "标注表达式，可以使用多个表达式，顺序执行，从第二个开始，可以使用label字段。[可用数据字段](https://bigquant.com/docs/develop/datasource/deprecated/history_data.html)，添加benchmark_前缀，可使用对应的benchmark数据。可用操作符和函数见[表达式引擎](https://bigquant.com/docs/develop/bigexpr/usage.html)",
            default=DEFAULT_LABEL_EXPR,
            specific_type_name="列表",
            auto_complete_type="history_data_fields,bigexpr_functions",
        ),
        start_date: I.str("开始日期，示例 2017-02-12，一般不需要指定，使用 代码列表 里的开始日期") = "",
        end_date: I.str("结束日期，示例 2017-02-12，一般不需要指定，使用 代码列表 里的结束日期") = "",
        benchmark: I.str("基准指数，如果给定，可以使用 benchmark_* 变量") = "000300.SHA",
        drop_na_label: I.bool("删除无标注数据，是否删除没有标注的数据") = True,
        cast_label_int: I.bool("将标注转换为整数，一般用于分类学习") = True,
        user_functions: I.code(
            "自定义表达式函数，字典格式，例:{'user_rank':user_rank}，字典的key是方法名称，字符串类型，字典的value是方法的引用，参考文档[表达式引擎](https://bigquant.com/docs/develop/bigexpr/usage.html)",
            I.code_python,
            "{}",
            specific_type_name="字典",
        ) = {},
    ) -> [I.port("标注数据", "data")]:
        """
        可以使用表达式，对数据做任何标注。比如基于未来给定天数的收益/波动率等数据，来实现对数据做自动标注。标注后数据可作为预测对象或者离散化的特征。
        """

        self.__instruments = smart_list(instruments)
        self.__start_date = start_date
        self.__end_date = end_date
        if isinstance(self.__instruments, dict):
            self.__start_date = self.__start_date or self.__instruments["start_date"]
            self.__end_date = self.__end_date or self.__instruments["end_date"]
            self.__instruments = self.__instruments["instruments"]
        self.__label_expr = smart_list(label_expr)
        self.__benchmark = benchmark
        self.__drop_na_label = drop_na_label
        self.__cast_label_int = cast_label_int
        self.__user_functions = user_functions

    def __load_data(self):
        import bigexpr

        BENCHMARK_FEATURE_PREFIX = "benchmark_"

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
        general_features = list(general_features)

        instrument_general_features = [f for f in general_features if not f.startswith(BENCHMARK_FEATURE_PREFIX)]
        if not instrument_general_features:
            raise Exception("在表达式中没有发现需要加载的字段: %s" % self.__label_expr)
        history_data = D2.history_data(
            instruments=self.__instruments,
            start_date=self.__start_date,
            end_date=self.__end_date,
            fields=["date", "instrument", "amount"] + instrument_general_features,
        )

        benchmark_general_features = [f[len(BENCHMARK_FEATURE_PREFIX) :] for f in general_features if f.startswith(BENCHMARK_FEATURE_PREFIX)]
        if benchmark_general_features:
            benchmark_data = D2.history_data(
                instruments=self.__benchmark,
                start_date=self.__start_date,
                end_date=self.__end_date,
                fields=["date", "instrument"] + benchmark_general_features,
            )
            benchmark_data.columns = [c if c in ["date"] else BENCHMARK_FEATURE_PREFIX + "%s" % c for c in benchmark_data.columns]
            history_data = history_data.merge(benchmark_data, on="date", how="left")

        history_data = history_data[history_data.amount > 0]
        log.info("加载历史数据: %s 行" % len(history_data))
        return history_data

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
        df.columns = [col if col in ["date", "instrument", "label"] else "m:" + col for col in df.columns]

        data = DataSource.write_df(df)
        outputs = Outputs(data=data, cast_label_int=self.__cast_label_int)

        return outputs


def bigquant_postrun(
    outputs,
) -> [I.doc("绘制标注数据分布", "plot_label_counts"),]:
    from .postrun import plot_label_counts

    extend_class_methods(outputs, plot_label_counts=plot_label_counts)

    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
