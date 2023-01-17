import datetime
import pandas as pd

import bigexpr
from sdk.datasource import DataReaderV2
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

D2 = DataReaderV2()
bigquant_cacheable = True
bigquant_deprecated = None

# 模块接口定义
bigquant_category = "特征抽取"
bigquant_friendly_name = "基础特征抽取"
bigquant_doc_url = "https://bigquant.com/docs/develop/modules/general_feature_extractor.html"
# log = logbook.Logger(bigquant_friendly_name)
log = BigLogger(bigquant_friendly_name)


class BigQuantModule:
    def __init__(
        self,
        instruments: I.port("代码列表", specific_type_name="列表|DataSource"),
        features: I.port("特征列表", specific_type_name="列表|DataSource"),
        start_date: I.str("开始日期，示例 2017-02-12，一般不需要指定，使用 代码列表 里的开始日期") = "",
        end_date: I.str("结束日期，示例 2017-02-12，一般不需要指定，使用 代码列表 里的结束日期") = "",
        before_start_days: I.float("向前取数据天数，比如，用户通过表达式计算的衍生特征，可能需要用到开始日期之前的数据，可以通过设置此值实现，则数据将从 开始日期-向前取数据天数 开始取。考虑到节假日等，建议将此值得大一些") = 90,
    ) -> [I.port("基础特征数据", "data")]:
        """基础特征(因子)抽取：读取基础数据字段，这里抽取的是基础特征，例如，对于衍生特征 close_1/close_0，这里只会读取出 close_0，close_1。要进行衍生特征抽取，需要结合衍生特征抽取模块。"""
        self.__instruments = smart_list(instruments)
        self.__start_date = start_date
        self.__end_date = end_date
        self.__market = None
        if isinstance(self.__instruments, dict):
            self.__start_date = self.__start_date or self.__instruments["start_date"]
            self.__end_date = self.__end_date or self.__instruments["end_date"]
            self.__market = self.__market or self.__instruments.get("market")
            self.__instruments = self.__instruments["instruments"]

        self.__before_start_days = before_start_days
        self.__features = self._get_features_except_factor_aliases(features)

    @staticmethod
    def _get_features_except_factor_aliases(features):
        expressions = smart_list(features)
        factor_alias_set = set()
        for expr in expressions:
            factor_alias_list, target_expression = bigexpr.parse_assignment(expr)
            if factor_alias_list:
                factor_alias_set.update(factor_alias_list)
        all_features = set(bigexpr.extract_variables(expressions))
        return list(all_features - factor_alias_set)

    def run(self):
        feature_ds = DataSource()
        output_store = feature_ds.open_df_store()

        instruments = set(self.__instruments)
        start_date = pd.to_datetime(self.__start_date)
        if self.__before_start_days:
            start_date -= datetime.timedelta(days=self.__before_start_days)

        end_date = pd.to_datetime(self.__end_date)
        total_feature_rows = 0

        for year in range(start_date.year, end_date.year + 1):
            if year == start_date.year:
                _start_date = start_date
            else:
                _start_date = "%s-01-01" % year
            if year == end_date.year:
                _end_date = end_date
            else:
                _end_date = "%s-12-31" % year

            # year_df = D2.history_data(instruments, start_date=_start_date, end_date=_end_date, fields=self.__features)
            year_df = D2.features(instruments, start_date=_start_date, end_date=_end_date, fields=self.__features, market=self.__market)
            cnt = 0
            if year_df is not None and len(year_df) > 0:
                year_df = year_df[sorted(year_df.columns)]
                output_store["y_%s" % year] = year_df
                cnt = len(year_df)
            log.info("年份 %s, 特征行数=%s" % (year, cnt))
            total_feature_rows += cnt

        feature_ds.close_df_store()

        if total_feature_rows == 0:
            raise Exception("no features extracted.")
        log.info("总行数: %s" % total_feature_rows)

        return Outputs(data=feature_ds)


def bigquant_postrun(outputs):
    return outputs


def bigquant_cache_key(kwargs):
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    if kwargs["end_date"] >= today:
        return None
    if "features" in kwargs:
        kwargs["features"] = bigexpr.extract_variables(smart_list(kwargs["features"]))
    return kwargs
