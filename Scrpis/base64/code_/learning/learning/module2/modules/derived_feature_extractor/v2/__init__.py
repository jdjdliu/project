import pandas as pd
import time

import bigexpr
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

# log = logbook.Logger('derived_feature_extractor')
log = BigLogger("derived_feature_extractor")
bigquant_cacheable = True
bigquant_deprecated = None

# 模块接口定义
bigquant_category = "特征抽取"
bigquant_friendly_name = "衍生特征抽取"
bigquant_doc_url = "https://bigquant.com/docs/develop/modules/derived_feature_extractor.html"


class BigQuantModule:
    def __init__(
        self,
        input_data: I.port("特征数据，包含用于构建衍生因子的基础因子数据，一般来自基础特征抽取或者衍生特征抽取模块", specific_type_name="DataSource"),
        features: I.port(
            "特征列表，需要抽取的衍生特征，由表达式构建。可用数据字段来自输入的data，可用操作符和函数见 `表达式引擎 <https://bigquant.com/docs/develop/bigexpr/usage.html>`_",
            specific_type_name="列表|DataSource",
        ),
        date_col: I.str("日期列名，如果在表达式中用到切面相关函数时，比如 rank，会用到此列名") = "date",
        instrument_col: I.str("证券代码列名，如果在表达式中用到时间序列相关函数时，比如 shift，会用到此列名") = "instrument",
        user_functions: I.code(
            "自定义表达式函数，字典格式，例:{'user_rank':user_rank}，字典的key是方法名称，字符串类型，字典的value是方法的引用，参考 文档-表达式引擎 https://bigquant.com/docs/develop/bigexpr/usage.html",
            I.code_python,
            "{}",
            specific_type_name="字典",
            auto_complete_type="feature_fields,bigexpr_functions",
        ) = {},
    ) -> [I.port("特征数据", "data")]:
        """
        衍生特征(因子)抽取：对于衍生特征（通过表达式定义的，e.g. close_1/close_0），通过表达式引擎，计算表其值
        """
        self.__data = input_data
        self.__features = smart_list(features)
        self.__user_functions = user_functions
        self.__date_col = date_col
        self.__instrument_col = instrument_col

    def run(self):
        input_store = self.__data.open_df_store()
        # 1. load input columns
        tables = input_store.keys()
        input_df_0 = input_store[tables[0]]

        # 2. calculate features to extract
        input_columns = set(input_df_0.columns)
        to_extract_features = []
        dependencies = set()
        for col in [self.__date_col, self.__instrument_col]:
            if col in input_columns:
                dependencies.add(col)
        for f in self.__features:
            if f in input_columns:
                continue
            deps = bigexpr.extract_variables(f)
            for dep in deps:
                if dep not in input_columns:
                    raise Exception("failed to extract %s: %s not found" % (f, dep))
                dependencies.add(dep)
            to_extract_features.append(f)

        # 3. load dependencies
        dependencies = list(dependencies)
        input_df_list = [input_df_0[dependencies]]
        del input_df_0
        for table in tables[1:]:
            _df = input_store[table]
            input_df_list.append(_df[dependencies])
            del _df
        self.__data.close_df_store()
        del input_store

        input_df = pd.concat(input_df_list, ignore_index=True)
        del input_df_list

        # 4. extract derived features
        derived_df = input_df
        for feature in to_extract_features:
            start_time = time.time()
            try:
                derived_df[feature] = bigexpr.evaluate(derived_df, feature, self.__user_functions)
                log.info("提取完成 %s, %.3fs" % (feature, time.time() - start_time))
            except Exception as e:
                log.info("提取失败 %s: %s" % (feature, e))
        # + ['date', 'instrument']
        derived_df = derived_df[to_extract_features]

        # 5. merge and save
        input_store = self.__data.open_df_store()
        feature_ds = DataSource()
        output_store = feature_ds.open_df_store()

        start_index = 0
        for table in tables:
            input_df = input_store[table]
            log.info("%s, %s" % (table, len(input_df)))

            end_index = start_index + len(input_df)
            input_df.index = range(start_index, end_index)
            df = pd.concat([input_df, derived_df[start_index:end_index]], axis=1)
            start_index = end_index

            output_store[table] = df
            del df
            del input_df

        feature_ds.close_df_store()
        self.__data.close_df_store()

        return Outputs(data=feature_ds)


def bigquant_postrun(outputs):
    return outputs
