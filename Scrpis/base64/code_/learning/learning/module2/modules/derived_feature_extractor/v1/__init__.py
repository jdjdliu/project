import json
import os
import pandas as pd
import time

import bigexpr
from learning.module2.common.data import DataSource, Outputs
from sdk.utils import BigLogger

# log = logbook.Logger('derived_feature_extractor')
log = BigLogger("derived_feature_extractor")
bigquant_cacheable = True
bigquant_deprecated = None


def get_features_from_model(model_id):
    from learning.module2.modules.stock_ranker_train.v1 import full_training_path

    ranker_path = full_training_path(model_id, "ranker.ini")
    if not os.path.exists(ranker_path):
        raise Exception("model not found")

    feature_mapping_path = full_training_path(model_id, "features.ini.mapping.json")
    if not os.path.exists(feature_mapping_path):
        raise Exception("old model not supported")

    features = []
    with open(ranker_path) as reader:
        for line in reader:
            for tag in ["Name=", "Expression="]:
                if line.startswith(tag):
                    features.append(line[len(tag) :].strip())
                    break
    with open(feature_mapping_path) as reader:
        feature_map = json.loads(reader.read())
    # feature_map = {k:v for k, v in feature_map.items()}
    return {feature_map.get(feature, feature) for feature in features}


class BigQuantModule:
    def __init__(self, data, features=None, model_id=None, user_functions=None):
        """
        衍生因子/特征抽取：抽取衍生因子，用于机器学习训练

        :param DataSource data: 数据文件，包含用于构建衍生因子的基础因子数据
        :param 字符串数组 features: 需要抽取的衍生特征，由表达式构建。可用数据字段来自输入的data，可用操作符和函数见 `表达式引擎 <https://bigquant.com/docs/develop/bigexpr/usage.html>`_
        :param 字符串 model_id: 模型id，如果给定，模型中使用的衍生特征也将抽取
        :param dict user_functions: 用户自定义函数，参考文档 `表达式引擎 <https://bigquant.com/docs/develop/bigexpr/usage.html>`_
        :return: 特征数据

            - .data: DataSource, 特征数据，包含基础特征和构成衍生特征的基础特征
        :rtype: Outputs

        帮助文档：`M.derived_feature_extractor <https://bigquant.com/docs/develop/modules/derived_feature_extractor.html>`_
        """
        self.__data = data
        self.__features = self.__process_features(features, model_id)
        self.__user_functions = user_functions

    def __process_features(self, features, model_id):
        if model_id is None and features is None:
            raise Exception("either model_id or features is required")

        features = features or []
        if model_id is not None:
            features += self.__load_features_from_model(model_id)
        if not features:
            raise Exception("no features")

        return features

    def __load_features_from_model(self, model_id):
        try:
            return get_features_from_model(model_id)
        except Exception as e:
            raise e

    def run(self):
        input_store = self.__data.open_df_store()
        # 1. load input columns
        tables = input_store.keys()
        input_df_0 = input_store[tables[0]]

        # 2. calculate features to extract
        input_columns = set(input_df_0.columns)
        to_extract_features = []
        dependencies = {"date", "instrument"}
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
                log.info("extracted %s, %.3fs" % (feature, time.time() - start_time))
            except Exception as e:
                log.info("failed to extract %s: %s" % (feature, e))
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
