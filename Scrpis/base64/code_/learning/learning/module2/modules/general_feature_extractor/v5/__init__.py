import json
import os
import pandas as pd

import bigexpr
from sdk.datasource import DataReaderV2
from learning.module2.common.data import DataSource, Outputs
from sdk.utils import BigLogger

D2 = DataReaderV2()
# log = logbook.Logger('general_feature_extractor')
log = BigLogger("general_feature_extractor")
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
    def __init__(self, instruments, start_date, end_date, features=None, model_id=None):
        """
        因子/特征抽取：抽取数据用于分析和机器学习

        :param 字符串数组 instruments: 股票代码列表，见 :doc:`data_instruments`
        :param 字符串 start_date: 开始日期，e.g. '2017-01-01'
        :param 字符串 end_date: 结束日期，e.g. '2017-02-01'
        :param 字符串数组 features: 需要抽取的特征，可以是基础特征 (例如 close_0)，也可以是衍生特征 (例如 close_0 / close_1)
        :param 字符串 model_id: 模型id，如果给定，模型中使用的特征也将抽取
        :return: 特征数据

            - .data: DataSource, 特征数据，包含基础特征和构成衍生特征的基础特征
        :rtype: Outputs

        帮助文档：`M.general_feature_extractor <https://bigquant.com/docs/develop/modules/general_feature_extractor.html>`_
        """
        self.__instruments = instruments
        self.__start_date = start_date
        self.__end_date = end_date
        self.__features = self.__process_features(features, model_id)

    def __process_features(self, features, model_id):
        if model_id is None and features is None:
            raise Exception("either model_id or features is required")

        features = features or []
        if model_id is not None:
            features += self.__load_features_from_model(model_id)
        if not features:
            raise Exception("no features")

        general_features = []
        if features is not None:
            for feature in features:
                general_features += bigexpr.extract_variables(feature)
        general_features = sorted(set(general_features))
        return general_features

    def __load_features_from_model(self, model_id):
        try:
            return get_features_from_model(model_id)
        except Exception as e:
            raise e

    def run(self):
        feature_ds = DataSource()
        output_store = feature_ds.open_df_store()

        instruments = set(self.__instruments)
        start_date = pd.to_datetime(self.__start_date)
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
            year_df = D2.features(instruments, start_date=_start_date, end_date=_end_date, fields=self.__features)
            cnt = 0
            if year_df is not None and len(year_df) > 0:
                year_df = year_df[sorted(year_df.columns)]
                output_store["y_%s" % year] = year_df
                cnt = len(year_df)
            log.info("year %s, featurerows=%s" % (year, cnt))
            total_feature_rows += cnt

        feature_ds.close_df_store()

        if total_feature_rows == 0:
            raise Exception("no features extracted.")
        log.info("total feature rows: %s" % total_feature_rows)

        return Outputs(data=feature_ds)


def bigquant_postrun(outputs):
    return outputs


def bigquant_cache_key(kwargs):
    if "features" in kwargs:
        general_features = []
        for feature in kwargs["features"]:
            general_features += bigexpr.extract_variables(feature)
        general_features = sorted(set(general_features))
        kwargs["features"] = general_features
    return kwargs
