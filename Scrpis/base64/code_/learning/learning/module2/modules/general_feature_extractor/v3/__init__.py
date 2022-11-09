import json
import os
import pandas as pd

import bigexpr
from sdk.datasource import DataReaderV2
from learning.module2.common.data import Outputs, DataSource
from learning.module2.common.utils import read_hdf
from sdk.utils import BigLogger

D2 = DataReaderV2()
# log = logbook.Logger('general_feature_extractor')
log = BigLogger("general_feature_extractor")
bigquant_cacheable = True
bigquant_deprecated = "请更新到 ${MODULE_NAME} 最新版本"


def get_features_from_model(path):
    features = []
    with open(path) as reader:
        for line in reader:
            for tag in ["Name=", "Expression="]:
                if line.startswith(tag):
                    features.append(line[len(tag) :].strip())
                    break
    with open(path + ".mapping.json") as reader:
        feature_map = json.loads(reader.read())
    feature_map = {v: k for k, v in feature_map.items()}
    return {feature_map[feature] for feature in features}


class BigQuantModule:
    def __init__(self, history_data, features=None, model_id=None):
        """
        抽取简单特征数据。使用最新因子库：https://bigquant.com/docs/develop/datasource/deprecated/features.html
        :param history_data: 历史数据定义
        :param features:
        :param model_id:
        """
        self.history_data = history_data
        self.__features = self.__process_features(features, model_id)

    def __process_features(self, features, model_id):
        if model_id is None and features is None:
            raise Exception("either model_id or features is required")

        features = features or []
        if model_id is not None:
            features += self.__load_features_from_model(model_id)
        if not features:
            raise Exception("no features")

        if features is not None:
            features = bigexpr.extract_variables(features)
        return features

    def __load_features_from_model(self, model_id):
        from learning.module2.modules.stock_ranker_train.v1 import full_training_path

        path = full_training_path(model_id, "ranker.ini")
        if not os.path.exists(path):
            raise Exception("model not found")
        if not os.path.exists(path + ".mapping.json"):
            raise Exception("old model not supported")
        return get_features_from_model(path)

    def run(self):
        feature_ds = DataSource()
        output_store = pd.HDFStore(feature_ds.path)

        instruments = set(self.history_data["instruments"])
        start_date = pd.to_datetime(self.history_data["start_date"])
        end_date = pd.to_datetime(self.history_data["end_date"])
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

            year_df = D2.history_data(instruments, start_date=_start_date, end_date=_end_date, fields=self.__features)
            output_store["y_%s" % year] = year_df
            log.info("year %s, featurerows=%s" % (year, len(year_df)))
            total_feature_rows += len(year_df)

        output_store.close()

        if total_feature_rows == 0:
            raise Exception("no features extracted.")
        log.info("total feature rows: %s" % total_feature_rows)

        return Outputs(data=feature_ds)


def bigquant_postrun(outputs):
    outputs.extend_methods(read_data=lambda self, year=None: read_hdf(self.data.path, year))

    return outputs


def bigquant_cache_key(kwargs):
    if "features" in kwargs:
        kwargs["features"] = bigexpr.extract_variables(kwargs["features"])
    return kwargs
