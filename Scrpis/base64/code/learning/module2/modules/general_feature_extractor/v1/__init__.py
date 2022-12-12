import os
import sys
from datetime import datetime

import bigexpr
import numpy as np
import pandas as pd

from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import get_feature_extraction_dir, read_hdf, read_history_data_source
from learning.module2.feature_rating_join.v1 import BigQuantModule as feature_rating_join_module

bigquant_cacheable = True
bigquant_deprecated = "请更新到 ${MODULE_NAME} 最新版本"


class BigQuantModule:
    def __init__(self, history_data, user_feature_extractor=None, rating_ds=None, features=None):
        """
        抽取简单特征数据
        :param history_data:
        :param user_feature_extractor:
        :param rating_ds:
        :param features:
        """
        self.history_data = history_data
        self.user_feature_extractor = user_feature_extractor
        self.feature_extraction_dir = get_feature_extraction_dir()
        self.rating_ds = rating_ds
        self.features = features
        if self.features is not None:
            self.features = set(bigexpr.extract_variables(self.features) + ["date", "instrument", "rating"])

    def _extract_user_features(self):
        if self.user_feature_extractor is None:
            return None

        raise Exception("TODO: not supported yet")
        feature_list = []
        print("%s, _extract_user_features" % (datetime.now()))
        instrument_count = 0
        feature_row_count = 0
        for instrument, history_df in read_history_data_source(self.history_data):
            sys.stdout.write(".")

            user_features = self.user_feature_extractor(history_df)
            if user_features is not None:
                user_features.set_index(["date"], inplace=True)
                user_features = user_features.astype(np.int32)
                user_features.reset_index(inplace=True)
                user_features["instrument"] = [instrument] * len(user_features["date"])
                feature_list.append(user_features)

            instrument_count += 1
            feature_row_count += len(user_features)
        user_features = pd.concat(feature_list)
        print("%s, _extract_user_features: %s feature rows for %s instruments" % (datetime.now(), feature_row_count, instrument_count))
        return user_features

    def run(self):
        user_features = self._extract_user_features()
        ratings = feature_rating_join_module.read_ratings(self.rating_ds)

        market_type = ""
        if self.history_data["instruments"] and self.history_data["instruments"][0].split(".")[-1] in [
            "DCE",
            "SHF",
            "CZC",
            "CFE",
            "CFX",
            "INE",
            "ZCE",
        ]:
            market_type = "future."
        feature_store = pd.HDFStore(os.path.join(self.feature_extraction_dir, market_type + "features.h5"), mode="r")
        feature_ds = DataSource()
        output_store = pd.HDFStore(feature_ds.path)

        instruments = set(self.history_data["instruments"])
        start_date = pd.to_datetime(self.history_data["start_date"])
        end_date = pd.to_datetime(self.history_data["end_date"]) + pd.to_timedelta("1d")
        total_feature_rows = 0
        tables = feature_store.keys()
        for year in range(start_date.year, end_date.year + 1):
            start_time = datetime.now()
            table = "/y_%s" % year
            if table not in tables:
                continue
            year_df = pd.read_hdf(feature_store, table)

            if self.features:
                columns = [col for col in year_df.columns if col.startswith("m:") or col in self.features]
                year_df = year_df[columns]

            year_df = year_df[(year_df.instrument.isin(instruments)) & (year_df.date >= start_date) & (year_df.date < end_date)]
            if user_features is not None:
                year_df = year_df.merge(user_features, on=["date", "instrument"], how="inner")
            if ratings is not None:
                year_df = feature_rating_join_module.merge_ratings(year_df, ratings)
            year_df.to_hdf(output_store, "y_%s" % year)
            print("year %s, featurerows=%s, timetaken=%ss" % (year, len(year_df), (datetime.now() - start_time).total_seconds()))
            total_feature_rows += len(year_df)

        feature_store.close()
        output_store.close()

        if total_feature_rows == 0:
            raise Exception("no features extracted.")
        print("total feature row: %s" % total_feature_rows)

        return Outputs(data=feature_ds)


def bigquant_postrun(outputs):
    # add an alias first
    outputs.extend_methods(read_data=lambda self, year=None: read_hdf(self.data.path, year))

    return outputs


def bigquant_cache_key(kwargs):
    kwargs["features"] = bigexpr.extract_variables(kwargs["features"])
    return kwargs
