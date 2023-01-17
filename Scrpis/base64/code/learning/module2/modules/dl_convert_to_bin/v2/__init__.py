import numpy as np

import bigexpr
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

bigquant_cacheable = True
bigquant_deprecated = None

# 模块接口定义
bigquant_category = r"深度学习\数据处理"
bigquant_friendly_name = "序列窗口滚动(深度学习)"
bigquant_doc_url = ""
# log = logbook.Logger(bigquant_friendly_name)
log = BigLogger(bigquant_friendly_name)


class BigQuantModule:
    def __init__(
        self,
        input_data: I.port("DataSource数据", specific_type_name="DataSource"),
        features: I.port("特征列表", specific_type_name="列表|DataSource"),
        window_size: I.int("窗口大小，滚动窗口大小，1为不滚动，只用当前一行特征") = 1,
        feature_clip: I.int("特征裁剪值") = 5,
        flatten: I.bool("是否特征展开，如果为True是(window_size*feature_size,)，False是(window_wize，feature_size)") = True,
        window_along_col: I.str("分组滚动窗口，按window_along_col字段分组窗口滚动") = "instrument",
    ) -> [I.port("输出", "data"),]:
        """
        对使用深度学习模块的数据进行预处理，一般在标准化后使用。模块先根据“分组滚动窗口”对输入数据进行分组，再剪除特征绝对值大于“特征剪裁值”的数据避免极值的影响，最后根据“序列窗口大小”确定序列长度，如：窗口大小设置为5即为使用过去5天的因子数据。
        """
        self._features = smart_list(features)
        self._window_size = window_size
        self._data = input_data
        self._window_along_col = window_along_col
        self._flatten = flatten
        self._feature_clip = feature_clip

    def run(self):
        df = self._data.read_df()
        data = {}
        if self._window_size > 1:
            feature_list = list(data_generator(df, self._features, window_size=self._window_size, window_along_col=self._window_along_col))
            feature_array = np.asarray(feature_list, np.float32)
            if self._flatten:
                feature_array = np.reshape(feature_array, [-1, self._window_size * len(self._features)])
            data["x"] = feature_array
        else:
            data["x"] = df[self._features].values
        if self._feature_clip:
            data["x"] = np.clip(data["x"], -self._feature_clip, self._feature_clip)
        if "label" in df.columns:
            data["y"] = df["label"].values
        ds = DataSource.write_pickle(data)
        return Outputs(data=ds)


def _data_generator(features, data_idx, window_size, data_id2instrument_id, data_id2item_id, instrument_id2data_id_list):
    pad_features = np.zeros([window_size, features.shape[1]], np.float32)
    if window_size > 1:
        instrument_id = data_id2instrument_id[data_idx]  # 通过data_id找到对应股票的id
        group_data_id_list = instrument_id2data_id_list[instrument_id]  # 通过股票id，找到这只股票的数据id列表，是按日期有序排放的
        item_id = data_id2item_id[data_idx]  # 通过data_id，找到这个数据在这只股票的所有数据中的位置索引
        for idx in range(0, window_size):
            if item_id - window_size + idx + 1 < 0:
                continue  # 取股票前面特征的，如果没有填充为0
            pad_features[idx, :] = features[group_data_id_list[item_id - window_size + idx + 1]]
    else:
        pad_features[0, :] = features[data_idx]
    return pad_features


def data_generator(data_df, feature_list, window_size=1, window_along_col="instrument"):
    data_df = data_df.reset_index(drop=True)
    features = data_df[feature_list].values

    # 如果window_size大于1，需要找到对应股票前一个几个date的特征
    if window_along_col:
        instrument_id2data_id_list = data_df.groupby(window_along_col).apply(lambda x: list(x.index)).tolist()  # instrument_id 到data_id的对应
    else:
        instrument_id2data_id_list = [list(data_df.index)]  # instrument_id 到data_id的对应
    data_id2instrument_id = np.zeros(features.shape[0], np.int32)  # data_id 到 instrument_id的对应
    data_id2item_id = np.zeros(features.shape[0], np.int32)  # data_id 到 instrument_id的对应
    for instrument_id, items in enumerate(instrument_id2data_id_list):
        for item_id, data_id in enumerate(items):
            data_id2instrument_id[data_id] = instrument_id
            data_id2item_id[data_id] = item_id
    for data_idx in range(features.shape[0]):
        yield _data_generator(features, data_idx, window_size, data_id2instrument_id, data_id2item_id, instrument_id2data_id_list)


def bigquant_postrun(outputs):
    return outputs


def bigquant_cache_key(kwargs):
    if "features" in kwargs:
        kwargs["features"] = bigexpr.extract_variables(smart_list(kwargs["features"]))
    return kwargs
