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
    ) -> [I.port("输出", "data"),]:
        """
        对使用深度学习模块的数据进行预处理
        """
        self.__features = smart_list(features)
        self.__window_size = window_size
        self.__data = input_data

    def run(self):
        def rolling_window(a, window, axis=-1):
            if axis == -1:
                axis = a.ndim - 1

            if 0 <= axis <= a.ndim - 1:
                shape = a.shape[:axis] + (a.shape[axis] - window + 1, window) + a.shape[axis + 1 :]
                strides = a.strides[:axis] + (a.strides[axis],) + a.strides[axis:]
                return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)
            else:
                raise ValueError("rolling_window: axis out of bounds")

        df = self.__data.read_df()
        data = {}
        if self.__window_size > 1:
            data["x"] = rolling_window(df[self.__features].values, self.__window_size, 0)
        else:
            data["x"] = df[self.__features].values
        if "label" in df.columns:
            data["y"] = df["label"].values[self.__window_size - 1 :]
        ds = DataSource.write_pickle(data)
        return Outputs(data=ds)


def bigquant_postrun(outputs):
    return outputs


def bigquant_cache_key(kwargs):
    if "features" in kwargs:
        kwargs["features"] = bigexpr.extract_variables(smart_list(kwargs["features"]))
    return kwargs
