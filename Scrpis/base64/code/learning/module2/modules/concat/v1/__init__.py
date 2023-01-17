# -*- coding: utf-8 -*-


from collections import namedtuple
from sdk.utils import BigLogger

import logbook
import pandas as pd

from learning.module2.common.data import DataSource, Outputs

# log = logbook.Logger('concat')
log = BigLogger("concat")
bigquant_cacheable = True
bigquant_deprecated = "请更新到 ${MODULE_NAME} 最新版本"


# 模块主函数


def bigquant_run(data=[]):
    dataset_ds = DataSource()
    store = pd.HDFStore(dataset_ds.path)

    InputSet = namedtuple("InputSet", ["store", "keys"])
    inputs = []
    for input in data:
        input_store = pd.HDFStore(input.path, mode="r")
        input_keys = set(input_store.keys())
        inputs.append(InputSet(input_store, input_keys))

    keys = set()
    for input in inputs:
        keys |= input.keys

    row_count = 0
    for key in keys:
        df_list = []
        for input_dataset in inputs:
            if key not in input_dataset.keys:
                continue
            df = pd.read_hdf(input_dataset.store, key)
            df_list.append(df)
            row_count += len(df)
            log.info("concat: %s, rows=%s" % (key, len(df)))
        df = pd.concat(df_list)
        df.to_hdf(store, key)
    for input_dataset in inputs:
        input_dataset.store.close()
    store.close()

    return Outputs(data=dataset_ds, row_count=row_count)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):
    outputs.extend_methods(read_data=lambda self: pd.read_hdf(self.data.path))

    return outputs


if __name__ == "__main__":
    # 测试代码
    import sys

    logbook.StreamHandler(sys.stdout).push_application()

    df = pd.DataFrame({"date": [1, 2, 3], "score": [30, 20, 10], "random_score": [30, 20, 10]})
    input_1 = DataSource()
    df.to_hdf(input_1.path, "data")

    df = pd.DataFrame({"date": [4, 5, 5], "score": [30, 20, 10], "random_score": [30, 10, 20]})
    input_2 = DataSource()
    df.to_hdf(input_2.path, "data")

    m = bigquant_run(data=[input_1, input_2])
    m = bigquant_postrun(m)

    df = m.read_data()
    assert len(df) == 6
    assert df.score.iloc[3] == 30
