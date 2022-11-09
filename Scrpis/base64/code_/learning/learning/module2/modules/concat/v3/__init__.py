# -*- coding: utf-8 -*-


from collections import namedtuple
import logbook
import pandas as pd

import learning.module2.common.interface as I
from learning.module2.common.data import DataSource, Outputs
from sdk.utils import BigLogger

# log = logbook.Logger('concat')
log = BigLogger("concat")
bigquant_cacheable = True

# 模块接口定义
bigquant_category = "数据处理"
bigquant_friendly_name = "数据合并"
bigquant_doc_url = "https://bigquant.com/docs/develop/modules/concat.html"

# 模块主函数


def bigquant_run(
    input_data_1: I.port("输入1，DataSource第1个", optional=False) = None,
    input_data_2: I.port("输入2，DataSource第2个", optional=False) = None,
    input_data_3: I.port("输入3，DataSource第3个", optional=True) = None,
    input_data_list: I.doc("输入列表，DataSource列表") = [],
) -> [I.port("合并后的数据", "data")]:
    """
    将输入的数据按行进行上下合并，比如df1、df2各自10行，合并后就是20行的一个数据
    """

    dataset_ds = DataSource()
    store = dataset_ds.open_df_store()

    InputSet = namedtuple("InputSet", ["store", "keys"])
    if not input_data_list:
        input_data_list = [x for x in [input_data_1, input_data_2, input_data_3] if x]
    inputs = []
    for input in input_data_list:
        input_store = input.open_df_store()
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
            log.info("合并: %s, 行数=%s" % (key, len(df)))
        df = pd.concat(df_list)
        df.to_hdf(store, key)
    for input in input_data_list:
        input.close_df_store()
    dataset_ds.close_df_store()

    return Outputs(data=dataset_ds, row_count=row_count)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # 测试代码
    import sys

    logbook.StreamHandler(sys.stdout).push_application()

    df = pd.DataFrame({"date": [1, 2, 3], "score": [30, 20, 10], "random_score": [30, 20, 10]})
    input_1 = DataSource.write_df(df)

    df = pd.DataFrame({"date": [4, 5, 5], "score": [30, 20, 10], "random_score": [30, 10, 20]})
    input_2 = DataSource.write_df(df)

    m = bigquant_run(data=[input_1, input_2])
    m = bigquant_postrun(m)

    df = m.data.read_df()
    assert len(df) == 6
    assert df.score.iloc[3] == 30
