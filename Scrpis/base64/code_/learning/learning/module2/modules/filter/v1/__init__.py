# -*- coding: utf-8 -*-
import pandas as pd

from learning.module2.common.data import Outputs, DataSource
from learning.module2.common.utils import read_hdf, iter_hdf
from sdk.utils import BigLogger

# log = logbook.Logger('filter')
log = BigLogger("filter")
bigquant_cacheable = True
bigquant_deprecated = "请更新到 ${MODULE_NAME} 最新版本"


class BigQuantModule:
    def __init__(self, data, expr):
        """
        使用给定表达式过滤数据
        :param data:
        :param expr:
        """
        self.__dataset_ds = data
        self.__expr = expr

    def run(self):
        dataset_ds = DataSource()
        output_store = pd.HDFStore(dataset_ds.path)

        log.info("filter with expr %s" % self.__expr)
        row_count = 0
        for key, df in iter_hdf(self.__dataset_ds.path):
            final_df = df.query(self.__expr)
            if len(final_df) > 0:
                final_df.to_hdf(output_store, key)
                row_count += len(final_df)
            log.info("filter %s, %s/%s" % (key, len(final_df), len(df)))

        output_store.close()

        return Outputs(data=dataset_ds, row_count=row_count)


def bigquant_postrun(outputs):
    outputs.extend_methods(read_data=lambda self, key=None: read_hdf(self.data.path, key))

    return outputs


if __name__ == "__main__":
    # 测试代码
    df = pd.DataFrame({"date": [1, 2, 3], "score": [30, 20, 10], "random_score": [30, 20, 10]})
    input = DataSource()
    df.to_hdf(input.path, "data")

    m = BigQuantModule(input, "score > 20")
    output = m.run()
    output = bigquant_postrun(output)

    df = output.read_dataset()
    assert len(df) == 1
    print(df)
