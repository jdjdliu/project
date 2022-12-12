# -*- coding: utf-8 -*-
import pandas as pd

from learning.module2.common.data import Outputs, DataSource
from sdk.utils import BigLogger

# log = logbook.Logger('filter')
log = BigLogger("filter")
bigquant_cacheable = True


class BigQuantModule:
    def __init__(self, data, expr):
        """
        过滤DataSource (数据内容DataFrame)

        :param DataSource data: 输入数据
        :param 字符串 expr: 过滤表达式， 参考 `使用样例` 和 `DataFrame.query <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.query.html>`_
        :return: 输出数据，.data字段
        :rtype: Outputs
        """
        self.__dataset_ds = data
        self.__expr = expr

    def run(self):
        dataset_ds = DataSource()
        output_store = dataset_ds.open_df_store()

        log.info("filter with expr %s" % self.__expr)
        row_count = 0
        for key, df in self.__dataset_ds.iter_df():
            final_df = df.query(self.__expr)
            if len(final_df) > 0:
                final_df.to_hdf(output_store, key)
                row_count += len(final_df)
            log.info("filter %s, %s/%s" % (key, len(final_df), len(df)))

        dataset_ds.close_df_store()

        return Outputs(data=dataset_ds, row_count=row_count)


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # 测试代码
    df = pd.DataFrame({"date": [1, 2, 3], "score": [30, 20, 10], "random_score": [30, 20, 10]})
    input = DataSource.write_df(df)

    m = BigQuantModule(input, "score > 20")
    output = m.run()
    output = bigquant_postrun(output)

    df = output.data.read_df()
    assert len(df) == 1
    print(df)
