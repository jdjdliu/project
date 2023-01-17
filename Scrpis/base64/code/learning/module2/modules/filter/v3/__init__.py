# -*- coding: utf-8 -*-
from sdk.utils import BigLogger

import pandas as pd
from sdk.datasource import evalex

import learning.module2.common.interface as I
from learning.module2.common.data import DataSource, Outputs

# log = logbook.Logger('filter')
log = BigLogger("filter")
bigquant_cacheable = True

# 模块接口定义
bigquant_category = "数据处理"
bigquant_friendly_name = "数据过滤"
bigquant_doc_url = "https://bigquant.com/docs/develop/modules/filter.html"


class BigQuantModule:
    def __init__(
        self,
        input_data: I.port("输入数据", specific_type_name="DataSource"),
        expr: I.str(
            r"过滤表达式， 参考示例代码和[DataFrame.query](http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.query.html)，包含特使字符的列名需要使用反单引号(\`)引起来，例如 \`close_10/close0\` > 0.91",
            specific_type_name="字符串",
        ),
        output_left_data: I.bool("输出剩余数据") = False,
    ) -> [I.port("输出数据", "data"), I.port("剩余数据", "left_data"),]:
        """
        根据过滤表达式根据过滤表达式过滤DataSource (数据类型为 DataFrame)
        """
        self.__dataset_ds = input_data
        self.__output_left_data = output_left_data
        self.__expr = expr

    def run(self):
        dataset_ds = DataSource()
        output_store = dataset_ds.open_df_store()
        left_dataset_ds = None
        if self.__output_left_data:
            left_dataset_ds = DataSource()
            left_output_store = left_dataset_ds.open_df_store()

        log.info("使用表达式 %s 过滤" % self.__expr)
        row_count = 0
        left_count = 0
        try:
            for key, df in self.__dataset_ds.iter_df():
                result = evalex(df, self.__expr)
                final_df = df[result]
                if len(final_df) > 0:
                    final_df.to_hdf(output_store, key)
                    row_count += len(final_df)
                if left_dataset_ds is not None:
                    left_df = df[~result]
                    left_size = len(left_df)
                    if len(left_df) > 0:
                        left_df.to_hdf(left_output_store, key)
                        left_count += len(left_df)
                else:
                    left_size = 0
                log.info("过滤 %s, %s/%s/%s" % (key, len(final_df), left_size, len(df)))
        except Exception:
            row_count = 0
            left_count = 0
            dataset_ds = DataSource()
            output_store = dataset_ds.open_df_store()
            df = self.__dataset_ds.read_df()
            result = evalex(df, self.__expr)
            final_df = df[result]
            if len(final_df) > 0:
                final_df.to_hdf(output_store, "data")
                row_count += len(final_df)

            if left_dataset_ds is not None:
                left_df = df[~result]
                if len(left_df) > 0:
                    left_df.to_hdf(left_output_store, key)
                    left_count += len(left_df)

            log.info("过滤 %s/%s/%s" % (row_count, left_count, len(df)))

        dataset_ds.close_df_store()
        if left_dataset_ds is not None:
            left_dataset_ds.close_df_store()

        return Outputs(data=dataset_ds, row_count=row_count, left_data=left_dataset_ds)


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
