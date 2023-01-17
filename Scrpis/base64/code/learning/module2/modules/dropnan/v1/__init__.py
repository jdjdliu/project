from learning.module2.common.data import Outputs, DataSource
import learning.module2.common.interface as I
from sdk.utils import BigLogger
import pandas as pd

# log = logbook.Logger('dropnan')
log = BigLogger("dropnan")
bigquant_cacheable = True

# 模块接口定义
bigquant_category = "数据处理"
bigquant_friendly_name = "缺失数据处理"
bigquant_doc_url = "https://bigquant.com/docs/"


class BigQuantModule:
    def __init__(self, input_data: I.port("输入数据源")) -> [I.port("数据", "data")]:
        """


        缺失数据处理：删除有缺失数据的行


        """
        self.__data = input_data

    def run(self):
        output_ds = DataSource()
        output_store = output_ds.open_df_store()

        before_drop_row_count = 0
        after_drop_row_count = 0
        for key, df in self.__data.iter_df():
            before_drop_row_count += len(df)
            final_df = df.dropna()
            if len(final_df) > 0:
                for column in final_df.columns:
                    if pd.api.types.is_bool(final_df[column].iloc[0]):
                        final_df[column] = final_df[column].astype("int")
                output_store[key] = final_df
                after_drop_row_count += len(final_df)
            log.info("%s, %s/%s" % (key, len(final_df), len(df)))

        output_ds.close_df_store()

        if after_drop_row_count == 0:
            raise Exception("no data left after dropnan")
        log.info("行数: %s/%s" % (after_drop_row_count, before_drop_row_count))

        return Outputs(data=output_ds)


def bigquant_postrun(outputs):
    return outputs
