from datetime import datetime

from sdk.utils import BigLogger
from learning.module2.common.data import Outputs, DataSource

# log = logbook.Logger('join')
log = BigLogger("join")
bigquant_cacheable = True


MAIN_TABLE_NAMES = ["/main", "/data"]


class BigQuantModule:
    def __init__(self, data1, data2, on, how="inner", sort=False, fill_na_value=None, raise_exception_if_no_data=True):
        """
        连接两个DataSource (数据内容DataFrame)，参考 `DataFrame.join <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.join.html>`_

        :param DataSource data1: 第一个DataSource
        :param DataSource data2: 第二个DataSource
        :param 字符串数组 on: join时使用的主要列
        :param 'left'|'right'|'outer|'inner' how: 合并方法
        :param boolean sort: 是否对结果按on指定的列排序
        :return: join后的数据, .data字段
        :rtype: Outputs
        """

        self.__data1 = data1
        self.__data2 = data2
        self.__on = on
        self.__how = how
        self.__sort = sort
        self.__fill_na_value = fill_na_value
        self.__raise_exception_if_no_data = raise_exception_if_no_data

    @staticmethod
    def merge_ratings(features, ratings):
        return features.merge(ratings, on=["date", "instrument"], how="inner")

    def __join_table(self, output_store, store1, table1, df2):
        start_time = datetime.now()
        df1 = store1[table1]
        df_merged = df1.merge(df2, on=self.__on, how=self.__how, sort=self.__sort)
        if self.__fill_na_value is not None:
            df_merged.fillna(self.__fill_na_value, inplace=True)
        output_store[table1] = df_merged
        ts = (datetime.now() - start_time).total_seconds()
        log.info("%s, rows=%s/%s, timetaken=%ss" % (table1, len(df_merged), len(df1), ts))
        return len(df_merged)

    def __join_per_table(self, output_store, store1, store2, tables):
        rows = 0
        for table in tables:
            rows += self.__join_table(output_store, store1, table, store2[table])
        return rows

    def __join_with_small_main_table(self, output_store, large_store, large_store_tables, small_store, small_store_table):
        small_df = small_store[small_store_table]
        rows = 0
        for table in large_store_tables:
            rows += self.__join_table(output_store, large_store, table, small_df)
        return rows

    def run(self):
        outputs = Outputs(data=DataSource())
        output_store = outputs.data.open_df_store()

        store1 = self.__data1.open_df_store()
        store2 = self.__data2.open_df_store()

        tables1 = store1.keys()
        tables2 = store2.keys()

        if len(tables1) == 1 and tables1[0] in MAIN_TABLE_NAMES:
            outputs.rows = self.__join_with_small_main_table(output_store, store2, tables2, store1, tables1[0])
        elif len(tables2) == 1 and tables2[0] in MAIN_TABLE_NAMES:
            outputs.rows = self.__join_with_small_main_table(output_store, store1, tables1, store2, tables2[0])
        else:
            tables = set(tables1) & set(tables2)
            if not tables:
                log.warning("no common table: %s, %s" % (tables1, tables2))
            outputs.rows = self.__join_per_table(output_store, store1, store2, tables)

        self.__data1.close_df_store()
        self.__data2.close_df_store()
        outputs.data.close_df_store()

        log.info("total result rows: %s" % outputs.rows)
        if self.__raise_exception_if_no_data and outputs.rows == 0:
            raise Exception("result set is empty")

        return outputs


def bigquant_postrun(outputs):
    return outputs
