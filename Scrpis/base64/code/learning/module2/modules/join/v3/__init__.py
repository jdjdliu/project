from datetime import datetime
import re
import pandas as pd

from learning.module2.common.data import Outputs, DataSource
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

# log = logbook.Logger('join')
log = BigLogger("join")
bigquant_cacheable = True


MAIN_TABLE_NAMES = ["/main", "/data"]


# 模块接口定义
bigquant_category = "数据处理"
bigquant_friendly_name = "连接数据"
bigquant_doc_url = "https://bigquant.com/docs/"


HOW_SWITCH = {
    "left": "right",
    "right": "left",
}


class BigQuantModule:
    def __init__(
        self,
        data1: I.port("第一个输入数据", specific_type_name="DataSource"),
        data2: I.port("第二个输入数据", specific_type_name="DataSource"),
        on: I.str("关联列，多个列用英文逗号分隔") = "date,instrument",
        how: I.choice("连接方式", ["left", "right", "outer", "inner"]) = "inner",
        sort: I.bool("对结果排序") = False,
    ) -> [I.port("连接后的数据", "data")]:
        """
        连接两个DataSource (数据内容DataFrame)，按列进行横向连接
        """

        self.__data1 = data1
        self.__data2 = data2
        self.__on = smart_list(on, sep=",")
        self.__how = how
        self.__sort = sort

    def __join_table(self, output_store, store1, table1, df2):
        start_time = datetime.now()
        df1 = store1[table1]
        df_merged = df1.merge(df2, on=self.__on, how=self.__how, sort=self.__sort)
        output_store[table1] = df_merged
        ts = (datetime.now() - start_time).total_seconds()
        log.info(f"{table1}, 行数={len(df_merged)}/{len(df1)}, 耗时={ts}s")
        return len(df_merged)

    def __join_per_table(self, store1, store2, tables):
        outputs = Outputs(data=DataSource())
        output_store = outputs.data.open_df_store()
        outputs.rows = 0
        for table in tables:
            outputs.rows += self.__join_table(output_store, store1, table, store2[table])
        outputs.data.close_df_store()
        return outputs

    def __join_with_small_main_table(self, large_store, large_store_tables, df):
        outputs = Outputs(data=DataSource())
        output_store = outputs.data.open_df_store()
        outputs.rows = 0
        for large_store_table in large_store_tables:
            outputs.rows += self.__join_table(output_store, large_store, large_store_table, df)
        outputs.data.close_df_store()
        return outputs

    def __direct_merge(self):
        df1 = self.__data1.read()
        df2 = self.__data2.read()

        start_time = datetime.now()
        df_merged = df1.merge(df2, on=self.__on, how=self.__how, sort=self.__sort)
        ts = (datetime.now() - start_time).total_seconds()

        data = DataSource.write_df(df_merged)
        outputs = Outputs(data=data)
        outputs.rows = len(df_merged)
        log.info(f"行数={outputs.rows}/{len(df1)}/{len(df2)}, 耗时={ts}s")
        return outputs

    def run(self):
        store1 = self.__data1.open_df_store()
        store2 = self.__data2.open_df_store()

        tables1 = store1.keys()
        tables2 = store2.keys()

        if self.__how == "inner" and len(tables1) == 1:
            small_df = store1[tables1[0]]
            outputs = self.__join_with_small_main_table(store2, tables2, small_df)
        elif self.__how == "inner" and len(tables2) == 1:
            small_df = store2[tables2[0]]
            outputs = self.__join_with_small_main_table(store1, tables1, small_df)
        elif self.__how == "inner" and any(map(lambda x: bool(re.match(r"/y_\d{4}", x)), tables1 + tables2)):
            tables = sorted(list(set(tables1) & set(tables2)))
            if not tables:
                log.warning(f"没有共同的表: {tables1}, {tables2}")
                outputs = Outputs(data=DataSource().write_df(pd.DataFrame()), rows=0)
            else:
                outputs = self.__join_per_table(store1, store2, tables)
        elif self.__how == "left":
            df_right = self.__data2.read()
            outputs = self.__join_with_small_main_table(store1, tables1, df_right)
        elif self.__how == "right":
            df_left = self.__data1.read()
            self.__how = HOW_SWITCH.get(self.__how, self.__how)
            outputs = self.__join_with_small_main_table(store2, tables2, df_left)
        else:
            outputs = self.__direct_merge()

        log.info(f"最终行数: {outputs.rows}")

        if id(self.__data1) != id(self.__data2):
            self.__data1.close_df_store()
            self.__data2.close_df_store()
        else:
            self.__data1.close_df_store()

        return outputs


def bigquant_postrun(outputs):
    return outputs
