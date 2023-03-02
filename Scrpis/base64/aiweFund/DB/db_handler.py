import pymysql
import pandas as pd
from DB.res import CONN_DIC


class MysqlFetch:

    def __init__(self):
        # 打开数据库连接
        # self.db = pymysql.connect(host=CONN_DIC['host'], port=CONN_DIC['port'], user=CONN_DIC['user'],
        #                           password=CONN_DIC['password'], database=CONN_DIC['database'])
        pass

    def __del__(self):
        # 回收对象时关闭数据库连接
        print('close mysql connection....')
        self.db.close()

    def execute_sql(self, sql_str):
        print('execute_sql: ', sql_str)
        cursor = self.db.cursor()
        cursor.execute(sql_str)
        # 使用 fetchall() 方法获取全部数据.
        data = cursor.fetchall()
        cursor.execute(sql_str)
        schema = cursor.description

        return data, schema

    def select_table_schema(self, table):
        table_schema, table_name = table.split('.')

        sql_str = f"""
                    select 
                        table_schema, 
                        table_name, 
                        column_name, 
                        column_comment, 
                        column_type, 
                        column_key, 
                        ordinal_position 
                    from 
                        information_schema.columns 
                    where 
                        table_schema = '{table_schema}' 
                        and table_name = '{table_name}'
                    """
        print(sql_str)
        table_df = self.select_data(table=table, sql_str=sql_str)
        return table_df

    def select_data(self, table, params="", where_condition="", sql_str=""):
        if sql_str:
            pass
        else:
            if len(table.split('.')) != 2:
                raise Exception(f'table【{table}】must have database, eg: aiwefund.fund_basicinfo')

            if not params:
                params = "*"

            sql_str = f"""select {params} from {table}"""

            if where_condition:
                sql_str = sql_str + f' where {where_condition}'
        data, schema = self.execute_sql(sql_str=sql_str)
        schema = [x[0] for x in schema]
        df = pd.DataFrame(data, columns=schema)
        return df

    def pandas_read_sql(self, sql_str):
        res = pd.read_sql(sql_str, con=self.db)
        return res

    def _read_basic_smb(self, basic_table, fnd_cd_lst):
        params = 'fnd_cd, smb, exg_cd'
        if fnd_cd_lst:
            if len(fnd_cd_lst) == 1:
                fnd_cd_params = f"('{fnd_cd_lst[0]}')"
            else:
                fnd_cd_params = str(tuple(fnd_cd_lst))
            sql_str = f"""
                    select {params} from {basic_table} where fnd_cd in {fnd_cd_params} 
            """
            print(sql_str)
            basic_df = self.select_data(table=basic_table, sql_str=sql_str)

        else:
            basic_df = pd.DataFrame(columns=params.split(','))
        return basic_df


if __name__ == "__main__":
    obj = MysqlFetch()
    test_df = obj.select_data(table='AIWEFUND.FUND_BASICINFO', sql_str='select fnd_cd, smb from AIWEFUND.FUND_BASICINFO limit 1;')
    print(test_df)
    print(str(tuple(test_df.fnd_cd.unique().tolist())))

