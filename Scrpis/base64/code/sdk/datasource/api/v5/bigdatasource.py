import os
import re
import datetime
from collections import defaultdict

import requests
import pandas as pd
try:
    import pyarrow as pa
except Exception:  # noqa
    pass

from . import BaseDataSource
from sdk.datasource.settings import alpha_repo_api, DAY_FORMAT, DATE_FORMAT
from sdk.datasource.api.constant import FileType
from sdk.datasource.api.utils import gen_pkl_path
from sdk.datasource.api.utils.parallel import parallel_map
from sdk.datasource.api.utils.bdb import write_bdb_helper, read_bdb_helper
from sdk.datasource.api.utils.dataplatform import get_metadata
from sdk.datasource.api.utils.dsimpl import STORAGE_KEY, is_system_ds, write_df, read_df, \
    iter_df, write_pickle, read_pickle, write_csv, read_csv, open_temp_path, \
    close_temp_path, on_temp_path, open_file, close_file, open_df_store, close_df_store, \
    id_to_intermediate_path, gen_id_for_temp_ds, on_create, on_visit, id_to_path
from sdk.datasource.api.utils.proxy import write_df_proxy

class DataSource(BaseDataSource):

    def __init__(self, id=None, **kwargs):
        """
        Bigquant DataSource

        Args:
            id: str 表名或 datasource id

        """
        self.__bq_protected_id = id.replace(".", "_") if isinstance(id, str) else id
        self.__bq_protected_writable = False
        if not self.__bq_protected_id:
            self.__bq_protected_id = gen_id_for_temp_ds()
            self.__bq_protected_writable = True
        super(DataSource, self).__init__(id=self.__bq_protected_id, **kwargs)

    def read(self, instruments=None, start_date=None, end_date=None, fields=None, query=None, product_codes=None, **kwargs):
        """
        读取数据

        Args:
            instruments: str/list 证券代码
            start_date: str/datetime 开始时间 默认 2005-01-01
            end_date: str/datetime 结束时间 默认当前日期向后10天
            fields:  str/list 字段名称
            query: str 查询条件
            product_codes:  str/list 期货品种代码
            **kwargs: 其他
                bdb: bool 返回数据格式, 默认 False, 返回 DataFrame, True: 返回为 bdb_table
                adjust_type: str 复权模式, 默认post后复权, 可选pre 前复权(仅支持股票日线表)

        Returns: DataFrame / None / bdb_table

        """
        self.log.debug('datasource read start: id:{} instrument_count:{} start_date:{} end_date:{} fields:{} query:{} product_codes:{} kwargs:{}'.format(
            self.id, len(instruments) if instruments else None, start_date, end_date, fields, query, product_codes, kwargs))
        instruments, start_date, end_date, fields, product_codes = self.handler_args(instruments, start_date, end_date, fields, product_codes)
        self.log.debug('real arguments: id:{} instruments:{} start_date:{} end_date:{} fields:{} query:{} product_codes:{} kwargs:{}'.format(
            self.id, len(instruments) if instruments else None, start_date, end_date, fields, query, product_codes, kwargs))
        bdb = kwargs.get("bdb", False)

        metadata = get_metadata(self.id, self.username, get_meta=True)
        data = None
        adjust_type = str(kwargs.get("adjust_type", 'post'))
        if self.id == 'bar1d_CN_STOCK_A' and adjust_type.upper() != 'POST':
            # 前复权数据计算
            data = self.calculate_by_adjtype(instruments, start_date, end_date, fields, adjust_type)

        elif is_system_ds(self.id):
            # 检查系统数据权限
            is_active = metadata.get("active", True)
            public = metadata.get("public", False)
            if self.id.startswith("alpha"):
                self.__bq_protected_id = self.get_alpha_id(alpha_id=self.__bq_protected_id)
                # TODO: 检查用户权限， 临时全部开放
                public = True
            if not is_active and not public:
                msg = metadata.get("msg", "")
                self.log.warning(f'permission deny for table {self.id} {msg}')
                return None
            # 系统数据或者用户数据
            data = self._read(instruments=instruments, start_date=start_date, end_date=end_date, fields=fields, product_codes=product_codes, bdb=bdb)

        else:
            # 读取逻辑表或者缓存数据
            schema = metadata.get("schema", {})
            if isinstance(schema, dict) and schema.get('parent_table'):
                data = self.read_parent_table(instruments=instruments, start_date=start_date, end_date=end_date, fields=fields, product_codes=product_codes)
            else:
                # TODO 读取缓存，考虑缓存的id和系统数据冲突的情况
                data = self.read_cache_pkl(metadata, **kwargs)

        if data is None:
            self.log.debug("No data in table {} {}->{}".format(self.id, start_date, end_date))
            return None

        if isinstance(data, pd.DataFrame):
            data = self.handler_df(data, query)
            if bdb:
                self.log.debug('trans pd.DataFrame -> bdb Table')
                data = pa.Table.from_pandas(data)
        return data

    def handler_df(self, data, query):
        """处理数据"""
        if self.id.startswith('level') or 'bar1m' in self.id:
            sort_columns = ['instrument']
            if 'date' in data.columns:
                sort_columns = ['date', 'instrument']
            data.sort_values(sort_columns, inplace=True)
            data.reset_index(inplace=True, drop=True)
        data.sort_index(inplace=True)
        self.log.debug('read data end: {} {}'.format(self.id, data.shape))
        return data

    def handler_args(self, instruments=None, start_date=None, end_date=None, fields=None, product_codes=None):

        if not start_date:
            start_date = '2005-01-01'
        if not end_date:
            end_date = (datetime.datetime.now() + datetime.timedelta(days=10)).strftime(DAY_FORMAT)
        start_date = self.convert_str(start_date)
        end_date = self.convert_str(end_date)
        instruments = self.convert_list(instruments)
        product_codes = self.convert_list(product_codes)
        fields = self.convert_list(fields)

        if instruments and not product_codes and "CN_FUTURE" in self.id.upper():
            product_codes = [re.findall(r'\D+', instrument) for instrument in instruments]
            product_codes = [p[0] for p in product_codes if p]
        if product_codes:
            product_codes = list(set([p.upper() for p in product_codes]))

        if self.__bq_protected_id.startswith("alpha_"):
            self.__bq_protected_id = self.get_alpha_id(self.__bq_protected_id)
        return instruments, start_date, end_date, fields, product_codes

    def read_cache_pkl(self, metadata, **kwargs):
        self.log.debug('read_cache_pkl {} {}'.format(kwargs, metadata))
        df = None
        try:
            df = self.read_df()
        except Exception as e:
            self.log.debug(f"read df failed, use read pickle: {e}")
            df = self.read_pickle(**kwargs)
        return df

    def read_parent_table(self, instruments, start_date, end_date, fields=None, product_codes=None):
        """读取逻辑父表(多个子表合并)"""
        self.log.debug('read parent table data: {} ...'.format(self.id))
        metadata = get_metadata(self.id, username=self.username, get_meta=True)
        if not metadata:
            return None
        # owner = metadata.get('owner')
        schema = metadata.get('schema', {})
        primary_key = schema.get("primary_key") or ['date', 'instrument']

        all_fields = schema.get('fields')
        table_fields = defaultdict(list)
        for field, desc in all_fields.items():
            if fields and field not in fields:
                continue
            if field in primary_key:
                self.log.debug('{} in primary_key {}'.format(field, primary_key))
                continue
            table = desc.get('table')
            table_fields[table].append(field)
            # TODO 权限限制
            # public = desc.get('public', False)
            # if owner in [admin_user, self.username] or public:
            #     table_fields[table].append(field)
            # else:
            #     self.log.debug('user {} no permission on field {}'.format(self.username, field))
        res = None
        for table, _fields in table_fields.items():
            self.log.debug('read child table {} {} {} {} {} {}'.format(table, instruments, start_date, end_date, _fields, product_codes))
            _df = DataSource(table)._read(instruments=instruments, start_date=start_date, end_date=end_date, fields=_fields, product_codes=product_codes)
            if _df is None:
                continue
            if res is None:
                res = _df
            else:
                res = res.merge(_df, on=primary_key, how='outer')
            del _df
        return res

    def get_alpha_id(self, alpha_id):
        """因子看板因子获取"""

        data = {
            "SERVICE_AUTH_TOKEN": os.getenv('SERVICE_AUTH_TOKEN'),
            "id": alpha_id,
            'username': self.username
        }
        try:
            self.log.debug('get alpha id: {} {}'.format(alpha_id, self.username))
            response = requests.post(alpha_repo_api, json=data)
            resp_data = response.json()
            alpha_id = resp_data.get("data").get("factor").get("datasource")
            self.log.debug('get alpha id success, {}'.format(alpha_id))
        except Exception as e:
            self.log.debug('Error get alpha id, {}'.format(e))
        return alpha_id

    def list(self, sys_table=True, contain_public=True):
        """
        查看可以读取的数据表

            以DataFrame展示，包含表名，描述，数据开始时间，结束时间

        """
        # TODO 展示表
        return None

    @staticmethod
    def convert_str(item):
        if not item:
            return item
        if isinstance(item, (datetime.datetime, pd.Timestamp)):
            item = item.strftime(DATE_FORMAT)
        return item

    @staticmethod
    def convert_list(item):
        if not item:
            return item
        if isinstance(item, str):
            item = item.split(",")
        if not isinstance(item, list) and item is not None:
            item = list(item)
        return item

    # TODO remove alias=None, temp=True,
    @staticmethod
    def write_df(df, alias=None, sync_data_proxy=False, key=STORAGE_KEY, use_cache=False, **kwargs):
        datasource = DataSource(write_df(df, key, use_cache, **kwargs))
        if sync_data_proxy:
            alias = alias or datasource.id
            write_df_proxy(df, alias=alias)
        return datasource

    def read_all_df(self, key=None):
        # TODO: remove this??
        return self.read_df(key)

    def read_df(self, key=None):
        if self.__bq_protected_writable:
            raise Exception('DataSource不可读 (protected_writable={})'.format(self.__bq_protected_writable))
        return read_df(self.id, self.intermediate_version, key)

    def iter_df(self):
        if self.__bq_protected_writable:
            raise Exception('DataSource不可读 (protected_writable={})'.format(self.__bq_protected_writable))
        return iter_df(self.id, self.intermediate_version)

    @staticmethod
    def write_pickle(obj, alias=None, temp=True, use_dill=False, use_cache=False, **kwargs):
        return DataSource(write_pickle(obj, use_dill, use_cache, **kwargs))

    def read_pickle(self, use_dill=False, return_use_dill=False):
        return read_pickle(self.id, self.intermediate_version, use_dill, return_use_dill)

    @staticmethod
    def write_csv(df, alias=None, temp=True, **kwargs):
        return DataSource(write_csv(df, **kwargs))

    def read_csv(self, index_col=0, **kwargs):
        return read_csv(self.id, self.intermediate_version, index_col=index_col, **kwargs)

    @staticmethod
    def write_bdb(df):
        source_id = gen_id_for_temp_ds()
        path = id_to_intermediate_path(source_id)
        write_bdb_helper(df, fn=path)
        on_create(source_id, file_type=FileType.BDB)
        return DataSource(source_id)

    def read_bdb(self, bdb=False):
        on_visit(self.id)
        path = id_to_path(self.id, self.intermediate_version)
        res = read_bdb_helper(fn=path, bdb=bdb)
        return res

    def open_temp_path(self):
        self.__bq_protected_temp_path = open_temp_path(self.id, self.intermediate_version, self.__bq_protected_writable)
        return self.__bq_protected_temp_path

    def close_temp_path(self):
        close_temp_path(self.id, self.intermediate_version, self.__bq_protected_writable, self.__bq_protected_temp_path)
        self.__bq_protected_writable = False
        del self.__bq_protected_temp_path

    def on_temp_path(self, func):
        ds = on_temp_path(self.id, self.intermediate_version, self.__bq_protected_writable, func)
        self.__bq_protected_writable = False
        return ds

    def open_file(self, binary=False):
        self.__bq_protected_file = open_file(self.id, self.intermediate_version, self.__bq_protected_writable, binary)
        return self.__bq_protected_file

    def close_file(self):
        close_file(self.__bq_protected_file, self.id, self.intermediate_version, self.__bq_protected_writable)
        self.__bq_protected_writable = False
        del self.__bq_protected_file

    def open_df_store(self):
        self.__bq_protected_df_store = open_df_store(
            self.id, self.intermediate_version, self.__bq_protected_writable)

        return self.__bq_protected_df_store

    def close_df_store(self):
        close_df_store(
            self.__bq_protected_df_store,
            self.id,
            self.intermediate_version,
            self.__bq_protected_writable,
        )
        self.__bq_protected_writable = False
        del self.__bq_protected_df_store

    def exist(self):
        path = id_to_path(self.id, self.intermediate_version)
        return os.path.exists(path)

    def calculate_pre(self, bar1d_df, start_date, end_date, price_cols):
        instruments = sorted(bar1d_df['instrument'].unique())
        self.log.debug('instruments: {}; start_date: {}; end_date: {}, df.shape: {}{}'.format(len(instruments), start_date, end_date, len(bar1d_df), len(bar1d_df.columns)))
        devidend_df = DataSource('dividend_send_CN_STOCK_A').read(instruments=instruments, start_date='2000-01-01',
                                                                  fields=['ex_date', 'bonus_conversed_sum', 'cash_after_tax'])
        if devidend_df is None or devidend_df.empty:
            return bar1d_df
        devidend_df = devidend_df[devidend_df['ex_date'].between(start_date, end_date)]
        devidend_df['date'] = devidend_df['ex_date']
        df = bar1d_df.merge(devidend_df, on=['date', 'instrument'], how='outer').drop(columns=['report_date'])
        self.log.debug('devided instruments: {}; df.shape: {}{}'.format(len(instruments), len(bar1d_df), len(bar1d_df.columns)))
        if len(df['instrument'].unique()) > 50:
            key_list = [(ins_df, price_cols) for ins, ins_df in df.groupby(['instrument'])]
            df_list = parallel_map(self.calculate_by_instrument, key_list, show_progress=False, chunk_size=20)
        else:
            df_list = []
            for ins, ins_df in df.groupby(['instrument']):
                df_list.append(self.calculate_by_instrument((ins_df, price_cols)))
        res_df = pd.concat(df_list, ignore_index=True).sort_values(['date', 'instrument'])
        return res_df

    def records_pre(self, df, records, price_cols):
        for col in price_cols:
            for record in records:
                df[col] = (df[col] - record.get('cash_after_tax', 0)) / (1 + record.get('bonus_conversed_sum', 0))
        return df.drop(columns=['cash_after_tax', 'bonus_conversed_sum', 'ex_date'])

    def calculate_by_instrument(self, args):
        ins_df, price_cols = args
        ins_df['ex_date'] = ins_df['ex_date'].shift(-1).fillna(method='bfill')
        ins_df['cash_after_tax'] = ins_df['cash_after_tax'].shift(-1)
        ins_df['bonus_conversed_sum'] = ins_df['bonus_conversed_sum'].shift(-1)
        df_list = []
        for date, date_df in ins_df.groupby(['ex_date']):
            records = ins_df[ins_df.ex_date >= date].dropna()[
                ['bonus_conversed_sum', 'cash_after_tax', 'ex_date']].to_dict('records')
            res_df = self.records_pre(date_df, records, price_cols)
            df_list.append(res_df)
        if not ins_df[ins_df.ex_date.isna()].empty:
            df_list.append(self.records_pre(ins_df[ins_df.ex_date.isna()], [], price_cols))
        return pd.concat(df_list, ignore_index=True)

    def calculate_by_adjtype(self, instruments, start_date, end_date, fields, adjust_type):
        self.log.debug('The real price calculation beginning ****')
        price_cols = ['open', 'close', 'high', 'low']
        if fields:
            price_cols = set(price_cols) & set(fields)
            fields = sorted({'date', 'instrument', 'adjust_factor'} | set(fields))
        df = self.read(instruments, start_date, end_date, fields=fields, adjust_type='post')
        if df is None or df.empty:
            return None
        for col in price_cols:
            if col not in df.columns:
                continue
            df[col] = df[col] / df['adjust_factor']
        if adjust_type.upper() == 'PRE':
            self.log.debug('The pre price calculation beginning ****')
            df = self.calculate_pre(df, start_date, end_date, price_cols)
        return df
