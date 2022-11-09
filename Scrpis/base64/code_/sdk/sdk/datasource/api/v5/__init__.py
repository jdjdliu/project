import datetime
import logging
import os

import pandas as pd
from sdk.datasource.api.utils import bigschema, ensure_dtypes, gen_bdb_path, gen_table_path
from sdk.datasource.api.utils.bdb import bdb_table_to_df, concat_bdb_tables, filter_bdb_table, read_bdb_helper, write_bdb_helper
from sdk.datasource.api.utils.bigschema import BigSchema, DatePartitioner
from sdk.datasource.api.utils.parallel import parallel_map
from sdk.datasource.api.utils.proxy import read_proxy
from sdk.datasource.settings import (
    CURRENT_USER,
    DATA_VERSION_CATCHUP,
    DATA_VERSION_NAME,
    DAY_FORMAT,
    PROJECT_NAME,
    READ_PROXY_DATA,
    datasource_version,
    parallel_min_count,
    parallel_processes_count,
)
from sdk.utils import BigLogger


class BaseDataSource(object):

    # 缓存数据版本，写入目录
    intermediate_version = "v3"

    WRITE_MODE_UPDATE = "update"  # 更新
    WRITE_MODE_FULL = "full"  # 覆盖当前文件
    WRITE_MODE_REWRITE = "rewrite"  # 删除所有文件，重新写入数据

    version = datasource_version

    def __init__(self, id=None, **kwargs):
        """
        Bigquant DataSource

        Args:
            id: str 表名或数据id(在使用read接口时此参数为必填参数)

        """
        self.__bq_protected_id = id
        self.username = CURRENT_USER or kwargs.get("username")
        self.log = BigLogger(PROJECT_NAME)

    def __repr__(self):
        return "DataSource({})".format(self.id)

    @property
    def id(self):
        return self.__bq_protected_id

    @staticmethod
    def init(version=None, use_memory_map=False, catchup=False, debug=False):
        """
        数据初始化设置

        Args:
            version (str, optional): 设置数据版本,例如: version='2022-01-01'
            use_memory_map (bool, optional): 使用memory_map方式读取数据(多次读取同一数据更快)
            debug (bool, optional): 展示debug日志
            catchup (bool, optional): 是否自动合并所设置数据日期之后的数据修改内容, 默认False
        """
        # 数据版本设置
        if version:
            os.environ[DATA_VERSION_NAME] = version
        else:
            os.environ[DATA_VERSION_NAME] = ""

        if catchup:
            os.environ[DATA_VERSION_CATCHUP] = "True"
        else:
            os.environ[DATA_VERSION_CATCHUP] = ""

        if use_memory_map:
            os.environ["BDB_USE_MEMORY_MAP"] = "True"
        else:
            os.environ["BDB_USE_MEMORY_MAP"] = ""

        # 开启debug模式
        if debug is True:
            os.environ["LOGGING_LEVEL"] = str(logging.DEBUG)
        else:
            os.environ["LOGGING_LEVEL"] = str(logging.INFO)

    def _read(self, instruments=None, start_date=None, end_date=None, fields=None, schema=None, product_codes=None, bdb=False):
        self.log.debug(
            "datasource_read id: {} instrument_count:{} start_date:{} end_date:{} fields:{} product_codes:{}".format(
                self.id, len(instruments) if instruments else None, start_date, end_date, fields, product_codes
            )
        )
        if not start_date:
            start_date = "2005-01-01"
        if not end_date:
            end_date = (datetime.datetime.now() + datetime.timedelta(days=10)).strftime(DAY_FORMAT)

        table_path = gen_table_path(self.id)
        if READ_PROXY_DATA and not os.path.exists(table_path):
            # 使用数据代理读取数据
            df = read_proxy(username=self.username, table=self.id, instruments=instruments, start_date=start_date, end_date=end_date, fields=fields)
            return df
        df = self._read_bdb(
            instruments=instruments, start_date=start_date, end_date=end_date, fields=fields, schema=schema, product_codes=product_codes, bdb=bdb
        )
        return df

    def _read_bdb(self, instruments=None, start_date=None, end_date=None, fields=None, product_codes=None, schema=None, bdb=False):
        self.log.debug(
            "read bdb instrument_count:{} start_date:{} end_date:{} fields:{} schema:{}".format(
                len(instruments) if instruments else None, start_date, end_date, fields, schema
            )
        )
        big_schema = BigSchema(table=self.id, schema=schema)
        partition_field = big_schema.partition_field
        date_field = big_schema.date_field
        if fields:
            fields = list(set(big_schema.primary_key + fields))
            if partition_field and partition_field not in fields:
                fields.append(partition_field)
            if date_field and date_field not in fields:
                fields.append(date_field)
        if partition_field:
            bdb_table = self._read_bdb_partition_field(
                instruments=instruments, start_date=start_date, end_date=end_date, big_schema=big_schema, product_codes=product_codes, fields=fields
            )
        else:
            partitioner = DatePartitioner(big_schema)
            bdb_table = self._read_bdb_table(
                partitioner,
                start_date,
                end_date,
                fields=fields,
                date_field=date_field,
                instruments=instruments,
                product_codes=product_codes,
            )
        if not bdb_table:
            return None

        if bdb:
            return bdb_table
        df = bdb_table_to_df(bdb_table)
        df.reset_index(drop=True, inplace=True)
        return df

    def _read_bdb_table(self, partitioner, start_date, end_date, sub_dir=None, fields=None, date_field=None, instruments=None, product_codes=None):
        _tables = []
        for filename in partitioner.on_read(start_date, end_date):
            fn = gen_bdb_path(table=self.id, filename=filename, sub_dir=sub_dir)
            if not os.path.exists(fn):
                continue
            _table = read_bdb_helper(fn, bdb=True)
            self.log.debug("read file {}.bdb shape: {}".format(filename, _table.shape))
            # 过滤数据
            _table = filter_bdb_table(
                _table,
                start_date=start_date,
                end_date=end_date,
                fields=fields,
                date_field=date_field,
                instruments=instruments,
                product_codes=product_codes,
            )
            self.log.debug("filter data shape: {} ".format(_table.shape))
            _tables.append(_table)
            del _table
        bdb_table = concat_bdb_tables(_tables)
        return bdb_table

    def _read_bdb_partition_field(self, instruments=None, start_date=None, end_date=None, product_codes=None, big_schema=None, fields=None):
        sub_dirs = None
        if big_schema.partition_field == "instrument":
            # 按照股票存储，每支股票的数据存储一个目录, 数据整体存储在文件中，不按照字段拆分目录，按照 instrument 拆分目录
            sub_dirs = instruments
            instruments = None
        if big_schema.partition_field == "product_code":
            # 期货数据按照品种拆分, 大写品种, 数据整体存储在文件中，不按照字段拆分目录，按照 product_code 拆分目录
            sub_dirs = product_codes

        if not sub_dirs:
            # 没有指定读取数据时，读取表目录下的所有文件目录的数据
            t_path = gen_table_path(table=self.id)
            sub_dirs = os.listdir(t_path) if os.path.exists(t_path) else []

        partitioner = DatePartitioner(big_schema)
        self.log.debug("_read_bdb_partition_field sub_dirs={}".format(len(sub_dirs)))
        if len(sub_dirs) < parallel_min_count:
            bdb_table_list = []
            for sub_dir in sub_dirs:
                _table = self._read_bdb_table(
                    partitioner,
                    start_date,
                    end_date,
                    sub_dir,
                    fields=fields,
                    date_field=big_schema.date_field,
                    instruments=instruments,
                    product_codes=product_codes,
                )
                bdb_table_list.append(_table)
        else:
            args_list = []
            for sub_dir in sub_dirs:
                args_list.append(
                    {
                        "partitioner": partitioner,
                        "start_date": start_date,
                        "end_date": end_date,
                        "sub_dir": sub_dir,
                        "fields": fields,
                        "date_field": big_schema.date_field,
                        "instruments": instruments,
                        "product_codes": product_codes,
                    }
                )
            bdb_table_list = parallel_map(
                self._read_bdb_table, args_list, processes_count=parallel_processes_count, return_result=True, use_threads=True
            )
        self.log.debug("concat_bdb_tables: {}".format(len(bdb_table_list)))
        bdb_table = concat_bdb_tables(bdb_table_list)
        return bdb_table

    def _write_bdb(self, df, alias, schema):
        self.log.info("write_bdb: {}".format(df.shape))
        return self._update_bdb(df, alias, schema, write_mode=self.WRITE_MODE_REWRITE)

    def _update_bdb(self, df, alias, schema, write_mode=None, update_schema=False):
        self.log.info("update_bdb: {}, write_mode={}".format(df.shape, write_mode))
        df, schema = ensure_dtypes(df, schema=schema)
        big_schema = BigSchema(table=alias, schema=schema)
        if big_schema.partition_field:
            self._update_bdb_partition_field(df, alias, write_mode=write_mode, big_schema=big_schema)
        else:
            partitioner = DatePartitioner(big_schema)
            for filename, _df in partitioner.on_write(df):
                fn = gen_bdb_path(alias, filename, auto_mkdir=True)
                self._upsert_bdb_file(_df, fn, big_schema=big_schema, write_mode=write_mode)
        if update_schema or write_mode == self.WRITE_MODE_REWRITE or not os.path.exists(big_schema.file_path):
            big_schema.to_file()

    def _upsert_bdb_file(self, df, fn, big_schema, write_mode=None):
        """写入 / 更新 bdb 文件"""
        if os.path.exists(fn):
            if write_mode in [self.WRITE_MODE_REWRITE, self.WRITE_MODE_FULL]:
                os.remove(fn)
            else:
                old_df = read_bdb_helper(fn)
                # TODO 可以优化为 直接 bdb concat 之后直接报错数据，不用转df
                df = pd.concat([old_df, df], ignore_index=True)
        if big_schema.primary_key:
            df.drop_duplicates(list(big_schema.primary_key), keep="last", inplace=True)
        if big_schema.date_field:
            df.sort_values(big_schema.date_field, inplace=True)
        self.log.info("save bdb data: {} {}".format(fn, df.shape))
        write_bdb_helper(df=df, fn=fn)

    def _update_bdb_partition_field(self, df, alias, write_mode=None, big_schema=None):
        self.log.debug("_update_bdb_partition_field ...")
        if big_schema.partition_field not in df.columns:
            raise Exception("partition_field {} not in df columns!".format(big_schema.partition_field))

        self.log.info("parallel_update data ....")
        args_list = [(alias, _df, big_schema, _sub_dir, write_mode) for _sub_dir, _df in df.groupby(big_schema.partition_field)]
        if len(args_list) > parallel_min_count:
            self.log.info("parallel_update data start ....")
            parallel_map(func=self._update_partition_bdb, items=args_list, processes_count=parallel_processes_count)
        else:
            for args in args_list:
                self._update_partition_bdb(args)

    def _update_partition_bdb(self, args):
        table, _df, big_schema, _sub_dir, write_mode = args
        partitioner = DatePartitioner(big_schema)
        for filename, ins_df in partitioner.on_write(_df):
            fn = gen_bdb_path(table=table, filename=filename, sub_dir=_sub_dir, auto_mkdir=True)
            self._upsert_bdb_file(ins_df, fn, big_schema=big_schema, write_mode=write_mode)
