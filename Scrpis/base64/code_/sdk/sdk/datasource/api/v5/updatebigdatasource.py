import datetime
import hashlib
import os
import pickle
import zlib

import pandas as pd

from sdk.common import base64
from sdk.datasource.api.constant import DataBase, FileType
from sdk.datasource.api.utils.dataplatform import get_metadata, push_metadata
from sdk.datasource.api.utils.proxy import update_proxy
from sdk.datasource.settings import UPDATE_PROXY_DATA, USER_ALIAS_SUFFIX, default_sysytem_user, update_max_size

from .bigdatasource import DataSource

# 默认系统
default_system_owner = "system"


class UpdateDataSource(DataSource):
    def __init__(self, id=None, owner=None, **kwargs):
        """

        Args:
            id: str   datasource id
            owner: str 拥有者
            kwargs: dict
                parent_tables: list 父表列表，可以通过父表名读取所有子表数据聚合
                auto_update_metadata: bool  是否每次更新都同步更新metadata
                debug: bool 是否开启debug模式
                current_node: str (已废弃) 当前数据节点执行本地更新，其他节点使用client同步更新
                                  如果为 None 则在更新数据时会将 node1/2/3 的数据都通过 bigdataservice2 client 更新
        """
        self.owner = os.environ.get("JPY_USER") or owner or default_system_owner
        self.parent_tables = kwargs.get("parent_tables", [])
        super(UpdateDataSource, self).__init__(id=id, username=owner, debug=kwargs.get("debug", False))

    def write(self, df, alias, schema, **kwargs):
        """

        Args:
            df: DataFrame
            alias: str
            schema: dict
            **kwargs
                public: bool
        """
        public = kwargs.get("public", False)
        self.update(df=df, alias=alias, schema=schema, public=public, update_schema=True, write_mode=self.WRITE_MODE_FULL)

    def update(self, df, alias, schema=None, **kwargs):
        """

        Args:
            df: DataFrame
            alias: str
            schema: dict
                {
                    'friendly_name': 'A股股票分钟行情数据',  # 数据文档中的标题
                    'desc': '股票分钟行情数据',  # 数据描述
                    'category': 'A股/行情数据',  # 数据文档中的分类
                    'date_field': 'date',  # 日期字段，用于底层分文件存储
                    'partition_date': 'M',  # 底层分文件时间访问 Y-年 M-月 D-日
                    'primary_key': ['date', 'instrument'],  # 主键字段 (数据去重)
                    # 'partition_field': 'instrument'  # (可选参数) 存储数据按照某个指定字段进行拆分
                    'rank': 1002006,  # 数据文档排序
                    'active': False,  # 是否展示到数据文档中
                    'file_type': 'bdb',  # 底层数据存储格式, 默认是h5，可选(h5, bdb)
                    'fields': {  # 数据字段描述
                        'date': {'desc': '日期', 'type': 'datetime64[ns]'},
                        'instrument': {'desc': '证券代码', 'type': 'str'},
                        'low': {'desc': '最低价', 'type': 'float32'},
                        'open': {'desc': '开盘价', 'type': 'float32'},
                        ...
                    },
                }
            **kwargs
                write_mode: str update/full/rewrite
                update_schema: bool
                public: bool
        """
        # 用户开发环境数据更新
        if df is None:
            self.log.warning("No data in given df!")
            return

        df, alias, schema = self.validate_args(df, alias, schema)
        write_mode = kwargs.get("write_mode", DataSource.WRITE_MODE_UPDATE)
        update_schema = kwargs.get("update_schema", False)

        date_field = schema.get("date_field")
        if date_field and date_field in df.columns:
            df[date_field] = pd.to_datetime(df[date_field])  

        public = kwargs.get("public", False)
        if write_mode != DataSource.WRITE_MODE_UPDATE:
            update_schema = True
        if not update_schema and not self.get_metadata(alias):
            update_schema = True

        self._update_bdb(df, alias, schema=schema, write_mode=write_mode, update_schema=update_schema)

        self.update_metadata(alias=alias, schema=schema, public=public, df=df, update_schema=update_schema)
        if self.parent_tables:
            self.update_parent_table(alias, df, public=public)

        if UPDATE_PROXY_DATA:
            data = {
                "alias": alias,
                "schema": schema,
                "write_mode": write_mode,
                "public": public,
                "file_type": kwargs.get("file_type", "bdb"),
                "df": self.compress_df(df),
                "owner": self.owner,
                "parent_tables": self.parent_tables,
                "kwargs": kwargs,
                "options": "update",
            }
            # 远程同步更新
            update_proxy(data)

    def update_parent_table(self, alias, df=None, public=False):
        """
        更新父表
            逻辑表，记录字段对应在哪个表中，读取时直接拼接对应的数据

        Args:
            df: pd.DataFrame
            alias: str 子表名

        """
        self.log.info("update parent table {} with child table {} ".format(self.parent_tables, alias))
        current_metadata = self.get_metadata(alias)
        current_schema = {}
        if current_metadata:
            current_schema = current_metadata.get("schema", {})
        primary_key = current_schema.get("primary_key", [])
        columns = list(current_schema.get("fields", {}).keys())
        if isinstance(df, pd.DataFrame):
            columns = list(df.columns)
        self.log.debug("child columns is: {}".format(columns))
        fields = {}
        for field in columns:
            desc = current_schema.get("fields", {}).get(field, {}).get("desc")
            type = current_schema.get("fields", {}).get(field, {}).get("type")
            fields[field] = {"desc": desc, "type": type, "table": alias, "public": public}

        for parent_table in self.parent_tables:
            old_metadata = self.get_metadata(parent_table)
            if old_metadata:
                if self.owner not in [old_metadata.get("owner"), default_sysytem_user]:
                    raise Exception("table {} already exist!, you don't have permission, change another table name!".format(parent_table))
                old_schema = old_metadata.get("schema", {})
                old_fields = old_schema.get("fields", {})
                old_fields.update(fields)
                fields = old_fields
                primary_key = old_schema.get("primary_key", [])
            schema = {
                "active": False,
                "desc": "parent_table",
                "parent_table": True,
                "fields": fields,
                "friendly_name": "parent_table",
                "primary_key": primary_key,
            }
            self.update_metadata(alias=parent_table, schema=schema, df=df)

    @staticmethod
    def compress_df(df):
        """压缩df"""
        df = pickle.dumps(df)
        df_bytes = zlib.compress(df, level=4)
        df = base64.b64encode(df_bytes).decode("utf-8")
        return df

    @staticmethod
    def decompress_df(df):
        """解压df"""
        if isinstance(df, str):
            df = base64.b64decode(df.encode("utf-8"))
        decom_bytes = zlib.decompress(df)
        df = pickle.loads(decom_bytes)
        return df

    def get_metadata(self, alias):
        return get_metadata(alias, get_meta=True)

    def update_metadata(self, alias, df=None, update_schema=False, **kwargs):
        """
        更新metadata
        only update schema: update_metadata(alias, schema=schema)

        Args:
            alias: str 表名
            **kwargs: dict
                '_id': str,
                'alias': str,
                'data_base': str,
                'owner': str,
                'file_type': str,
                'temp': bool,
                'public': bool,
                'schema': dict,
                'create_time': datetime.datetime.now(),
                'update_time': datetime.datetime.now(),

        """
        metadata = self.get_metadata(alias)
        if metadata:
            metadata["owner"] = self.owner
            metadata["update_time"] = datetime.datetime.now()
            metadata.update(kwargs)
        else:
            if alias.endswith(USER_ALIAS_SUFFIX):
                data_base = DataBase.USER_PRIVATE
            else:
                data_base = DataBase.BIGQUANT

            schema = kwargs.get("schema")
            if not schema:
                raise Exception("schema cannot be 'None' when create new metadata!")

            # 兼容bigjupyteruservice中返回visit_data, id必须为str
            _id = kwargs.get("_id")
            if not _id:
                _id = hashlib.md5(alias.encode(encoding="utf-8")).hexdigest()
            metadata = {
                "_id": _id,
                "alias": alias,
                "data_base": data_base,
                "owner": self.owner,
                "file_type": kwargs.get("file_type", FileType.BDB),
                "temp": False,
                "schema": schema,
                "update_time": datetime.datetime.now(),
                "version": self.version,
                "public": kwargs.get("public", False),
            }
        push_metadata(table=alias, metadata=metadata, df=df, update_schema=update_schema)

    def validate_args(self, df, alias, schema):
        alias = alias.replace(".", "_") if isinstance(alias, str) else alias
        # 非用户数据特殊处理
        if isinstance(df, DataSource):
            df = df.read()
        if isinstance(df, str):
            df = self.decompress_df(df)
        if not schema:
            metadata = self.get_metadata(alias)
            schema = metadata.get("schema") if metadata else {}

        # 限制每次更新是的数据量，避免打爆机器内存
        data_size = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        if data_size > update_max_size:
            raise Exception(
                "The amount of data in this update exceeds the data limit for a single update! "
                "It may bring down the server, You are advised to update "
                "data less than {} M, current is {} M".format(update_max_size, data_size)
            )
        schema["file_type"] = FileType.BDB
        return df, alias, schema
