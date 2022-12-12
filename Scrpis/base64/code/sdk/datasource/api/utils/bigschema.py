import os
import json
import datetime

import pandas as pd

from sdk.datasource.api.utils import gen_table_path


class BigSchema(object):
    YEAR = "Y"
    MONTH = "M"
    DAY = "D"

    default_date_field = 'date'

    def __init__(self, table=None, schema=None):
        self.table = table
        self.__schema = schema

    def __str__(self):
        return json.dumps(self.schema, ensure_ascii=False)

    @property
    def file_path(self):
        t_path = gen_table_path(self.table)
        schema_file = os.path.join(t_path, 'schema.json')
        return schema_file

    @property
    def schema(self):
        if self.__schema:
            return self.__schema
        if not os.path.exists(self.file_path):
            return {}
        with open(self.file_path, 'r', encoding='utf-8') as fp:
            schema = json.load(fp)
        return schema

    def to_file(self):
        with open(self.file_path, 'w', encoding='utf-8') as fp:
            json.dump(self.schema, fp, ensure_ascii=False)

    @property
    def date_field(self):
        return self.schema.get('date_field')

    @property
    def primary_key(self):
        return self.schema.get('primary_key', [])

    @property
    def fields(self):
        return self.schema.get('fields', {})

    @property
    def partition_date(self):
        return self.schema.get('partition_date')

    @property
    def partition_field(self):
        # 按照给定的数据字段分文件目录， 支持 instrument(证券代码)  product_code(期货品种)
        return self.schema.get('partition_field')

    @property
    def friendly_name(self):
        return self.schema.get('friendly_name', '')

    @property
    def desc(self):
        return self.schema.get('desc', '')

    @property
    def file_type(self):
        return self.schema.get('file_type', '')


class DatePartitioner(object):

    def __init__(self, big_schema):
        self.big_schema = big_schema

    def __iter_dates(self, start_date, end_date):
        cdate = pd.to_datetime(start_date).date()
        edate = pd.to_datetime(end_date).date()
        while cdate <= edate:
            yield cdate
            cdate += datetime.timedelta(days=1)

    def __iter_months(self, start_date, end_date):
        sdate = pd.to_datetime(start_date).date()
        edate = pd.to_datetime(end_date).date()
        cur = (sdate.year, sdate.month)
        end = (edate.year, edate.month)
        while cur <= end:
            yield datetime.datetime(cur[0], cur[1], 1)
            y, m = divmod(cur[1] + 1, 13)
            cur = (cur[0] + y, y + m)

    def on_read(self, start_date, end_date):
        if not self.big_schema.date_field or not self.big_schema.partition_date:
            return ['all']
        if self.big_schema.partition_date == self.big_schema.YEAR:
            return list(map(str, range(int(start_date[:4]), int(end_date[:4]) + 1)))
        if self.big_schema.partition_date == self.big_schema.MONTH:
            return [x.strftime('%Y-%m') for x in self.__iter_months(start_date, end_date)]
        if self.big_schema.partition_date == self.big_schema.DAY:
            return [x.isoformat() for x in self.__iter_dates(start_date, end_date)]
        raise Exception('unknown partition_date %s' % self.big_schema.partition_date)

    def on_write(self, df):
        date_field = self.big_schema.date_field
        if not date_field or not self.big_schema.partition_date:
            return [('all', df)]
        if date_field not in df.columns:
            raise Exception("cannot find date column {} in df".format(date_field))
        date_column = df[date_field]
        if 'datetime' not in str(df[date_field].dtype):
            date_column = pd.to_datetime(date_column)
        partition_date = self.big_schema.partition_date
        if partition_date == self.big_schema.YEAR:
            keys = date_column.dt.year
            return sorted([(str(k), v) for k, v in df.groupby(keys)])
        elif partition_date == self.big_schema.MONTH:
            keys = date_column.dt.year * 100 + date_column.dt.month
            return sorted([('{}-{}'.format(str(k)[:4], str(k)[-2:]), v) for k, v in df.groupby(keys)])
        elif partition_date == self.big_schema.DAY:
            keys = date_column.dt.year * 10000 + date_column.dt.month * 100 + date_column.dt.day
            return sorted([("{}-{}-{}".format(str(k)[:4], str(k)[4:6], str(k)[6:]), v) for k, v in df.groupby(keys)])
        raise Exception('unknown partition_date {}'.format(partition_date))
