import datetime

def change_fields_type(df, schema):
    fields_info = schema['fields']
    primary_lst = schema['primary_key']
    date_field = schema['date_field']
    if date_field:
        primary_lst.append(date_field)

    for col in df.columns:
        col_type = str(df[col].dtype)
        field_type = fields_info[col]['type']
        if col_type == field_type:
            continue
        print(col, col_type, field_type)
        if field_type == 'datetime64[ns]':
            if col not in primary_lst:
                df.loc[df[col] < datetime.date(1800,1,1), col] = None
            df = df[(df[col] >= datetime.date(1800, 1, 1)) | (df[col].isnull())]  # MySQL读取的date类型pandas会自己转为datetime.date

        df.loc[df[col].isnull(), col] = None
        if 'int' in field_type:
            df[col] = df[col].fillna(0).astype(field_type)
        else:
            df[col] = df[col].astype(field_type)
    return df


def is_trading_day(date=datetime.datetime.now(), country_code="CN", delta_days=0, trading_ds="all_trading_days", holidays_ds="holidays_CN"):
    """
    判断是否是交易日

    Args:
        date: str/datetime.datetime   日期，默认是当前日期
        country_code: str 国家, 默认 CN
        delta_days: int  延迟时间，负值表示向前取天数
        trading_ds: str  交易日历 表名
        holidays_ds: str  假期表 表名

    Returns: bool

    """
    # from bigdatasource.impl.bigdatasource import DataSource
    from sdk.datasource import DataSource
    day_format = "%Y-%m-%d"
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, day_format)

    if delta_days:
        date += datetime.timedelta(days=delta_days)
    date_str = date.strftime(day_format)
    # 判断是否是周末
    if date.weekday() in [5, 6]:
        return False

    # 判断是否在交易日历中
    trading_day_df = DataSource(trading_ds).read(start_date=date_str, end_date=date_str)
    if not trading_day_df is None and 'country_code' in trading_day_df.columns:
        trading_day_df = trading_day_df[trading_day_df.country_code == country_code]
        if not trading_day_df.empty:
            return True

    # 判断是否是假期
    holidays_df = DataSource(holidays_ds).read()
    holidays_df = holidays_df[holidays_df.date == date_str]
    if not holidays_df.empty:
        return False

    return True
