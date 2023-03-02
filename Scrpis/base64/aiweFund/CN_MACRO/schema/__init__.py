
TABLES_MAPS = {
    'MACRO_ZSYHEDB': {
        'source_table': 'AIWEMACRO.MACRO_ZSYHEDB',
        'date': 'index_date',
        'date_format': '%Y-%m-%d',
        'instrument': 'edb_code',
        'schema': {
            'desc': 'edb指标信息',
            'active': True,
            'date_field': 'date',
            'category': '宏观（原表）/收益率曲线',
            'rank': 6002001,
            'friendly_name': 'edb指标信息',
            'partition_date': 'Y',
            'primary_key': ['id'],
            'fields': {
                'id': {'desc': '主键id', 'type': 'int64'},
                'object_id': {'desc': '对象id', 'type': 'str'},
                'edb_code': {'desc': '指标edb代码', 'type': 'str'},
                'index_name': {'desc': '指标名称', 'type': 'str'},
                'unit': {'desc': '指标单位', 'type': 'str'},
                'frequency': {'desc': '更新频率，1：日，2：周，3：月，4：季，5：半年，6：年', 'type': 'int64'},
                'index_month': {'desc': '指标值月份时间', 'type': 'str'},
                'index_date': {'desc': '指标日期', 'type': 'datetime64[ns]'},
                'index_value': {'desc': '指标值', 'type': 'float64'},
                # 'op_date': {'desc': '上游操作日期', 'type': 'datetime64[ns]'},
                # 'op_mode': {'desc': '上游操作类型', 'type': 'str'},
                'create_time': {'desc': '创建时间', 'type': 'datetime64[ns]'},
                'update_time': {'desc': '更新时间', 'type': 'datetime64[ns]'},

                'date': {'desc': '指标日期', 'type': 'datetime64[ns]'},
                'instrument': {'desc': '指标edb代码', 'type': 'str'},
            },
        },
    },


}