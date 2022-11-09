# DataSource文档



## 配置环境变量

`bigdatasource__settings__datasource_version`: 'v5' 

​		通过 site_conf 中的 datasource_version 控制，线上默认是 v5, 已部署的B端客户默认是 'v3', 新部署的客户应该设置为 v5



`bigdatasource__settings__sync_update_per_node`:  False (默认)   

​		是否同步更新各个节点的数据(暂时只对Bigquant线上多份数据有用)



`CURRENT_NODE`： 'node2' 

​		 当前节点，更新数据时会默认自动更新 (Bigquant专用)


​		 数据节点 (Bigquant专用)



`bigdatasource__settings__table_base_folder_map`: {"table": 'folder_name'}  

​		指定表存储在不同的目录下，默认是 /var/app/data/bigquant/datasource/bigquant  指定folder_name后是

​		/var/app/data/bigquant/datasource/folder_name



`bigdatasource__settings__default_public_tables`： List  

​		默认不用检查权限的表，需要额外添加也可以在 bigjupyteruserservice 中设置环境变量

​		datasource_permission_unlimited_user

​		datasource_permission_unlimited_tables

​		datasource_permission_readable_by_user



## 更新数据

**不可以在策略开发环境中使用**

### 代码示例

```python

from bigdatasource.api import UpdateDataSource

alias = 'xxxx'  # 数据存储的表名
df = DataFrame  # 数据  DataFrame
schema = {}  # 数据定义

# v5 版本
res = UpdateDataSource().update(df=df, alias=alias, schema=schema)
# v4
res = UpdateDataSource().update(df=df, alias=alias, schema=schema)
# v3
res = UpdateDataSource().update_df(df=df, alias=alias, schema=schema)

```

### 参数

**df**： 数据 DataFrame

**alias**: str 表名

**schema**: dict 数据定义

​    ***friendly_name***： str 中文表名

​    ***desc***： str 表描述

​    ***primary_key***： list 数据表的主键字段(**用于数据去重，没有此字段默认不去重**)

​    ***date_field***: str  指定日期字段

​    ***partition_date***: str 分文件方式 (**必须需要date_field不为空**)

​	***fields***： dict 字段描述

​    	<u>field</u>: {'desc': 'xxxx', 'type': 'dtype'}

​    ***partition_field***: str 指定分目录字段  如: instrument 会根据传入的数据中的instrument字段，将数据groupby								后分别存储在对应的以 instrument 命名的目录下  (**v5新增**)    

**write_mode**： str 更新模式   

​    **update** 增量更新 

​    **rewrite** 删除原数据，覆盖更新  

​	**full** 删除当前数据所涉及的数据文件，部分覆盖更新

**public**： bool 公开数据 （**主要是用于用户数据**）



#### schema示例

表  **basic_info_CN_FUTURE**

```json
{'active': True,
 'category': '期货/基本信息',
 'date_field': None,
 'desc': '期货的基本信息',
 'doc_show_all_data': True,
 'fields': {'instrument': {'desc': '合约代码', 'type': 'str'},
  'delist_date': {'desc': '退市日期, 期指和期货主力默认为2099-01-01',
   'type': 'datetime64[ns]'},
  'exchange': {'desc': '交易市场', 'type': 'str'},
  'last_ddate': {'desc': '最后交割日, 期指和期货主力默认为2099-01-01',
   'type': 'datetime64[ns]'},
  'list_date': {'desc': '上市日期, 期指和期货主力默认为1993-01-01',
   'type': 'datetime64[ns]'},
  'multiplier': {'desc': '合约乘数', 'type': 'float64'},
  'name': {'desc': '期货合约名称', 'type': 'str'},
  'symbol': {'desc': '交易标识', 'type': 'str'},
  'price_tick': {'desc': '报价单位', 'type': 'float64'},
  'price_tick_desc': {'desc': '报价单位说明', 'type': 'str'}},
 'friendly_name': '期货基本信息',
 'partition_date': None,
 'primary_key': ['instrument'],
 'rank': 2001000,
 'version': '4.0',
 'partition_field': 'product_code'}
```





## 读取数据

代码示例

```python
from bigdatasource.api import DataSource  # 在策略开发环境中不用import

table = 'bar1d_CN_STOCK_A'
df = DataSource(table).read(instruments=['000001.SZA'], start_date='2021-01-01', end_date='2021-04-01', fields=['open', 'close'], query='close>open')

```



### 参数

**table**: str 表名

**instruments**： list 证券列表

**product_codes**: list 品种代码(期货数据)

**start_date**： str 开始时间

**query**： str 结束时间

**fields**： list 字段列表

**query**： str 过滤条件

**除table外的其他参数都是可选参数**





