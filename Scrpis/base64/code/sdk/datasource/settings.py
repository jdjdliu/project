import os

from sdk.common import OSEnv

default_sysytem_user = "bigquant"
CURRENT_USER = os.getenv("JPY_USER", default_sysytem_user)

debug = False

datasource_version = "v5"

PROJECT_NAME = "bigdatasource"

# 最高权限用户
admin_user = os.getenv("ADMIN_USER", "bigquant")

BASE_PATH = "/var/app/data/bigquant/datasource"
# BASE_PATH = '/tmp/datasource'

# 数据目录  /var/app/data/bigquant/datasource/bigquant/v5
DATA_BASE_DIR_NAME = "v5"


# 保存到每日数据构建文件目录中，只对数据构建环境使用
datasource_daily_path = "/var/app/data/bigquant/datasource/daily"
# 被删除的数据临时存放目录
delete_data_path = "/var/app/data/bigquant/datasource/bigquant/.delete"
# 用户数据目录
USERPRIVATE_FOLDER = "userprivate"

USER_ALIAS_SUFFIX = "_U"  # 用户数据alias后缀
USER_HASH_SUFFIX = "U"  # 用户数据uuid后缀


KB = 1024
MB = 1024 * KB
GB = 1024 * MB

DAY_FORMAT = "%Y-%m-%d"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 废弃的因子: 可替代的新因子
deprecated_factors = {}

CN_FUTURE_dic_path = "/var/app/data/bigquant/datasource/bigquant/CN_FUTURE.json"
CN_FUND_dic_path = "/var/app/data/bigquant/datasource/bigquant/CN_FUND.json"
CN_STOCK_A_dic_path = "/var/app/data/bigquant/datasource/bigquant/CN_STOCK_A.json"
# 股票因子map(旧版,B端客户还在使用)
features_dic_path = "/var/app/data/bigquant/datasource/bigquant/features_map.json"
# sys_table_info = '/var/app/data/bigquant/datasource/bigquant/sys_tables.h5'
# 因子map
features_map_file = "/var/app/data/bigquant/datasource/bigquant/features_map_{market}.json"

# 表对应的基础目录映射，默认是放在 bigquant 目录下
table_base_folder_map = {
    # 表名: 基础目录， 默认是在 bigquant 目录下
    "test_tablexxxx": "test_folderxxxx"
}

# 默认公开数据，不会检查数据权限，所有用户都可读取  TODO　添加期货、期权、债券等的基础表
default_public_tables = ["all_trading_days", "basic_info_CN_STOCK_A", "holidays_CN", "trading_days"]

# 并发读取数据设置
parallel_min_count = 80  # 并发读写取临界值，读写的目录大于这个值，才会并发读写，目前只用于高频按照品种拆分的数据
parallel_processes_count = 10  # 并发读写进程数

# 单次更新数据最大数据量 MB
update_max_size = int(os.getenv("UPDATE_MAX_SIZE", 1024 * 50))


# 数据版本环境变量名称
DATA_VERSION_NAME = "CURRENT_DATA_VERSION"
DATA_VERSION_CATCHUP = "DATA_VERSION_CATCHUP"

# 因子库
alpha_repo_api = os.getenv("ALPHA_REPO_API", "http://bigweb:20000/factor_repo/factor/datasource")


# 数据平台服务地址
DATAPLATFORM_HOST = OSEnv.str("DATAPLATFORM_HOST", "http://dataplatform:8000")
is_paper_trading = os.environ.get("RUN_MODE", "backtest") == "papertrading"
DATAPLATFORM_TOKEN_NAME = OSEnv.str("DATAPLATFORM_TOKEN_NAME", "bqdataplatformtoken")
DATAPLATFORM_TOKEN = OSEnv.str("DATAPLATFORM_TOKEN", "Xb*mx!Ogx2oz@B#r")
DATAPLATFORM_USER_TOKEN_NAME = os.getenv("DATAPLATFORM_USER_TOKEN_NAME", "bqusername")
DATAPLATFORM_METADATA_URL = os.getenv("DATAPLATFORM_METADATA_URL", "/api/dataplatform/datasource/metadata")
DATAPLATFORM_STATISTICS_URL = os.getenv("DATAPLATFORM_STATISTICS_URL", "/api/dataplatform/dashboard/statistic")
DATAPLATFORM_CACHE_CREATE_URL = os.getenv("DATAPLATFORM_CACHE_CREATE_URL", "/api/dataplatform/datasource/cache/create")
# 修改数据任务状态url
DATAPLATFORM_DATATASK_STATUS_URL = os.getenv("DATAPLATFORM_DATATASK_STATUS_URL", "/api/dataplatform/datatask/status")
# 数据代理
DATA_PROXY_URL = OSEnv.str("DATA_PROXY_URL", "http://dataproxy:8000")
# 同步更新远程数据服务
UPDATE_PROXY_DATA = os.getenv("UPDATE_PROXY_DATA", False)
# 是否使用代理读取数据
READ_PROXY_DATA = OSEnv.bool("READ_PROXY_DATA", False)
