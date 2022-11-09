from typing import Any, Dict

from sdk.common import OSEnv

TORTOISE_CONFIG_TYPE = Dict[str, Dict[str, Any]]

DB_ADAPTER = OSEnv.str("DB_ADAPTER", "mysql", description="数据库适配器")
DB_HOST = OSEnv.str("DB_HOST", "10.24.110.119", description="数据库地址")
DB_PORT = OSEnv.int("DB_PORT", 3306, description="数据库端口")
DB_USER = OSEnv.str("DB_USER", "root", description="数据库用户名")
DB_PASS = OSEnv.str("DB_PASS", "123456", description="数据库密码")
DB_URL = OSEnv.str("DB_URL", "", description="数据库连接字符串")
DB_CONNECTION_PARAMS = OSEnv.str("DB_CONNECTION_PARAMS", "?charset=utf8mb4&use_unicode=true&connect_timeout=10000")

if not DB_URL:
    DB_URL = f"{DB_ADAPTER}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{{database}}{DB_CONNECTION_PARAMS}"


TORTOISE_ORM: TORTOISE_CONFIG_TYPE = {
    "connections": {
        "default": DB_URL.format(database="aiwequantdb"),
    },
    "apps": {
        # add `aerich.models` to satisfy aerich migration
        "aerich": {"models": ["aerich.models"]},
        "strategy": {"models": ["strategy.models"]},
        "dataplatform": {"models": ["dataplatform.models"]},
        "alpha": {"models": ["alpha.models"]},
        "task": {"models": ["task.models"]},
        "userservice": {"models": ["userservice.models"]},
    },
}
