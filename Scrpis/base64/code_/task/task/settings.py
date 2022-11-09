from sdk.common import OSEnv
from sdk.db import sql

TORTOISE_ORM = sql.build_config(sql.TORTOISE_ORM, ["task"])

AIFLOW_URL = OSEnv.str("AIFLOW_URL", "http://cmbprd.bigquant.com/aiflow/web/api/v1")
AIFLOW_AUTH_USER = OSEnv.str("AIFLOW_AUTH_USER", "aiflow")
AIFLOW_AUTH_PASSWORD = OSEnv.str("AIFLOW_AUTH_PASSWORD", "abc123!")


DEBUG_MODE = OSEnv.bool("DEBUG_MODE", False, description="是否是debug模式")
