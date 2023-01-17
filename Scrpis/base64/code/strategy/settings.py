from sdk.db import sql

API_PREFIX = "/api/strategy"

TORTOISE_ORM = sql.build_config(sql.TORTOISE_ORM, ["strategy"])
