from sdk.db import sql

API_PREFIX = "/api/alpha"

TORTOISE_ORM = sql.build_config(sql.TORTOISE_ORM, ["alpha"])
