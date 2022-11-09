import copy
from typing import List, Optional

from tortoise import Tortoise
from tortoise.backends.mysql.client import MySQLClient

from .settings import TORTOISE_CONFIG_TYPE, TORTOISE_ORM


def build_config(config: TORTOISE_CONFIG_TYPE, enabled_apps: Optional[List[str]] = None) -> TORTOISE_CONFIG_TYPE:
    if enabled_apps is not None:
        config = copy.deepcopy(config)
        for app_name in list(config["apps"].keys()):
            if app_name not in enabled_apps and app_name != "aerich":
                config["apps"].pop(app_name, None)
    else:
        config = copy.deepcopy(config)

    return config


async def test_connections(enabled_apps: Optional[List[str]] = None) -> None:
    for conn_name in TORTOISE_ORM["connections"].keys():
        try:
            connection: MySQLClient = Tortoise.get_connection(conn_name)
            for _ in range(connection.pool_maxsize):
                async with connection.acquire_connection() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1;")
        except Exception as e:
            pass


if __name__ == "__main__":
    import asyncio

    async def test() -> None:
        enabled_apps = ["aerich", "alpha"]
        await Tortoise.init(build_config(TORTOISE_ORM, enabled_apps))
        await test_connections()

    loop = asyncio.get_event_loop()
    loop.run_until_complete
    loop.run_until_complete(test())
