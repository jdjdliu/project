from typing import Any, Dict

from fastapi import APIRouter
from sdk.db.redis import REDIS_CLUSTER_MODE, REDIS_URL, RedisClient
from sdk.db.sql import test_connections
from sdk.kubernetes import configuration, refresh_api_key_hook
from userservice.utils.sync_user import sync_user_info

router = APIRouter()


@router.get("")
async def ping() -> Dict[str, Any]:
    refresh_api_key_hook(configuration)

    client = RedisClient.get_client(REDIS_URL, REDIS_CLUSTER_MODE)
    client.set("AIWE_QUANTPLATFORM:KUBERNETES:API_KEY", configuration.api_key["authorization"])

    await sync_user_info()
    await test_connections()
    return {"message": "pong"}
