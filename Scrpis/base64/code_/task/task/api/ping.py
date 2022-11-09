from typing import Any, Dict

from fastapi import APIRouter

from sdk.db.sql import test_connections

router = APIRouter()


@router.get("")
async def ping() -> Dict[str, Any]:
    await test_connections()
    return {"message": "pong"}
