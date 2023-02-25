import asyncio
from fastapi import APIRouter
from schemas import ResponseSchema


router = APIRouter()

@router.get("/bigdata")
async def post_to_ip() -> ResponseSchema:
    import time
    await asyncio.sleep(5)  # 异步test
    return ResponseSchema(data="post_success") 


@router.get("/ping")
async def post_to_ip() -> ResponseSchema:
    return ResponseSchema(data="pong")
