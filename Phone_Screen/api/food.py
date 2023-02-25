import json
from typing import Any, Dict

import requests
from fastapi import APIRouter, Path

from schemas import FoodSchema, ResponseSchema
from settings import file_url

router = APIRouter()

# 1题
@router.get("")
async def post_to_ip() -> ResponseSchema:
    with open(file_url, "r", encoding="UTF-8") as f:  # 模拟接口获得数据
        data = json.loads(f.read())
    for i in data:
        food_review = FoodSchema(
            id=i["id"],
            sentence=i["text"],
            stars=i["stars"],
            date=i["date"],
        )
        requests.post(url="http://<ip-address>/one-sentence-food-review", data=food_review)
    return ResponseSchema(data="post_success")


# 1题
@router.get("/food_print")
async def food_print() -> ResponseSchema:
    with open(file_url, "r", encoding="UTF-8") as f:  # 模拟接口获得数据
        data = json.loads(f.read())
    # 按时间，星级排序
    a = sorted(data, key=lambda x: (x["date"], x["stars"]), reverse=True)
    for i in a:
        print(i["text"])
    return ResponseSchema(data="print success")


@router.get("/food/{id}")
async def food(
    id: int = Path(...),
) -> ResponseSchema[FoodSchema]:
    with open(file_url, "r", encoding="UTF-8") as f:  # 模拟接口获得数据
        data = json.loads(f.read())
    data = data[id]
    data = FoodSchema(
        id=data["id"],
        sentence=data["text"],
        stars=data["stars"],
        date=data["date"],
    )
    return ResponseSchema(data=data)
