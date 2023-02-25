from fastapi import APIRouter

from .food import router as food_router
from .test import router as test_router

api = APIRouter()


api.include_router(food_router, prefix="/food", tags=["food"])
api.include_router(test_router, prefix="/test", tags=["test"])