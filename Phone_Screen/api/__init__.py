from fastapi import APIRouter

from .food import router as food_router

api = APIRouter()


api.include_router(food_router, prefix="/food", tags=["food"])