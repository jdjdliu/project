from fastapi import APIRouter

from .module import router as module_router

router = APIRouter()

router.include_router(module_router, prefix="/module")
