from fastapi import APIRouter

from .cache import router as cache_router
from .module import router as module_router

router = APIRouter()

router.include_router(cache_router, prefix="/cache")
router.include_router(module_router, prefix="/module")
