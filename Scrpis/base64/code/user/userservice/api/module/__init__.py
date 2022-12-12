from fastapi import APIRouter

from .v1 import router as v1_router
from .v2 import router as v2_router

router = APIRouter()

router.include_router(v1_router, prefix="/v1", tags=["module/v1"])
router.include_router(v2_router, prefix="/v2", tags=["module/v2"])
