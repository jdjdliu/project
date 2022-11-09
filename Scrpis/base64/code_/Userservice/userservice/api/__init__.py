from fastapi import APIRouter

from .clone import router as clone_router
from .module import router as module_router
from .ping import router as ping_router
from .share import router as share_router
from .template import router as template_router
from .user import router as user_router

api = APIRouter()

api.include_router(ping_router, prefix="/ping", tags=["ping"])

api.include_router(user_router, prefix="/user", tags=["user"])
api.include_router(share_router, prefix="/share", tags=["share"])
api.include_router(module_router, prefix="/module", tags=["module"])
api.include_router(template_router, prefix="/template", tags=["template"])
api.include_router(clone_router, prefix="/clone", tags=["clone"])
