from fastapi import APIRouter

from .ping import router as ping_router
from .task import router as task_router

api = APIRouter()
api.include_router(task_router, prefix="/task", tags=["任务管理"])
api.include_router(ping_router, prefix="/ping", tags=["ping"])
