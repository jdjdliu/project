from fastapi import APIRouter

from .audit import router as audit_router
from .backtest import router as backtest_router
from .collection import router as collection_router
from .dashboard import router as dashboard_router
from .internal import router as interbal_router
from .member import router as member_router
from .ping import router as ping_router
from .repo import router as repo_router
from .strategy import router as strategy_router
from .share import router as share_router

api = APIRouter()

api.include_router(ping_router, prefix="/ping", tags=["ping"])
api.include_router(backtest_router, prefix="/backtest", tags=["backtest"])
api.include_router(member_router, prefix="/member", tags=["repo & member"])
api.include_router(repo_router, prefix="/repo", tags=["repo & member"])
api.include_router(audit_router, prefix="/audit", tags=["audit"])
api.include_router(strategy_router, prefix="/strategy", tags=["strategy"])
api.include_router(collection_router, prefix="/collection", tags=["collection"])
api.include_router(interbal_router, prefix="/internal", tags=["internal"])
api.include_router(dashboard_router, prefix="/dashboard", tags=["dashboard"])
api.include_router(share_router, prefix="/share", tags=["share"])
