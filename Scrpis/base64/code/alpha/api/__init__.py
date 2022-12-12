from fastapi import APIRouter

from .alpha import router as alpha_router
from .audit import router as audit_router
from .backtest import router as backtest_router
from .catalog import router as catalog_router
from .collection import router as collection_router
from .dashboard import router as dashboard_router
from .index import router as index_router
from .member import router as member_router
from .performance import router as performance_router
from .ping import router as ping_router
from .repo import router as repo_router

api = APIRouter()

# ping: keep db connection
api.include_router(ping_router, prefix="/ping", tags=["ping"])
api.include_router(index_router, prefix="/index", tags=["index"])
api.include_router(backtest_router, prefix="/backtest", tags=["backtest"])
api.include_router(alpha_router, prefix="/alpha", tags=["alpha"])
api.include_router(collection_router, prefix="/collection", tags=["collection"])

api.include_router(dashboard_router, prefix="/dashboard", tags=["dashboard"])
api.include_router(performance_router, prefix="/performance", tags=["performance"])

api.include_router(member_router, prefix="/member", tags=["repo & member"])
api.include_router(repo_router, prefix="/repo", tags=["repo & member"])
api.include_router(audit_router, prefix="/audit", tags=["audit"])

api.include_router(catalog_router, prefix="/catalog", tags=["catalog"])
