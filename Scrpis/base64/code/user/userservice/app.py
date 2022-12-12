"""Entrypoint, middleware and global exception handler includes"""
import os

from fastapi import FastAPI, Request
from sdk.db import sql
from sdk.exception import HTTPException
from sdk.httputils import add_log_middleware, add_process_time_middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from .api import api
from .settings import API_PREFIX

app = FastAPI(
    openapi_url=f"{API_PREFIX}/openapi.json",
    docs_url=f"{API_PREFIX}/docs",
    redoc_url=f"{API_PREFIX}/redoc",
)

app = sql.init_db(app, enabled_apps=["userservice"])

app.include_router(api, prefix=API_PREFIX)
app = add_process_time_middleware(app)
app = add_log_middleware(
    app,
    host="aiwe-quantplatform-web.paasoa.cmbchina.cn",
    business_id=os.getenv("BUSINESS_ID", "LX58AiweUserservice"),
    deploy_unit_id=os.getenv("DEPLOY_UNIT_ID", "LX58_AiweUserservice"),
    service_unit_id=os.getenv("SERVICE_UNIT_ID")
)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        {"code": exc.code, "message": exc.message},
        status_code=200,
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)
