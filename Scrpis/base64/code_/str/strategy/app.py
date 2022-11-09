import traceback

from fastapi import FastAPI, Request
from sdk.db import sql
from sdk.exception import HTTPException
from sdk.httputils import add_process_time_middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from .api import api
from .settings import API_PREFIX

app = FastAPI(
    openapi_url=f"{API_PREFIX}/openapi.json",
    docs_url=f"{API_PREFIX}/docs",
    redoc_url=f"{API_PREFIX}/redoc",
)

app.include_router(api, prefix=API_PREFIX)

app = sql.init_db(app, enabled_apps=["strategy"])
app = add_process_time_middleware(app)


@app.exception_handler(HTTPException)
async def http_exception_handle(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        {"code": exc.code, "message": exc.message},
        status_code=200,
    )


@app.exception_handler(Exception)
async def common_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    traceback.print_exception(type(exc), exc, exc.__traceback__)
    return JSONResponse(
        {"code": 500, "message": "Internal Server Error"},
        status_code=500,
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)
