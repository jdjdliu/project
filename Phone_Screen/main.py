import traceback
from typing import Any, Dict, Optional, Type

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

from api import api
from httputils import add_process_time_middleware

app = FastAPI(
    openapi_url="/Phone_Screen/openapi.json",
    docs_url="/Phone_Screen/docs",
    redoc_url="/Phone_Screen/redoc",
)
app = add_process_time_middleware(app)

app.include_router(api, prefix="/Phone_Screen")


class HTTPException(Exception):
    def __init__(
        self: "HTTPException",
        code: int,
        message: str,
        context: Optional[Dict[Any, Any]] = None,
        status_code: Optional[int] = None,
    ) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        self.context = context or dict()

    def clone(self: "HTTPException") -> "HTTPException":
        return HTTPException(
            code=self.code,
            message=self.message,
            context=self.context,
            status_code=self.status_code,
        )

    def set_status_code(self: "HTTPException", status_code: int) -> "HTTPException":
        self.status_code = status_code
        return self

    def set_context(self: "HTTPException", context: Optional[Dict[Any, Any]] = None) -> "HTTPException":
        self.context.update(context or dict())
        return self

@app.exception_handler(HTTPException)
async def http_exception_handle(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        {"code": 200, "message": exc.message},
        status_code=200,
    )


@app.exception_handler(Exception)
async def common_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    traceback.print_exception(type(exc), exc, exc.__traceback__)
    return JSONResponse(
        {"code": 500, "message": "Internal Server Error"},
        status_code=500,
    )