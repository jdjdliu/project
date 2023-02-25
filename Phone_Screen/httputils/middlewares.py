import json
import random
import time
from typing import Any, Callable, Coroutine

from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.responses import Response


def add_process_time_middleware(app: FastAPI) -> FastAPI:
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next: Callable[[Request], Coroutine[Any, Any, Response]]) -> Response:
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response

    return app
