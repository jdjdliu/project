import json
import random
import time
from datetime import datetime
from typing import Any, Callable, Coroutine
from uuid import uuid4

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


def add_log_middleware(app: FastAPI, host: str, business_id: str, deploy_unit_id: str, service_unit_id: str) -> FastAPI:
    @app.middleware("http")
    async def log(request: Request, call_next: Callable[[Request], Coroutine[Any, Any, Response]]) -> Response:
        start_time = time.time()
        now = datetime.now()

        response = await call_next(request)

        log_string = json.dumps({
            "_CMB_LOG_SPEC_VERSION": "2.0",
            "method": "unknown-0",
            "errorCode": "success",
            "type": "BASETYPE",
            "level": "INFO",
            "tid": "unknowns-0",
            "ts": now.strftime("%Y-%m-%dT%H:%M:%S") + f"000000{str(now.microsecond)[:3]}Z",
            "businessId": business_id,
            # "content": traceback.format_exception(type(exc), exc, exc.__traceback__),
            "content": request.url.path,
        }, ensure_ascii=False)

        print(log_string)

        log_string = json.dumps({
            "businessId": business_id,
            "traceId": uuid4().hex,
            "spanId": "".join([random.choice('0123456789abcdef') for _ in range(16)]),
            "parentSpanId": "0",
            "timestamp": int(time.time() * 1000),
            "deployUnitId": deploy_unit_id,
            "serviceUnitId": service_unit_id,
            "host": host,
            "api": request.url.path,
            "responseTime": (time.time() - start_time) * 1000,
            "returnCode": "SUC0000" if response.status_code == 200 else "IWE0000",
            "callStack": [],
            "tags": None,
        }, ensure_ascii=False)

        print(f'[[tracing]] {log_string} [[tracing]]')

        return response

    return app