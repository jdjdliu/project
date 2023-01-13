import time
import traceback

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

app = FastAPI()

"""HTTP 错误码"""
from typing import Any, Dict, Optional, Type


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


class HTTPExceptions:
    UNKNOWN = HTTPException(code=99999, message="未知错误，请联系工作人员")
    NO_ACCESS_TO_ID = HTTPException(code=10004, message="用户无权访问此id")
    ID_HAS_NO_RECORD = HTTPException(code=10005, message="此ID没有相关记录")
    INVALID_PARAMS = HTTPException(code=10000, message="参数错误")
    INTERNAL_SERVER_ERROR = HTTPException(code=10001, message="内部服务错误")
    NO_PERMISSION = HTTPException(code=10002, message="权限不足")
    EXISTED_NAME = HTTPException(code=10003, message="重复的名称")
    # sign-up errors, 100
    # login errors, 101
    UNAUTHORIZED = HTTPException(code=10100, message="未登录")
    INVALID_TOKEN = HTTPException(code=10101, message="无效的认证令牌", status_code=401)
    INVALID_LOGIN_TYPE = HTTPException(code=10102, message="无效的登录类型")
    BAD_USERNAME_OR_PASSWORD = HTTPException(code=10103, message="错误的用户名或密码")
    BAD_CAPTCHA = HTTPException(code=10104, message="验证码错误")
    # social account errors, 102
    # cash account errors, 103
    PUBLIC_KEY_EXPIRED = HTTPException(code=10300, message="验证密钥已过期")
    EMPTY_HARDWARE_INFO = HTTPException(code=10301, message="未获取到控件信息")
    WRONG_HARDWARE_INFO = HTTPException(code=10302, message="错误的控件信息")
    INVALID_ACCOUNT_PERMISSION = HTTPException(code=10303, message="账号尚未开通XBQ服务权限")
    # jwt errors, stars with 104
    # oauth errors, stars with 105
    # ldap errors, stars with 106

    # module errors, starts with 107
    INVALID_NAME = HTTPException(code=10700, message="模块命名不合法")
    EXISTED_NAME = HTTPException(code=10701, message="该名称已存在")
    CREATED_BY_OTHER = HTTPException(code=10702, message="该模块名已由其他用户使用，请使用其他名称命名您的模块")
    # 数据管理平台 108 开头
    TASK_NAME_ALREADY_EXISTS = HTTPException(code=108001, message="任务名称已存在, 请更换一个!")
    TASK_NOT_FUND = HTTPException(code=108002, message="任务不存在")
    FILE_NOT_FUND = HTTPException(code=108003, message="文件不存在")
    JOB_NOT_FUND = HTTPException(code=108004, message="子任务不存在")
    JOB_ALREADY_EXISTS = HTTPException(code=108005, message="子任务名(job_name)不能重复")
    MODULE_NOT_FUND = HTTPException(code=108006, message="模块未找到")
    DATAPLATFORM_TOKEN_ERROR = HTTPException(code=108007, message="交互接口token错误")
    # 因子平台 109 开头
    # member
    INVALID_MEMBER_TYPE = HTTPException(code=109001, message="无效的角色类型")
    # repo
    INVALID_REPO_WITH_CURRENT_USER = HTTPException(code=109100, message="当前用户提交的因子库不合法")
    # WARNING: do not use HTTPException(code=99998) above, it is used to define custom error
    # alpha 110开头
    INVALID_STATS = HTTPException(code=110001, message="状态不合法")
    # workspace 111开头
    INVALID_WORKSPACE_STATS = HTTPException(code=111001, message="工作台未开启, 克隆失败")
    WORPSACE_NOT_FUND =  HTTPException(code=111002, message="工作台未找到")
    # strategy 112开头
    STRATEGY_NOT_FUND = HTTPException(code=112001, message="策略不存在")
    # backtest 113开头
    BACKTEST_NOT_FUND = HTTPException(code=113001, message="回测信息找不到")

    @staticmethod
    def CUSTOM_ERROR(message: str) -> HTTPException:
        return HTTPException(code=99998, message=message)

    @classmethod
    def get_exception_by_code(cls: Type["HTTPExceptions"], code: Optional[int] = None) -> Optional[HTTPException]:
        if code is None:
            return None
        for exception in cls.__dict__.values():
            if isinstance(exception, HTTPException) and exception.code == code:
                return exception
        return None


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

@app.exception_handler(HTTPException)
async def http_exception_handle(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        {"code": exc.code, "message": exc.message},
        status_code=200,
    )


@app.exception_handler(Exception)
async def common_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    traceback.print_exception(type(exc), exc, exc.__traceback__)
    return JSONResponse(s
        {"code": 500, "message": "Internal Server Error"},
        status_code=500,
    )


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/items/{item_id}")
async def read_item(item_id: int):
    assert item_id != 0
    return {"item_id": item_id}