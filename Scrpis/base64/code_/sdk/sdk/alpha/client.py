from typing import Type, Union

from sdk.auth import Credential
from sdk.httputils import HTTPClient

from .schemas import BacktestSchema, UpdateBacktestRequest
from .schemas.performance import CreateIndexPerformanceSchema, CreatePerformanceSchema, PerformanceSchema, IndexPerformanceSchema
from .settings import ALPHA_HOST


class AlphaClient:
    HTTPClient = HTTPClient(ALPHA_HOST)

    @classmethod
    def create_backtest_performance(
        cls: Type["AlphaClient"], task_id: str, request: Union[CreatePerformanceSchema, CreateIndexPerformanceSchema], credential: Credential
    ) -> PerformanceSchema:
        return PerformanceSchema(
            **cls.HTTPClient.post("/api/alpha/performance", params={"task_id": task_id}, payload=request.dict(), credential=credential)
        )

    @classmethod
    def create_index_backtest_performance(
        cls: Type["AlphaClient"], task_id: str, request: CreateIndexPerformanceSchema, credential: Credential
    ) -> IndexPerformanceSchema:
        return IndexPerformanceSchema(
            **cls.HTTPClient.post("/api/alpha/performance/index", params={"task_id": task_id}, payload=request.dict(), credential=credential)
        )

    @classmethod
    def update_backtest_by_task_id(cls: Type["AlphaClient"], task_id: str, request: UpdateBacktestRequest, credential: Credential) -> BacktestSchema:
        return BacktestSchema(
            **cls.HTTPClient.put("/api/alpha/backtest", params={"task_id": task_id}, payload=request.dict(), credential=credential),
        )

    @classmethod
    def get_access_alpha(cls: Type["AlphaClient"], credential: Credential):
        return cls.HTTPClient.get("/api/alpha/index/strategy", credential=credential)
