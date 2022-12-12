from datetime import datetime
from typing import List, Type
from uuid import UUID

from sdk.auth import Credential
from sdk.httputils import HTTPClient

from .schemas import (
    BacktestPerformanceSchema,
    StrategyBacktestSchema,
    StrategyDailySchema,
    StrategyPerformanceSchema,
    StrategySchema,
    UpdateBacktestParamterRequest,
    UpdateStrategyRequest,
    CreateBacktestPerformanceRequest,
)
from .settings import STRATEGY_HOST



class StrategyClient:

    HttpClient = HTTPClient(STRATEGY_HOST)

    @classmethod
    def get_strategy_by_id(cls: Type["StrategyClient"], strategy_id: UUID, credential: Credential) -> StrategySchema:
        return StrategySchema(**cls.HttpClient.get(f"/api/strategy/internal/strategy/{strategy_id}", credential=credential))

    @classmethod
    def get_performance_by_strategy_id(cls: Type["StrategyClient"], strategy_id: UUID, credential: Credential) -> StrategyPerformanceSchema:
        return StrategyPerformanceSchema(**cls.HttpClient.get(f"/api/strategy/internal/performance/{strategy_id}", credential=credential))

    @classmethod
    def delete_strategy_daily_by_rundate(
        cls: Type["StrategyClient"], strategy_id: UUID, run_date: datetime, credential: Credential
    ) -> StrategyDailySchema:
        return StrategyDailySchema(
            **cls.HttpClient.delete(
                "/api/strategy/internal/strategy/daily",
                payload={"run_date": run_date, "strategy_id": strategy_id},
                credential=credential,
            )
        )

    @classmethod
    def create_strategy_daily(cls: Type["StrategyClient"], params: StrategyDailySchema, credential: Credential) -> StrategyDailySchema:
        p = params.dict()
        p['strategy_id'] =  str(p['strategy_id'])
        p['performance_id'] = str(p['performance_id'])

        return StrategyDailySchema(
            **cls.HttpClient.post(
                f"/api/strategy/internal/strategy/daily",
                payload=p,
                credential=credential,
            )
        )

    @classmethod
    def create_or_update_strategy_performance(
        cls: Type["StrategyClient"],
        params: StrategyPerformanceSchema,
        credential: Credential,
    ) -> StrategyPerformanceSchema:
        p = params.dict()
        p['strategy_id'] =  str(p['strategy_id'])
        p['id'] = str(p['id'])
        return StrategyPerformanceSchema(
            **cls.HttpClient.put(
                f"/api/strategy/internal/performance",
                payload=p,
                credential=credential,
            )
        )

    @classmethod
    def update_params_to_backtest(
        cls: Type["StrategyClient"],
        params: UpdateBacktestParamterRequest,
        credential: Credential,
    ) -> StrategyBacktestSchema:
        return StrategyBacktestSchema(
            **cls.HttpClient.put(
                f"/api/strategy/internal/backtest/parameter",
                payload=params.dict(),
                credential=credential,
            )
        )

    @classmethod
    def update_backtest_performance(
        cls: Type["StrategyClient"],
        params: CreateBacktestPerformanceRequest,
        credential: Credential,
    ) -> BacktestPerformanceSchema:
        return BacktestPerformanceSchema(
            **cls.HttpClient.post(
                f"/api/strategy/internal/backtest/performance",
                payload=params.dict(),
                credential=credential,
            )
        )

    @classmethod
    def get_all_strategy_daily(
        cls: Type["StrategyClient"],
        strategy_id: UUID,
        run_date: datetime,
        credential: Credential,
    ) -> List[StrategyDailySchema]:
        if isinstance(run_date, datetime):
            run_date = run_date.strftime("%Y-%m-%d")
        return [
            StrategyDailySchema(**i)
            for i in cls.HttpClient.get(
                f"/api/strategy/internal/strategy/{strategy_id}/daily?run_date={run_date}",
                credential=credential,
            )
        ]

    @classmethod
    def create_or_update_strategy(
        cls: Type["StrategyClient"],
        params: UpdateStrategyRequest,
        credential: Credential,
    ) -> StrategySchema:
        return StrategySchema(
            **cls.HttpClient.put(
                f"/api/strategy/internal/strategy",
                payload=params.dict(),
                credential=credential,
            )
        )
