import json
from datetime import datetime
from typing import List, Union
from uuid import UUID

import requests
from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.httputils.schemas import ResponseSchema
from sdk.strategy import CreateStrategyBacktestFromWizardRequest, StrategyBacktestSchema, StrategyParameter, StrategyPerformanceSchema, StrategySchema, CreateStrategyDailySchema, DeleteDailyRequest
from sdk.strategy.constants import ProductType

from strategy.models import BacktestPerformance, Strategy, StrategyBacktest, StrategyDaily, StrategyPerformance
from strategy.schemas import (
    BacktestPerformanceSchema,
    StrategyDailySchema,
    StrategyPerformanceSchema,
    UpdateBacktestParamterRequest,
    UpdateStrategyRequest,
    CreateBacktestPerformanceRequest,
    CreateStrategyDailySchema,
)
from strategy.utils import generate_notebook

router = APIRouter()

#! 此文件为内部接口


@router.get("/strategy/{strategy_id}")
async def get_strategy_by_id(
    strategy_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategySchema]:
    strategy = await Strategy.get(id=strategy_id)
    return ResponseSchema(data=StrategySchema.from_orm(strategy))


@router.put("/strategy")
async def update_or_create_strategy(
    params: UpdateStrategyRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategySchema]:

    stra = await Strategy.get_or_none(id=params.strategy_id)
    strategy = StrategySchema.from_orm(stra)
    if not stra:
        # create it
        #!! maybe lack attribute in StrategyParameter
        strategy_params = StrategyParameter(**params.parameter.dict())
        schema = CreateStrategyBacktestFromWizardRequest(
            name=params.name,
            description=params.description,
            parameter=strategy_params,
            product_type=ProductType.STOCK,
        )
        resp = requests.post("http://127.0.0.1:8000/api/strategy/backtest/wizard", json={"request": schema, "credential": credential})
        strategy = StrategySchema(**json.loads(resp.text).get("data", {}))

    return ResponseSchema(data=strategy)


@router.get("/performance/{strategy_id}")
async def get_performance_by_strategy_id(
    strategy_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyPerformanceSchema]:
    performance = await StrategyPerformance.get(strategy_id=strategy_id)
    return ResponseSchema(data=StrategyPerformanceSchema.from_orm(performance))


@router.post("/strategy/daily")
async def create_strategy_daily(
    request: CreateStrategyDailySchema,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyDailySchema]:
    strategy_daily = await StrategyDaily.create(**request.dict())
    return ResponseSchema(data=StrategyDailySchema.from_orm(strategy_daily))


@router.get("/strategy/{strategy_id}/daily")
async def get_all_strategy_daily(
    strategy_id: UUID,
    run_date: str,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[StrategyDailySchema]]:
    _run_date = datetime.strptime(run_date, "%Y-%m-%d")
    daily_perfs = await StrategyDaily.filter(strategy_id=strategy_id, run_date__lte=_run_date).order_by("created_at")
    return ResponseSchema(data=[StrategyDailySchema.from_orm(i) for i in daily_perfs])

@router.delete("/strategy/{strategy_id}/daily")
async def delete_strategy_daily_by_run_date(
    query:DeleteDailyRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyDailySchema]:
    strategy_daily = await StrategyDaily.filter(strategy_id=query.strategy_id, run_date=query.run_date)

    for i in strategy_daily:
        await i.delete()

    return ResponseSchema(data=[StrategyDailySchema.from_orm(i) for i in strategy_daily])


@router.put("/performance")
async def create_or_update_performace(
    request: StrategyPerformanceSchema,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyPerformanceSchema]:
    performance = await StrategyPerformance.get_or_none(strategy_id=request.strategy_id)
    if not performance:
        performance = await StrategyPerformance.create(**request.dict())
    else:
        _update_dict = request.dict()
        _update_dict.pop("id")
        _update_dict.pop("strategy_id")
        performance.update_from_dict(_update_dict)
        await performance.save()
    return ResponseSchema(data=StrategyPerformanceSchema.from_orm(performance))


@router.put("/backtest/parameter")
async def update_backtest_permter(
    request: UpdateBacktestParamterRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyBacktestSchema]:
    backtest = await StrategyBacktest.get(id=request.backtest_id)
    if backtest.parameter:
        for k, v in request.paramter.dict().items():
                try:
                    backtest.parameter[k] = v
                except KeyError:
                    continue  
    else:
        backtest.parameter = request.paramter.dict()
    await backtest.save()
    return ResponseSchema(data=StrategyBacktestSchema.from_orm(backtest))


@router.post("/backtest/performance")
async def create_backtest_performance(
    request: CreateBacktestPerformanceRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[ResponseSchema]:
    await BacktestPerformance.filter(backtest_id=request.backtest_id).delete()

    performance = await BacktestPerformance.create(**request.dict())
    performance.daily_data = []  # type: ignore
    return ResponseSchema(data=BacktestPerformanceSchema.from_orm(performance))
