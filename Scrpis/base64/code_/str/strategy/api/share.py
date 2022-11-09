import base64
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from sdk.auth import Credential, auth_required
from sdk.exception import HTTPException, HTTPExceptions
from sdk.httputils import ResponseSchema
from sdk.task import TaskClient
from strategy.models import Share, Strategy, StrategyBacktest, StrategyDaily, StrategyPerformance
from strategy.schemas import MyStrategy, SharedRecord, StrategyDailySchema, StrategyPerformanceSchema, StrategySchema, StrategyShareCreateRequest
from strategy.utils import get_benchmark_profit

router = APIRouter()


@router.post("/share", response_model=ResponseSchema[UUID])
async def create_share(
    request: StrategyShareCreateRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[UUID]:

    share, _ = await Share.get_or_create(
        record_id=request.record_id,
        record_type=request.record_type,
        share_performance=request.share_performance,
        share_notebook=request.share_notebook,
    )

    return ResponseSchema(data=share.id)


@router.get("", response_model=ResponseSchema[SharedRecord])
async def get_shared_record(
    share_id: UUID = Query(..., description="分享记录id"),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[SharedRecord]:
    try:
        share_record = await Share.get_or_none(id=share_id)
    except Exception:
        raise HTTPExceptions.ID_HAS_NO_RECORD
    if not share_record:
        raise HTTPExceptions.ID_HAS_NO_RECORD
    strategy = await Strategy.get_or_none(id=share_record.record_id)
    if not strategy:
        raise HTTPExceptions.STRATEGY_NOT_FUND

    try:
        task = TaskClient.get_task_detail(task_id=strategy.task_id, credential=credential)
        share_notebook = task.notebook
    except Exception as e:
        raise HTTPException(code=1, message=str(e))

    try:
        performance = await StrategyPerformance.get(strategy_id=share_record.record_id)
    except Exception as e:
        raise HTTPException(code=2, message=str(e))
    try:
        dailys_schema = [StrategyDailySchema.from_orm(i) for i in await StrategyDaily.filter(strategy_id=share_record.record_id).order_by("-run_date").all()]
    except Exception as e:
        raise HTTPException(code=3, message=str(e))
    try:
        last_daily = dailys_schema[0]
    except Exception as e:
        raise HTTPException(code=4, message=str(e))
    try:
        benchmark_profit = get_benchmark_profit(last_daily.trading_days, performance.benchmark_cum_return_plot)
    except Exception as e:
        raise HTTPException(code=5, message=str(e))
    try:
        my_strategy = MyStrategy(  # type: ignore
            strategy=StrategySchema.from_orm(strategy),
            performance=StrategyPerformanceSchema.from_orm(performance) if performance else None,
            daily=dailys_schema,
            benchmark_profit=benchmark_profit,
            task=task,
            notebook=base64.urlsafe_b64encode(task.notebook.encode()).decode(),
            overall_position=dailys_schema[0].portfolio["pv"] / (dailys_schema[0].portfolio["pv"] + dailys_schema[0].cash),
        )
    except Exception as e:
        raise HTTPException(code=6, message=str(e))
    try:
        return ResponseSchema(
            data=SharedRecord(
                record_id=share_record.record_id,
                record_type=share_record.record_type,
                notebook=share_notebook if share_record.share_notebook else None,
                strategy=my_strategy if share_record.share_performance else None,
            )
        )
    except Exception as e:
        raise HTTPException(code=7, message=str(e))