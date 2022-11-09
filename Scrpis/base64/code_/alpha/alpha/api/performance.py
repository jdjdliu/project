from typing import Union
from uuid import UUID

from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema

from alpha.constants import PerformanceSource
from alpha.models import Alpha, Backtest, IndexPerformance, Performance
from alpha.schemas import CreateIndexPerformanceSchema, CreatePerformanceSchema, IndexPerformanceSchema, PerformanceSchema

router = APIRouter()


# ! 这个接口要对用户屏蔽
@router.post("", response_model=ResponseSchema[PerformanceSchema])
async def update_backtest_performance(
    task_id: UUID,
    request: CreatePerformanceSchema,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PerformanceSchema]:
    obj: Union[Alpha, Backtest]

    if request.source == PerformanceSource.BACKTEST:
        obj = await Backtest.get(task_id=task_id, creator=credential.user.id)
    else:
        obj = await Alpha.get(task_id=task_id, creator=credential.user.id)

    await Performance.filter(alpha_id=obj.id, source=request.source, run_datetime=request.run_datetime).delete()
    performance = await Performance.create(alpha_id=obj.id, **request.dict())

    return ResponseSchema(data=PerformanceSchema.from_orm(performance))


# ! 这个接口要对用户屏蔽
@router.post("/index", response_model=ResponseSchema[IndexPerformanceSchema])
async def update_index_backtest_performance(
    task_id: UUID,
    request: CreateIndexPerformanceSchema,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[IndexPerformanceSchema]:
    obj: Union[Alpha, Backtest]

    if request.source == PerformanceSource.BACKTEST:
        obj = await Backtest.get(task_id=task_id, creator=credential.user.id)
    else:
        obj = await Alpha.get(task_id=task_id, creator=credential.user.id)

    await IndexPerformance.filter(alpha_id=obj.id, source=request.source, run_datetime=request.run_datetime).delete()
    index_performance = await IndexPerformance.create(alpha_id=obj.id, **request.dict())

    return ResponseSchema(data=IndexPerformanceSchema.from_orm(index_performance))
