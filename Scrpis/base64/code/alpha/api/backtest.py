import base64
import json
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

from alpha.constants import AlphaBuildType, AlphaType, PerformanceSource
from alpha.models import Backtest, IndexPerformance, Performance
from alpha.schemas import (BacktestSchema, BulkDeletBacktestRequest,
                           CreateBacktestByCodeRequest, CreateBacktestRequest,
                           CreateIndexBacktestRequest, IndexBacktestSchema,
                           IndexPerformanceSchema, MyBacktest,
                           PerformanceSchema, UpdateBacktestRequest)
from alpha.utils.notebook import generate_notebook, index_generate_notebook
from fastapi import APIRouter, Depends, Path
from fastapi.responses import HTMLResponse
from sdk.auth import Credential, auth_required
from sdk.httputils import PagerSchema, QueryParamSchema, ResponseSchema
from sdk.share import ShareClient
from sdk.share.schemas import CreateNotebookShareRequest
from sdk.task import (TaskClient, TaskCreateRequest, TaskQueryRequest,
                      TaskSchema, TaskType)

router = APIRouter()


@router.get("", response_model=ResponseSchema[PagerSchema[MyBacktest]])
async def get_backtests(
    order_by: str = "-created_at",
    alpha_type: AlphaType = AlphaType.ALPHA,
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[MyBacktest]]:
    if order_by != "-created_at":
        backtest_ids = [backtest.id for backtest in await Backtest.filter(creator=credential.user.id, alpha_type=alpha_type).only("id").order_by("-created_at")]
        if alpha_type == AlphaType.ALPHA:
            pager = await PagerSchema.from_queryset(
                Performance.filter(
                    alpha_id__in=backtest_ids,
                    source=PerformanceSource.BACKTEST,
                ).order_by(order_by),
                page_params=query,
            )
        else:
            pager = await PagerSchema.from_queryset(
                IndexPerformance.filter(  # type: ignore
                    alpha_id__in=backtest_ids,
                    source=PerformanceSource.BACKTEST,
                ).order_by(order_by),
                page_params=query,
            )
        backtests = await Backtest.filter(id__in=[perf.alpha_id for perf in pager.items], alpha_type=alpha_type)
        backtests_mapping = {backtest.id: backtest for backtest in backtests}

        tasks_mapping: Dict[UUID, TaskSchema] = {
            task.id: task
            for task in TaskClient.get_tasks(
                TaskQueryRequest(
                    task_ids=[backtest.task_id for backtest in backtests],
                ),  # type: ignore
                credential=credential,
            )
        }
        items = [
            MyBacktest(
                backtest=BacktestSchema.from_orm(backtests_mapping[performance.alpha_id]),
                performance=performance,
                task=tasks_mapping[backtests_mapping[performance.alpha_id].task_id],
            )
            for performance in pager.items
        ]
    else:
        pager = await PagerSchema.from_queryset(Backtest.filter(creator=credential.user.id, alpha_type=alpha_type).order_by(order_by), page_params=query)
        if alpha_type == AlphaType.ALPHA:
            performances = await Performance.filter(
                alpha_id__in=[backtest.id for backtest in pager.items],
                source=PerformanceSource.BACKTEST,
            )
        else:
            performances = await IndexPerformance.filter(  # type: ignore #TODO: fix type
                alpha_id__in=[backtest.id for backtest in pager.items],
                source=PerformanceSource.BACKTEST,
            )
        performances_mapping = {performance.alpha_id: performance for performance in performances}

        tasks_mapping: Dict[UUID, TaskSchema] = {
            task.id: task
            for task in TaskClient.get_tasks(
                TaskQueryRequest(
                    task_ids=[backtest.task_id for backtest in pager.items],
                ),
                credential=credential,
            )
        }

        items = [
            MyBacktest(
                backtest=backtest,
                performance=performances_mapping.get(backtest.id, None),
                task=tasks_mapping[backtest.task_id],
            )
            for backtest in pager.items
        ]

    return ResponseSchema(
        data=PagerSchema(
            page=pager.page,
            page_size=pager.page_size,
            total_page=pager.total_page,
            items_count=pager.items_count,
            items=items,
        )
    )


@router.get("/{backtest_id}", response_model=ResponseSchema[MyBacktest])
async def get_backtest_detail(
    backtest_id: int,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[MyBacktest]:
    backtest = await Backtest.get(id=backtest_id, creator=credential.user.id)
    task = TaskClient.get_task_detail(task_id=backtest.task_id, credential=credential)
    task.notebook = base64.urlsafe_b64encode(task.notebook.encode()).decode()

    if  backtest.alpha_type == AlphaType.ALPHA:
        performance = await Performance.get_or_none(alpha_id=backtest.id)
        performance = PerformanceSchema.from_orm(performance) if performance else None  # type: ignore
        schema = BacktestSchema
    else:
        performance = await IndexPerformance.get_or_none(alpha_id=backtest.id)  # type: ignore #TODO: fix type
        performance = IndexPerformanceSchema.from_orm(performance) if performance else None  # type: ignore
        schema = IndexBacktestSchema  # type: ignore
    return ResponseSchema(
        data=MyBacktest(
            backtest=schema.from_orm(backtest),
            task=task,
            performance=performance,
        )
    )


@router.delete("/{backtest_id}", response_model=ResponseSchema[BacktestSchema])
async def delete_backtest_by_id(
    backtest_id: int = Path(...),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[BacktestSchema]:
    backtest = await Backtest.get(id=backtest_id, creator=credential.user.id)

    TaskClient.delete_task(backtest.task_id, credential=credential)
    await backtest.delete()

    return ResponseSchema(data=BacktestSchema.from_orm(backtest))


@router.delete("", response_model=ResponseSchema[List[BacktestSchema]])
async def bulk_delete_by_id(
    request: BulkDeletBacktestRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[BacktestSchema]]:
    backtests = await Backtest.filter(id__in=request.backtest_ids, creator=credential.user.id)
    for backtest in backtests:
        TaskClient.delete_task(backtest.task_id, credential=credential)
        await backtest.delete()
    return ResponseSchema(data=[BacktestSchema.from_orm(backtest) for backtest in backtests])


@router.post("/composition", response_model=ResponseSchema[BacktestSchema])
async def create_backtest_by_alpha_composition(
    request: CreateBacktestRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[BacktestSchema]:
    task = TaskClient.create_task(
        TaskCreateRequest(
            task_type=TaskType.ONCE,
            dag_id="alpha_backtest",
            notebook=generate_notebook(request),
        ),  # type: ignore
        credential,
    )
    backtest_meta = await Backtest.create(
        name=f"{request.name}__{datetime.now().isoformat(timespec='seconds')}",
        creator=credential.user.id,
        column=request.column,
        alphas=[alpha.dict() for alpha in request.alphas],
        parameter=request.parameter.dict(),
        expression=request.expression,
        dependencies=request.dependencies,
        alpha_type=request.alpha_type,
        product_type=request.product_type,
        build_type=AlphaBuildType.WIZARD,
        # TODO: 标记为向导式还是代码, 默认向导式
        task_id=task.id,
    )

    return ResponseSchema(data=BacktestSchema.from_orm(backtest_meta))


@router.post("/index_composition", response_model=ResponseSchema[BacktestSchema])
async def create_backtest_by_alpha_index_composition(
    request: CreateIndexBacktestRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[IndexBacktestSchema]:
    task = TaskClient.create_task(
        TaskCreateRequest(
            task_type=TaskType.ONCE,
            dag_id="alpha_backtest",
            notebook=index_generate_notebook(request),
        ),  # type: ignore
        credential,
    )
    backtest_meta = await Backtest.create(
        name=f"{request.name}__{datetime.now().isoformat(timespec='seconds')}",
        creator=credential.user.id,
        column=request.column,
        alphas=[alpha.dict() for alpha in request.alphas],
        parameter=request.parameter.dict(),
        expression=request.expression,
        dependencies=request.dependencies,
        alpha_type=request.alpha_type,
        product_type=request.product_type,
        build_type=AlphaBuildType.WIZARD,
        # TODO: 标记为向导式还是代码, 默认向导式
        task_id=task.id,
    )
    return ResponseSchema(data=IndexBacktestSchema.from_orm(backtest_meta))


@router.post("/code", response_model=ResponseSchema[BacktestSchema])  # 提交任务
async def create_backtest_by_code(
    request: CreateBacktestByCodeRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[BacktestSchema]:

    task = TaskClient.create_task(
        TaskCreateRequest(
            task_type=TaskType.ONCE,
            dag_id="alpha_backtest",
            notebook=request.notebook,
        ),  # type: ignore
        credential,
    )

    backtest_meta = await Backtest.create(
        name=f"{request.name}__{datetime.now().isoformat(timespec='seconds')}",
        creator=credential.user.id,
        alpha_type=request.alpha_type,
        product_type=request.product_type,
        build_type=AlphaBuildType.CODED,
        task_id=task.id,
    )

    return ResponseSchema(data=BacktestSchema.from_orm(backtest_meta))


@router.put("", response_model=ResponseSchema[BacktestSchema])
async def update_backtest_by_task_id(
    task_id: UUID,
    request: UpdateBacktestRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[BacktestSchema]:
    backtest = await Backtest.get(task_id=task_id, creator=credential.user.id)
    not_null_request = {key: value for key, value in request.dict().items() if value}
    if backtest.expression:
        not_null_request["expression"] = backtest.expression
    if backtest.column:
        not_null_request["column"] = backtest.column

    backtest = await backtest.update_from_dict(not_null_request)
    await backtest.save()
    return ResponseSchema(data=BacktestSchema.from_orm(backtest))


@router.get("/{backtest_id}/notebook", response_class=HTMLResponse)
async def get_user_alpha_shared_html(
    backtest_id: int,
    keep_source: Optional[bool] = True,
    keep_button: Optional[bool] = True,
    credential: Credential = Depends(auth_required()),
) -> HTMLResponse:
    backtest = await Backtest.get(id=backtest_id, creator=credential.user.id)
    task = TaskClient.get_task_detail(task_id=backtest.task_id, credential=credential)

    shared_html = ShareClient.render_notebook(
        CreateNotebookShareRequest(
            name=f"backtest__{task.id}",
            notebook=task.notebook,
            keep_source=keep_source,
            keep_button=keep_button,
        ),  # type: ignore
        credential=credential,
    )
    return HTMLResponse(content=shared_html)
