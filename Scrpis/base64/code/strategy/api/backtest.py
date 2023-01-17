import base64
from typing import Dict, List
from uuid import UUID
from datetime import datetime

from fastapi import APIRouter, Depends, Path
from fastapi.responses import HTMLResponse
from sdk.alpha import AlphaClient
from sdk.auth import Credential, auth_required
from sdk.httputils.schemas import PagerSchema, QueryParamSchema, ResponseSchema
from sdk.share import ShareClient
from sdk.share.schemas import CreateNotebookShareRequest
from sdk.task import TaskClient, TaskCreateRequest, TaskQueryRequest, TaskSchema, TaskType
from strategy.constants import StrategyBuildType
from strategy.models import BacktestPerformance, StrategyBacktest
from strategy.schemas import (
    ViewStockRequest,
    BacktestPerformanceSchema,
    BulkDeletBacktestRequest,
    CreateStrategyBacktestFromWizardRequest,
    MyBacktest,
    MyBacktestPerformanceSchema,
    StrategyBacktestSchema,
    UpdateBacktestRequest,
    CreateBacktestByCodeRequest,
)
from strategy.utils import generate_notebook,ViewStock,generate_filter_cond_composition

router = APIRouter()


@router.get("", response_model=ResponseSchema[PagerSchema[MyBacktest]])
async def get_backtests(
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[MyBacktest]]:
    pager = await PagerSchema.from_queryset(StrategyBacktest.filter(creator=credential.user.id).order_by("-created_at"), page_params=query)
    performances = (
        await BacktestPerformance.filter(
            backtest_id__in=[backtest.id for backtest in pager.items],
        )
        .only("backtest_id", "return_ratio", "annual_return_ratio", "max_drawdown", "sharp")
        .all()
    )
    # 优化列表页输出
    performances_mapping = {}
    for performance in performances:
        performance.daily_data = []
        performances_mapping[performance.backtest_id] = performance

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
            performance=MyBacktestPerformanceSchema.from_orm(performances_mapping.get(backtest.id)) if performances_mapping.get(backtest.id) else None,
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

@router.get("/stock_date", response_model=ResponseSchema)
async def view_date(
    start_date: str,
    end_date: str,
    credential: Credential = Depends(auth_required()),
):
    from sdk.datasource import DataSource
    date_ = DataSource("all_trading_days").read(start_date=start_date, end_date=end_date) if DataSource("all_trading_days") else None
    if date_ is None:
        date = []
    else:
        date = date_[date_.country_code=='CN'][['date']]['date'].to_list()
    return ResponseSchema(data=date)


@router.get("/{backtest_id}", response_model=ResponseSchema[MyBacktest])
async def get_backtest_detail(
    backtest_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[MyBacktest]:
    backtest = await StrategyBacktest.get(id=backtest_id, creator=credential.user.id)
    task = TaskClient.get_task_detail(task_id=backtest.task_id, credential=credential)
    notebook = base64.urlsafe_b64encode(task.notebook.encode()).decode()  # type: ignore

    performance = await BacktestPerformance.get_or_none(backtest_id=backtest.id)
    perf_schema = BacktestPerformanceSchema.from_orm(performance) if performance else None

    return ResponseSchema(
        data=MyBacktest(
            backtest=StrategyBacktestSchema.from_orm(backtest),
            task=task,
            performance=perf_schema,
            notebook=notebook,
        )
    )


@router.post("/stock", response_model=ResponseSchema[PagerSchema])
async def view_stock(
    request: ViewStockRequest,
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema]:
    parameter = request.dict()
    parameter.pop("filter_cond", None)
    parameter.pop("sorter", None)
    try:
        view_stock = ViewStock(**parameter,filter_conde_composition=generate_filter_cond_composition(request.filter_cond)).view_stock()
        if request.sorter:
            if request.sorter["order"] == "ascend":
                view_stock.sort_values(by=request.sorter["columnKey"], inplace=True, ascending=True)
            elif request.sorter["order"] == "descend":
                view_stock.sort_values(by=request.sorter["columnKey"], inplace=True, ascending=False)
        else:
            view_stock.sort_values(by="总得分", inplace=True, ascending=False)
        view_stock.reset_index(drop=True,inplace=True)
        res = view_stock.to_dict(orient="records")
        pager = await PagerSchema.from_list(res,page_params=query)
        return ResponseSchema(
            data=PagerSchema(
                page=pager.page,
                page_size=pager.page_size,
                total_page=pager.total_page,
                items_count=pager.items_count,
                items=pager.items,
            )
        )
    except:
        return ResponseSchema(
            data=PagerSchema(
                page=query.page,
                page_size=query.page_size,
                total_page=0,
                items_count=0,
                items=[],
            )
        )


@router.post("/wizard", response_model=ResponseSchema[StrategyBacktestSchema])
async def create_strategy_backtest_from_wizard(
    request: CreateStrategyBacktestFromWizardRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyBacktestSchema]:

    task = TaskClient.create_task(
        TaskCreateRequest(
            task_type=TaskType.ONCE,
            dag_id="strategy_backtest",
            notebook=generate_notebook(request),
        ),
        credential,
    )

    strategy_backtest = await StrategyBacktest.create(
        creator=credential.user.id,
        name=f"{request.name}__{datetime.now().isoformat(sep=' ',timespec='seconds')}",
        description=request.description,
        parameter=request.parameter.dict(),
        product_type=request.product_type,
        build_type=StrategyBuildType.WIZARD,
        task_id=task.id
    )
    return ResponseSchema(data=StrategyBacktestSchema.from_orm(strategy_backtest))


@router.post("/code", response_model=ResponseSchema[StrategyBacktestSchema])  # 提交任务
async def create_backtest_by_code(
    request: CreateBacktestByCodeRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyBacktestSchema]:
    task = TaskClient.create_task(
        TaskCreateRequest(
            task_type=TaskType.ONCE,
            dag_id="strategy_backtest",
            notebook=request.notebook,
        ),  # type: ignore
        credential,
    )

    backtest_meta = await StrategyBacktest.create(
        name=f"{request.name}__{datetime.now().isoformat(sep=' ',timespec='seconds')}",
        creator=credential.user.id,
        product_type=request.product_type,
        build_type=StrategyBuildType.CODED,
        task_id=task.id,
    )

    return ResponseSchema(data=StrategyBacktestSchema.from_orm(backtest_meta))


@router.delete("/{backtest_id}", response_model=ResponseSchema[StrategyBacktestSchema])
async def delete_backtest_by_id(
    backtest_id: UUID = Path(...),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyBacktestSchema]:
    backtest = await StrategyBacktest.get(id=backtest_id, creator=credential.user.id)

    await BacktestPerformance.filter(backtest_id=backtest_id).delete()

    TaskClient.delete_task(backtest.task_id, credential=credential)
    await backtest.delete()

    return ResponseSchema(data=StrategyBacktestSchema.from_orm(backtest))


@router.delete("", response_model=ResponseSchema[List[StrategyBacktestSchema]])
async def bulk_delete_by_id(
    request: BulkDeletBacktestRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[StrategyBacktestSchema]]:
    backtests = await StrategyBacktest.filter(id__in=request.backtest_ids, creator=credential.user.id)

    for backtest in backtests:
        await BacktestPerformance.filter(backtest_id=backtest.id).delete()
        TaskClient.delete_task(backtest.task_id, credential=credential)
        await backtest.delete()
    return ResponseSchema(data=[StrategyBacktestSchema.from_orm(backtest) for backtest in backtests])


@router.put("", response_model=ResponseSchema[StrategyBacktestSchema])
async def update_backtest_by_task_id(
    task_id: UUID,
    request: UpdateBacktestRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategyBacktestSchema]:
    backtest = await StrategyBacktest.get(task_id=task_id, creator=credential.user.id)
    not_null_request = {key: value for key, value in request.dict().items() if value}
    if backtest.name:
        not_null_request["name"] = backtest.name
    if backtest.description:
        not_null_request["description"] = backtest.description
    if backtest.product_type:
        not_null_request["product_type"] = backtest.product_type

    backtest = await backtest.update_from_dict(not_null_request)
    await backtest.save()
    return ResponseSchema(data=StrategyBacktestSchema.from_orm(backtest))


@router.get("/{backtest_id}/notebook", response_class=HTMLResponse)
async def get_user_strategy_shared_html(
    backtest_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> HTMLResponse:
    backtest = await StrategyBacktest.get(id=backtest_id, creator=credential.user.id)
    task = TaskClient.get_task_detail(task_id=backtest.task_id, credential=credential)

    shared_html = ShareClient.render_notebook(
        CreateNotebookShareRequest(
            name=f"backtest__{task.id}",
            notebook=task.notebook,
            keep_button=False,
        ),  # type: ignore
        credential=credential,
    )
    return HTMLResponse(content=shared_html)

@router.get("/alpha/alpha", response_model=ResponseSchema)
async def get_access_alpha(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema:
    alphas = AlphaClient.get_access_alpha(credential=credential)
    return ResponseSchema(data=alphas)