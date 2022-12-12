import base64
from datetime import date
from math import ceil
from typing import Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Path
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import PagerSchema, QueryParamSchema, ResponseSchema
from sdk.task import TaskClient, TaskCreateRequest, TaskQueryRequest, TaskSchema, TaskType
from sdk.task.constants import TaskStatus

from strategy.constants import AuditStatus
from strategy.models import Audit, Member, Repo, Strategy, StrategyBacktest, StrategyDaily, StrategyPerformance
from strategy.schemas import (
    CreateStrategyFromBacktestRequest,
    MyStrategies,
    MyStrategy,
    MyStrategyPerformanceSchema,
    PlotCutSchema,
    StrategyDailySchema,
    StrategyIdListRequest,
    StrategyPerformanceSchema,
    StrategySchema,
)
from strategy.utils import get_benchmark_profit, get_cut_plot_new_return

router = APIRouter()


@router.get("", response_model=ResponseSchema[PagerSchema[MyStrategies]])
async def get_user_created_strategies(
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[MyStrategies]]:
    strategy_ids = [strategy.id for strategy in await Strategy.filter(creator=credential.user.id).only("id")]
    all_performances = await StrategyPerformance.filter(
        strategy_id__in=strategy_ids,
    ).values("id", "strategy_id", "run_date")

    perfs = {}
    for perf in all_performances:
        if perf["strategy_id"] not in perfs:
            perfs[perf["strategy_id"]] = perf
        else:
            if perf["run_date"] >= perfs[perf["strategy_id"]]["run_date"]:
                perfs[perf["strategy_id"]] = perf

    performance_ids = [perf["id"] for perf in perfs.values()]

    pager = await PagerSchema.from_queryset(
        Strategy.filter(id__in=strategy_ids).order_by("-created_at"),
        page_params=query,
    )

    strategies_mapping = {strategy.id: strategy for strategy in pager.items}

    tasks_mapping: Dict[UUID, TaskSchema] = {
        task.id: task
        for task in TaskClient.get_tasks(
            TaskQueryRequest(
                task_ids=[strategy.task_id for strategy in pager.items],
            ),  # type: ignore
            credential=credential,
        )
    }

    performances = (
        await StrategyPerformance.filter(id__in=performance_ids)
        .order_by("-created_at")
        .only("strategy_id", "max_drawdown", "annual_return", "cum_return", "sharpe")
    )
    performance_mapping = {performance.strategy_id: performance for performance in performances}

    items = [
        MyStrategies(
            strategy=strategies_mapping[strategy.id],
            performance=MyStrategyPerformanceSchema.from_orm(performance_mapping.get(strategy.id))
            if strategy.id in performance_mapping.keys()
            else None,
            task=tasks_mapping[strategies_mapping[strategy.id].task_id],
            daily=None,
        )
        for strategy in pager.items
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


@router.get("/{strategy_id}", response_model=ResponseSchema[MyStrategy])
async def get_user_strategy_by_id(
    strategy_id: UUID = Path(...),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[MyStrategy]:
    repo_ids = [member["repo_id"] for member in await Member.filter(user_id=credential.user.id).values("repo_id")]
    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]
    repo_ids = list(set(public_repo_ids + repo_ids))
    all_audit_strategy_ids = [audit["strategy_id"] for audit in await Audit.filter(repo_id__in=repo_ids).values("strategy_id")]

    strategy = await Strategy.get(id=strategy_id)
    if strategy.creator != credential.user.id and strategy.id not in all_audit_strategy_ids:
        raise HTTPExceptions.NO_PERMISSION

    task = TaskClient.get_task_detail(task_id=strategy.task_id, credential=credential)
    notebook = base64.urlsafe_b64encode(task.notebook.encode()).decode()

    if strategy is None:
        raise HTTPExceptions.NO_PERMISSION

    performance = await StrategyPerformance.get_or_none(strategy_id=strategy_id)
    # dailys_schema = [StrategyDailySchema.from_orm(i) for i in await StrategyDaily.filter(strategy_id=strategy_id).order_by("-run_date").all()]
    # dailys_schema = [StrategyDailySchema.from_orm(await StrategyDaily.filter(strategy_id=strategy_id).order_by("-run_date").first())]
    if performance:
        dailys_schema = [StrategyDailySchema.from_orm(await StrategyDaily.filter(strategy_id=strategy_id).order_by("-run_date").first())]
        benchmark_profit = get_benchmark_profit(dailys_schema, performance.benchmark_cum_return_plot) 
        overall_position = dailys_schema[0].portfolio["pv"] / (dailys_schema[0].portfolio["pv"] + dailys_schema[0].cash)
    else:
        dailys_schema = None
        benchmark_profit = None
        overall_position = 0
        
    return ResponseSchema(
        data=MyStrategy(  # type: ignore
            strategy=StrategySchema.from_orm(strategy),
            performance=StrategyPerformanceSchema.from_orm(performance) if performance else None,
            daily=dailys_schema,
            benchmark_profit=benchmark_profit,
            task=task,
            overall_position=overall_position,
            notebook = notebook,
        )
    )


@router.delete("/{strategy_id}", response_model=ResponseSchema[StrategySchema])
async def delete_strategy(
    strategy_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategySchema]:
    strategy = await Strategy.get(id=strategy_id, creator=credential.user.id)  # check permission

    audit = await Audit.get_or_none(strategy_id=strategy_id, status=AuditStatus.APPROVED)  # check strategy stats in repo  查看是否提交到某个策略库， 如果审核通过了就不能删除
    if audit:
        raise HTTPExceptions.NO_PERMISSION

    await StrategyPerformance.filter(strategy_id=strategy_id).delete()  # del performance
    await Audit.filter(strategy_id=strategy.id).delete()  # del audit
    await StrategyDaily.filter(strategy_id=strategy_id).delete()  # del daily
    await strategy.delete()  # del strategy

    TaskClient.delete_task(task_id=strategy.task_id, credential=credential)  # del task
    return ResponseSchema(data=strategy)


@router.delete("", response_model=ResponseSchema[List[StrategySchema]])
async def bulk_delete_strategy(
    request: StrategyIdListRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[StrategySchema]]:
    strategies = await Strategy.filter(
        id__in=request.strategy_ids,
        creator=credential.user.id,
    )
    for strategy in strategies:
        audit = await Audit.get_or_none(strategy_id=strategy.id, status=AuditStatus.APPROVED)  # 查看是否提交到某个策略库， 如果审核通过了就不能删除
        if audit:
            raise HTTPExceptions.NO_PERMISSION

        await StrategyPerformance.filter(strategy_id=strategy.id).delete()  # del performance
        await Audit.filter(strategy_id=strategy.id).delete()  # del audit
        await StrategyDaily.filter(strategy_id=strategy.id).delete()  # del daily
        await strategy.delete()  # del strategy

        TaskClient.delete_task(task_id=strategy.task_id, credential=credential)  # del task
    return ResponseSchema(data=[StrategySchema.from_orm(strategy) for strategy in strategies])


@router.post("/backtest", response_model=ResponseSchema[StrategySchema])
async def create_strategy_from_backtest(
    request: CreateStrategyFromBacktestRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[StrategySchema]:
    backtest = await StrategyBacktest.get(id=request.backtest_id, creator=credential.user.id)
    backtest_task = TaskClient.get_task_detail(backtest.task_id, credential=credential)

    if backtest_task.status != TaskStatus.SUCCESS:  # 任务执行成功才能创建策略
        raise HTTPExceptions.INVALID_STATS

    task = TaskClient.create_task(
        TaskCreateRequest(
            task_type=TaskType.CRON,
            dag_id="strategy_daily",
            notebook=backtest_task.notebook,
        ),  # type: ignore
        credential,
    )

    strategy = await Strategy.create(
        name=request.name,
        creator=credential.user.id,
        description=backtest.description,
        parameter=backtest.parameter if backtest.parameter else {},
        product_type=backtest.product_type,
        task_id=task.id,
        build_type=backtest.build_type,
    )
    await strategy.update_from_dict({"strategy_id": f"strategy_{str(strategy.id).rjust(6, '0')}"}).save()

    return ResponseSchema(data=StrategySchema.from_orm(strategy))


@router.get("/{strategy_id}/plot_cut")
async def get_plot_by_timestamp(
    strategy_id: UUID,
    cut_stamp: str,  # 1546444800000.0,1661356800000.0
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PlotCutSchema]:
    cut_list = [float(u) for u in cut_stamp.split(",")]
    performance = await StrategyPerformance.get(strategy_id=strategy_id)
    return ResponseSchema(
        data=get_cut_plot_new_return(
            cut_list,
            performance.cum_return_plot,
            performance.benchmark_cum_return_plot,
        )
    )


@router.get("/{strategy_id}/positions", response_model=ResponseSchema[PagerSchema])
async def get_strategy_positions(
    strategy_id: UUID,
    run_date: Optional[date] = None,
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema]:
    if run_date:
        daily = await StrategyDaily.filter(strategy_id=strategy_id, run_date=run_date).first().all()
    else:
        daily = await StrategyDaily.filter(strategy_id=strategy_id).order_by("-run_date").first().all()
    if daily is None:
        return ResponseSchema(data=PagerSchema(page=query.page, page_size=query.page_size, total_page=0, items_count=0, items=[]))
    response_list = []
    for position in daily.positions:
        position.update({"position_ratio": position["value"] / (daily.portfolio["pv"] + daily.cash)})
        response_list.append(position)
    return ResponseSchema(
        data=PagerSchema(
            page=query.page,
            page_size=query.page_size,
            total_page=ceil(len(response_list) / query.page_size),
            items_count=len(response_list),
            items=response_list[(query.page - 1) * query.page_size : query.page * query.page_size],
        )
    )


@router.get("/{strategy_id}/orders", response_model=ResponseSchema[PagerSchema])
async def get_strategy_orders(
    strategy_id: UUID,
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema]:
    daily = await StrategyDaily.filter(strategy_id=strategy_id).order_by("-run_date")
    if daily is None:
        return ResponseSchema(data=PagerSchema(page=query.page, page_size=query.page_size, total_page=0, items_count=0, items=[]))
    response_list = []
    for d in daily:
        for order in d.orders:
            order.update({"position_ratio": order["price"] * order["amount"] / (d.portfolio["pv"] + d.cash)})
            response_list.append(order)
    return ResponseSchema(
        data=PagerSchema(
            page=query.page,
            page_size=query.page_size,
            total_page=ceil(len(response_list) / query.page_size),
            items_count=len(response_list),
            items=response_list[(query.page - 1) * query.page_size : query.page * query.page_size],
        )
    )


@router.get("/{strategy_id}/transactions", response_model=ResponseSchema[PagerSchema])
async def get_strategy_transactions(
    strategy_id: UUID,
    run_date: Optional[date] = None,
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema]:
    if run_date:
        daily = await StrategyDaily.filter(strategy_id=strategy_id, run_date=run_date).first().only("transactions")
    else:
        daily = await StrategyDaily.filter(strategy_id=strategy_id).order_by("-run_date").first().only("transactions")
    if daily is None:
        return ResponseSchema(data=PagerSchema(page=query.page, page_size=query.page_size, total_page=0, items_count=0, items=[]))
    response_list = []
    for transaction in daily.transactions:
        response_list.append(transaction)
    return ResponseSchema(
        data=PagerSchema(
            page=query.page,
            page_size=query.page_size,
            total_page=ceil(len(response_list) / query.page_size),
            items_count=len(response_list),
            items=response_list[(query.page - 1) * query.page_size : query.page * query.page_size],
        )
    )


@router.get("/{strategy_id}/logs", response_model=ResponseSchema[PagerSchema])
async def get_strategy_logs(
    strategy_id: UUID,
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema]:
    daily = await StrategyDaily.filter(strategy_id=strategy_id).order_by("-run_date").only("logs","run_date")
    if daily is None:
        return ResponseSchema(data=PagerSchema(page=query.page, page_size=query.page_size, total_page=0, items_count=0, items=[]))
    response_list = []
    for d in daily:
        for log in d.logs:
            log.update({"run_date": d.run_date})
            response_list.append(log)
    return ResponseSchema(
        data=PagerSchema(
            page=query.page,
            page_size=query.page_size,
            total_page=ceil(len(response_list) / query.page_size),
            items_count=len(response_list),
            items=response_list[(query.page - 1) * query.page_size : query.page * query.page_size],
        )
    )