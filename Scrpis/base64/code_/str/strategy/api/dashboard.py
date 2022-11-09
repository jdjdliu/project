from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import PagerSchema, QueryParamSchema, ResponseSchema
from sdk.strategy.schemas.common import StrategyParameter
from tortoise.expressions import Q

from strategy.constants import AuditStatus, StrategySortKeyWords
from strategy.models import Audit, Collection, Member, Repo, Strategy, StrategyDaily, StrategyPerformance
from strategy.schemas import DashboardQuery, OrderSchema, StrategyDashboardSchema, StrategySchema

router = APIRouter()


@router.post("/query", response_model=ResponseSchema[PagerSchema[StrategyDashboardSchema]])
async def get_strategy_dashboard(
    request: DashboardQuery,
    order_by: str = "-created_at",
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[StrategyDashboardSchema]]:
    # TODO: 股票、基金、期货的筛选
    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]  # 公共repo
    member_repo_ids = [member.repo_id for member in await Member.filter(user_id=credential.user.id).only("repo_id")]  # 有权限的repo
    repo_ids = list(set(public_repo_ids + member_repo_ids))

    if request.repos:
        repo_ids = [repo_id for repo_id in repo_ids if repo_id in request.repos]
        strategy_ids = list(
            set([audit.strategy_id for audit in await Audit.filter(repo_id__in=repo_ids, status=AuditStatus.APPROVED).only("strategy_id")])
        )
    else:
        access_strategy_ids = [
            audit.strategy_id for audit in await Audit.filter(repo_id__in=repo_ids, status=AuditStatus.APPROVED).only("strategy_id")
        ]
        user_strategy_ids = [strategy.id for strategy in await Strategy.filter(creator=credential.user.id).only("id")]
        strategy_ids = list(set(access_strategy_ids + user_strategy_ids))

    if request.keyword:
        strategy_ids = [
            strategy.id
            for strategy in await Strategy.filter(
                Q(id__in=strategy_ids) & Q(name__contains=request.keyword) | Q(description__contains=request.keyword)
            ).only("id")
        ]
    collected_strategy_ids = await Collection.filter(creator=credential.user.id).only("strategy_id")

    if request.strategy_type:
        strategy_ids = [strategy.id for strategy in await Strategy.filter(id__in=strategy_ids, product_type__in=request.strategy_type)]

    items = []
    orderby = f"-{order_by}" if order_by not in [StrategySortKeyWords.MAX_DRAWDOWN, "-created_at"] else order_by

    pager = await PagerSchema.from_queryset(
        StrategyPerformance.filter(strategy_id__in=strategy_ids).order_by(orderby),  # type:ignore
        page_params=query,
    )

    strategys = await Strategy.filter(
        id__in=[perf.strategy_id for perf in pager.items],
    )

    strategy_mapping = {strategy.id: strategy for strategy in strategys}

    for perf in pager.items:
        strategy = StrategySchema.from_orm(strategy_mapping[perf.strategy_id])  # type: ignore
        items.append(
            StrategyDashboardSchema(
                capital_base=strategy.parameter.capital_base
                if isinstance(strategy.parameter, StrategyParameter)
                else strategy.parameter.get("capital_base"),
                owner_id=strategy.creator,
                strategy_id=strategy.id,
                name=strategy.name,
                description=strategy.description,
                sharpe=perf.sharpe,
                max_drawdown=perf.max_drawdown,
                cum_return=perf.cum_return,
                annual_return=perf.annual_return,
                today_return=perf.today_return,
                three_month_return=perf.three_month_return,
                cum_return_plot=perf.cum_return_plot,
                benchmark_cum_return_plot=perf.benchmark_cum_return_plot,
                relative_cum_return_plot=perf.relative_cum_return_plot,
                collected=strategy.id in collected_strategy_ids,
            )
        )

    return ResponseSchema(
        data=PagerSchema(
            page=pager.page,
            page_size=pager.page_size,
            total_page=pager.total_page,
            items_count=pager.items_count,
            items=items,
        )
    )


@router.get("/plan")
async def query_strategy_plan(
    strategy_id: UUID = Query(...),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[OrderSchema]]:

    strategy = await Strategy.get_or_none(id=strategy_id, creator=credential.user.id)
    if not strategy:
        # 检测用户是否有权限查询此id
        members_mapping = {member.repo_id: member for member in await Member.filter(user_id=credential.user.id)}
        repos_id = [repo.id for repo in await Repo.filter(Q(id__in=list(members_mapping.keys())) | Q(is_public=True))]
        strategys_belong_repo = [audit.strategy_id for audit in await Audit.filter(repo_id__in=repos_id, status=AuditStatus.APPROVED)]

        if strategy_id not in strategys_belong_repo:
            raise HTTPExceptions.NO_ACCESS_TO_ID

    strategy_daily = await StrategyDaily.filter(strategy_id=strategy_id).order_by("-run_date").first()
    if not strategy_daily:
        return ResponseSchema(data=None)

    return ResponseSchema(data=[OrderSchema(**i) for i in strategy_daily.orders])
