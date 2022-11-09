from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from tortoise.expressions import Q

from alpha.constants import AlphaType, AuditStatus, PerformanceSource
from alpha.models import Alpha, Audit, Collection, IndexPerformance, Member, Performance, Repo
from alpha.schemas import DashboardAlpha, DashboardIndex, DashboardQuery, IndexDashboardQuery, IndexPerformanceSchema, IndexUsers
from sdk.auth import Credential, auth_required
from sdk.httputils import QueryParamSchema
from sdk.httputils.schemas import PagerSchema, ResponseSchema

router = APIRouter()


@router.post("/query", response_model=ResponseSchema[PagerSchema[DashboardAlpha]])
async def get_alpha_dashboard(
    request: DashboardQuery,
    order_by: str = "-created_at",
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[DashboardAlpha]]:
    # query all repos that current user joined
    repo_ids = [member.repo_id for member in await Member.filter(user_id=credential.user.id).only("repo_id")]
    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]
    repo_ids = list(set(public_repo_ids + repo_ids))

    if request.repos:
        repo_ids = [repo_id for repo_id in repo_ids if repo_id in request.repos]

    # query all alpha that current user can access
    audit_alpha_ids = [audit.alpha_id for audit in await Audit.filter(repo_id__in=repo_ids, status=AuditStatus.APPROVED).only("alpha_id")]
    if not request.repos:
        user_alpha_ids = [alpha.id for alpha in await Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.ALPHA).only("id")]
        alpha_ids = list(set(audit_alpha_ids + user_alpha_ids))
    else:
        alpha_ids = list(set(audit_alpha_ids))
    if request.catalogs:
        alpha_ids = [alpha.id for alpha in await Alpha.filter(id__in=alpha_ids, catalog_id__in=request.catalogs, alpha_type=AlphaType.ALPHA).only("id")]

    collection_alpha_ids = [collection.alpha_id for collection in await Collection.filter(creator=credential.user.id, alpha_id__in=alpha_ids).only("alpha_id")]
    if request.just_collection:
        alpha_ids = [alpha_id for alpha_id in alpha_ids if alpha_id in collection_alpha_ids]
    if order_by != "-created_at" or request.filters:
        q = Q(alpha_id__in=alpha_ids, source=PerformanceSource.ALPHA)

        if request.filters:
            for flt in request.filters:
                q &= Q(**{f"{flt.key}__{flt.operator}": flt.value, f"{flt.key}__not_isnull": True})

        all_performances: List[Dict[str, Any]] = await Performance.filter(
            alpha_id__in=alpha_ids,
            source=PerformanceSource.ALPHA,
        ).values("id", "alpha_id", "run_datetime")

        perfs = {}
        for perf in all_performances:
            if perf["alpha_id"] not in perfs:
                perfs[perf["alpha_id"]] = perf
            else:
                if perf["run_datetime"] >= perfs[perf["alpha_id"]]["run_datetime"]:
                    perfs[perf["alpha_id"]] = perf

        performance_ids = [perf["id"] for perf in perfs.values()]

        pager = await PagerSchema.from_queryset(
            Performance.filter(q, id__in=performance_ids).order_by(order_by),
            page_params=query,
        )

        alphas = await Alpha.filter(id__in=[perf.alpha_id for perf in pager.items], alpha_type=AlphaType.ALPHA)
        alphas_mapping = {alpha.id: alpha for alpha in alphas}

        items = [
            DashboardAlpha(
                alpha=alphas_mapping[performance.alpha_id],
                performance=performance,
                is_collection=performance.alpha_id in collection_alpha_ids,
            )
            for performance in pager.items
        ]
    else:
        pager = await PagerSchema.from_queryset(
            Alpha.filter(id__in=alpha_ids, alpha_type=AlphaType.ALPHA).order_by(order_by),
            page_params=query,
        )

        all_performances = await Performance.filter(
            alpha_id__in=[alpha.id for alpha in pager.items],
            source=PerformanceSource.ALPHA,
        ).values("id", "alpha_id", "run_datetime")

        perfs = {}
        for perf in all_performances:
            if perf["alpha_id"] not in perfs:
                perfs[perf["alpha_id"]] = perf
            else:
                if perf["run_datetime"] >= perfs[perf["alpha_id"]]["run_datetime"]:
                    perfs[perf["alpha_id"]] = perf

        performance_ids = [perf["id"] for perf in perfs.values()]

        performances = await Performance.filter(id__in=performance_ids)

        performances_mapping = {performance.alpha_id: performance for performance in performances}

        items = [
            DashboardAlpha(
                alpha=alpha,
                performance=performances_mapping.get(alpha.id, None),
                is_collection=alpha.id in collection_alpha_ids,
            )
            for alpha in pager.items
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


@router.post("/query_index", response_model=ResponseSchema[PagerSchema[DashboardIndex]])
async def get_index_dashboard(
    request: IndexDashboardQuery,
    order_by: str = "id",
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[DashboardIndex]]:
    repo_ids = [member.repo_id for member in await Member.filter(user_id=credential.user.id).only("repo_id")]
    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]
    repo_ids = list(set(public_repo_ids + repo_ids))

    audit_alpha_ids = [audit.alpha_id for audit in await Audit.filter(repo_id__in=repo_ids, status=AuditStatus.APPROVED).only("alpha_id")]

    user_alpha_ids = [alpha.id for alpha in await Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.INDEX).only("id")]
    alpha_ids = list(set(audit_alpha_ids + user_alpha_ids))

    # query all alpha that current user can access
    audit_alpha_ids = [audit.alpha_id for audit in await Audit.filter(repo_id__in=repo_ids, status=AuditStatus.APPROVED).only("alpha_id")]
    user_alpha_ids = [alpha.id for alpha in await Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.INDEX).only("id")]
    alpha_ids = list(set(audit_alpha_ids + user_alpha_ids))

    collection_alpha_ids = [collection.alpha_id for collection in await Collection.filter(creator=credential.user.id, alpha_id__in=alpha_ids).only("alpha_id")]
    if request.just_collection:
        alpha_ids = [alpha_id for alpha_id in alpha_ids if alpha_id in collection_alpha_ids]

    if request.catalogs:
        alpha_ids = [alpha.id for alpha in await Alpha.filter(id__in=alpha_ids, catalog_id__in=request.catalogs, alpha_type=AlphaType.INDEX).only("id")]

    if request.product_type:
        product_type_ids = []
        for i in request.product_type:
            product_type_id = [Alpha.id for Alpha in await Alpha.filter(id__in=alpha_ids, product_type=i).only("id")]
            product_type_ids = list(set(product_type_id + product_type_ids))
        alpha_ids = product_type_ids
 
    if request.stock_pool:
        stock_pool_ids = []
        for i in request.stock_pool:
            alpha = await Alpha.filter(id__in=alpha_ids, alpha_type=AlphaType.INDEX).values("id", "parameter")
            stock_pool_id = []
            for a in alpha:
                if i == a["parameter"]["stock_pool"]:
                    stock_pool_id.append(a["id"])
            stock_pool_ids = list(set(stock_pool_id + stock_pool_ids))
        alpha_ids = stock_pool_ids

    if request.weight_method:
        weight_method_ids = []
        for i in request.weight_method:
            alpha = await Alpha.filter(id__in=alpha_ids, alpha_type=AlphaType.INDEX).values("id", "parameter")
            weight_method_id = []
            for a in alpha:
                if i == a["parameter"]["weight_method"]:
                    weight_method_id.append(a["id"])
            weight_method_ids = list(set(weight_method_ids + weight_method_id))
        alpha_ids = weight_method_ids

    if request.publisher:
        publisher_ids = []
        for i in request.publisher:
            publisher_id = [Alpha.id for Alpha in await Alpha.filter(id__in=alpha_ids, creator=i).only("id")]
            publisher_ids = list(set(publisher_id + publisher_ids))
        alpha_ids = publisher_ids
    pager = await PagerSchema.from_queryset(
        Alpha.filter(id__in=alpha_ids, alpha_type=AlphaType.INDEX).order_by(order_by),
        page_params=query,
    )

    all_performances = await IndexPerformance.filter(
        alpha_id__in=[alpha.id for alpha in pager.items],
    ).values("id", "alpha_id", "run_datetime")
    perfs = {}
    for perf in all_performances:
        if perf["alpha_id"] not in perfs:
            perfs[perf["alpha_id"]] = perf
        else:
            if perf["run_datetime"] >= perfs[perf["alpha_id"]]["run_datetime"]:
                perfs[perf["alpha_id"]] = perf

    performance_ids = [perf["id"] for perf in perfs.values()]
    performances = await IndexPerformance.filter(id__in=performance_ids)
    schema = IndexPerformanceSchema
    performances_mapping = {performance.alpha_id: schema.from_orm(performance) for performance in performances}
    items = [
        DashboardIndex(
            alpha=alpha,
            performance=performances_mapping.get(alpha.id, None),
            is_collection=alpha.id in collection_alpha_ids,
        )
        for alpha in pager.items
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


@router.get("/index_users", response_model=ResponseSchema[List[IndexUsers]])
async def get_all_users(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[IndexUsers]]:
    # query all repos that current user joined
    repo_ids = [member.repo_id for member in await Member.filter(user_id=credential.user.id).only("repo_id")]
    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]
    repo_ids = list(set(public_repo_ids + repo_ids))

    # query all alpha that current user can access
    audit_alpha_ids = [audit.alpha_id for audit in await Audit.filter(repo_id__in=repo_ids, status=AuditStatus.APPROVED).only("alpha_id")]
    user_alpha_ids = [alpha.id for alpha in await Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.INDEX).only("id")]
    alpha_ids = list(set(audit_alpha_ids + user_alpha_ids))

    index_users = set(alpha.creator for alpha in await Alpha.filter(id__in=alpha_ids, alpha_type=AlphaType.INDEX).only("creator"))
    return ResponseSchema(data=[IndexUsers(users_ids=i) for i in index_users])
