from typing import Dict, List
from uuid import UUID

from fastapi import APIRouter, Depends

from alpha.constants import AlphaType, AuditStatus, PerformanceSource, AlphaBuildType
from alpha.models import Alpha, Audit, IndexPerformance, Member, Repo
from alpha.schemas import AlphaIdListRequest, AlphaInfoResponse, IndexSchema, MyIndex, AlphaInfoResponseStrategy
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import PagerSchema, QueryParamSchema, ResponseSchema
from sdk.task import TaskClient, TaskQueryRequest, TaskSchema

router = APIRouter()


@router.get("", response_model=ResponseSchema[PagerSchema[MyIndex]])
async def get_user_created_index(
    order_by: str = "-created_at",
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[MyIndex]]:
    if order_by != "-created_at":
        alpha_ids = [alpha.id for alpha in await Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.INDEX).only("id").order_by("-created_at")]
        all_performances = await IndexPerformance.filter(
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
            IndexPerformance.filter(id__in=performance_ids).order_by(order_by),
            page_params=query,
        )

        alphas = await Alpha.filter(id__in=[perf.alpha_id for perf in pager.items], alpha_type=AlphaType.INDEX)
        alphas_mapping = {alpha.id: alpha for alpha in alphas}

        tasks_mapping: Dict[UUID, TaskSchema] = {
            task.id: task
            for task in TaskClient.get_tasks(
                TaskQueryRequest(
                    task_ids=[alpha.task_id for alpha in alphas],
                ),  # type: ignore
                credential=credential,
            )
        }

        items = [
            MyIndex(
                alpha=alphas_mapping[performance.alpha_id],
                performance=performance,
                task=tasks_mapping[alphas_mapping[performance.alpha_id].task_id],
            )
            for performance in pager.items
        ]
    else:
        pager = await PagerSchema.from_queryset(
            Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.INDEX).order_by(order_by),
            page_params=query,
        )

        all_performances = await IndexPerformance.filter(
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

        performances = await IndexPerformance.filter(id__in=performance_ids)

        performances_mapping = {performance.alpha_id: performance for performance in performances}

        tasks_mapping: Dict[UUID, TaskSchema] = {
            task.id: task
            for task in TaskClient.get_tasks(
                TaskQueryRequest(
                    task_ids=[alpha.task_id for alpha in pager.items],
                ),  # type: ignore
                credential=credential,
            )
        }

        items = [
            MyIndex(
                alpha=alpha,
                performance=performances_mapping.get(alpha.id, None),
                task=tasks_mapping[alpha.task_id],
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


@router.delete("/{alpha_id}", response_model=ResponseSchema[IndexSchema])
async def delete_Index(
    alpha_id: int,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[IndexSchema]:
    alpha = await Alpha.get(id=alpha_id, creator=credential.user.id, catalog_id=None, alpha_type=AlphaType.INDEX)  # check permission

    audit = await Audit.get_or_none(alpha_id=alpha_id, status=AuditStatus.APPROVED)  # check alpha stats in repo  查看是否提交到某个因子库， 如果审核通过了就不能删除
    if audit:
        raise HTTPExceptions.NO_PERMISSION

    await IndexPerformance.filter(alpha_id=alpha_id).delete()  # del performance
    await Audit.filter(alpha_id=alpha.id).delete()  # del audit
    await alpha.delete()  # del alpha

    TaskClient.delete_task(task_id=alpha.task_id, credential=credential)  # del task
    return ResponseSchema(data=IndexSchema.from_orm(alpha))


@router.delete("", response_model=ResponseSchema[List[IndexSchema]])
async def bulk_delete_index(
    request: AlphaIdListRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[IndexSchema]]:
    alphas = await Alpha.filter(
        id__in=request.alpha_ids,
        creator=credential.user.id,
        catalog_id=None,
        alpha_type=AlphaType.INDEX,
    )
    for alpha in alphas:
        audit = await Audit.get_or_none(alpha_id=alpha.id, status=AuditStatus.APPROVED)  # 查看是否提交到某个因子库， 如果审核通过了就不能删除
        if audit:
            raise HTTPExceptions.NO_PERMISSION

        await IndexPerformance.filter(alpha_id=alpha.id).delete()  # del performance
        await Audit.filter(alpha_id=alpha.id).delete()  # del audit
        await alpha.delete()  # del alpha

        TaskClient.delete_task(task_id=alpha.task_id, credential=credential)  # del task
    return ResponseSchema(data=[IndexSchema.from_orm(alpha) for alpha in alphas])


@router.get("/alpha", response_model=ResponseSchema[List[AlphaInfoResponse]])
async def get_access_alpha(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[AlphaInfoResponse]]:
    # query all repos that current user joined
    repo_ids = [member.repo_id for member in await Member.filter(user_id=credential.user.id).only("repo_id")]
    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]
    repo_ids = list(set(public_repo_ids + repo_ids))

    # query all alpha that current user can access
    audit_alpha_ids = [audit.alpha_id for audit in await Audit.filter(repo_id__in=repo_ids, status=AuditStatus.APPROVED).only("alpha_id")]
    user_alpha_ids = [alpha.id for alpha in await Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.ALPHA).only("id")]
    alpha_ids = list(set(audit_alpha_ids + user_alpha_ids))

    alphas = await Alpha.filter(id__in=alpha_ids, alpha_type=AlphaType.ALPHA, catalog_id__isnull=True).only("alpha_id", "name")
    return ResponseSchema(data=[AlphaInfoResponse(name=i.name, alpha_id=i.alpha_id) for i in alphas])


@router.get("/strategy", response_model=ResponseSchema[List[AlphaInfoResponseStrategy]])
async def get_access_alpha_(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[AlphaInfoResponseStrategy]]:
    # query all repos that current user joined
    repo_ids = [member.repo_id for member in await Member.filter(user_id=credential.user.id).only("repo_id")]
    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]
    repo_ids = list(set(public_repo_ids + repo_ids))

    # query all alpha that current user can access
    audit_alpha_ids = [audit.alpha_id for audit in await Audit.filter(repo_id__in=repo_ids, status=AuditStatus.APPROVED).only("alpha_id")]
    user_alpha_ids = [alpha.id for alpha in await Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.ALPHA).only("id")]
    alpha_ids = list(set(audit_alpha_ids + user_alpha_ids))

    alphas = await Alpha.filter(id__in=alpha_ids, alpha_type=AlphaType.ALPHA, catalog_id__isnull=True, build_type=AlphaBuildType.WIZARD).only("name", "expression")
    return ResponseSchema(data=[AlphaInfoResponseStrategy(name=i.name, expression=i.expression[8:]) for i in alphas])
