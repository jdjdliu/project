import base64
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from fastapi import APIRouter, Depends, Path
from fastapi.responses import HTMLResponse

from alpha.constants import AlphaType, AuditStatus, PerformanceSource
from alpha.models import (Alpha, Audit, Backtest, IndexPerformance, Member,
                          Performance, Repo)
from alpha.schemas import (AlphaIdListRequest, AlphaSchema,
                           CreateAlphaFromBacktestRequest, IndexSchema,
                           MyAlpha, MyIndex)
from sdk.alpha.schemas.performance import (IndexPerformanceSchema,
                                           PerformanceSchema)
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import PagerSchema, QueryParamSchema, ResponseSchema
from sdk.share import ShareClient
from sdk.share.schemas import CreateNotebookShareRequest
from sdk.task import (TaskClient, TaskCreateRequest, TaskQueryRequest,
                      TaskSchema, TaskType)
from sdk.task.constants import TaskStatus

router = APIRouter()


@router.get("", response_model=ResponseSchema[PagerSchema[MyAlpha]])
async def get_user_created_alphas(
    order_by: str = "-created_at",
    query: QueryParamSchema = Depends(),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[MyAlpha]]:
    if order_by != "-created_at":
        alpha_ids = [alpha.id for alpha in await Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.ALPHA).only("id").order_by("-created_at")]
        all_performances = await Performance.filter(
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
            Performance.filter(id__in=performance_ids).order_by(order_by),
            page_params=query,
        )

        alphas = await Alpha.filter(id__in=[perf.alpha_id for perf in pager.items], alpha_type=AlphaType.ALPHA)
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
            MyAlpha(
                alpha=alphas_mapping[performance.alpha_id],
                performance=performance,
                task=tasks_mapping[alphas_mapping[performance.alpha_id].task_id],
            )
            for performance in pager.items
        ]
    else:
        pager = await PagerSchema.from_queryset(
            Alpha.filter(creator=credential.user.id, alpha_type=AlphaType.ALPHA).order_by(order_by),
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
            MyAlpha(
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


@router.get("/{alpha_id}", response_model=ResponseSchema[Union[MyIndex, MyAlpha]])
async def get_user_alpha_by_id(
    alpha_id: int = Path(...),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[Union[MyIndex, MyAlpha]]:
    repo_ids = [member["repo_id"] for member in await Member.filter(user_id=credential.user.id).values("repo_id")]
    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]
    repo_ids = list(set(public_repo_ids + repo_ids))
    all_audit_alpha_ids = [audit["alpha_id"] for audit in await Audit.filter(repo_id__in=repo_ids).values("alpha_id")]

    alpha = await Alpha.get(id=alpha_id)
    if alpha.creator != credential.user.id and alpha.id not in all_audit_alpha_ids:
        raise HTTPExceptions.NO_PERMISSION

    task = TaskClient.get_task_detail(task_id=alpha.task_id, credential=credential)
    task.notebook = base64.urlsafe_b64encode(task.notebook.encode()).decode()

    if alpha is None:
        raise HTTPExceptions.NO_PERMISSION

    if alpha.alpha_type == AlphaType.ALPHA:
        performance = await Performance.filter(alpha_id=alpha_id, source=PerformanceSource.ALPHA).order_by("-run_datetime").first()
        return ResponseSchema(
            data=MyAlpha(  # type: ignore
                alpha=AlphaSchema.from_orm(alpha),
                performance=PerformanceSchema.from_orm(performance) if performance else None,
                task=task,
            )
        )
    else:
        index_performance = await IndexPerformance.filter(alpha_id=alpha_id).order_by("-run_datetime").first()  # type: ignore  # TODO: remove type ignore
        return ResponseSchema(
            data=MyIndex(  # type: ignore
                alpha=IndexSchema.from_orm(alpha),
                performance=IndexPerformanceSchema.from_orm(index_performance) if index_performance else None,
                task=task,
            )
        )


@router.delete("/{alpha_id}", response_model=ResponseSchema[AlphaSchema])
async def delete_alpha(
    alpha_id: int,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[AlphaSchema]:
    alpha = await Alpha.get(id=alpha_id, creator=credential.user.id, catalog_id=None)  # check permission

    audit = await Audit.get_or_none(alpha_id=alpha_id, status=AuditStatus.APPROVED)  # check alpha stats in repo  查看是否提交到某个因子库， 如果审核通过了就不能删除
    if audit:
        raise HTTPExceptions.NO_PERMISSION

    await Performance.filter(alpha_id=alpha_id).delete()  # del performance
    await Audit.filter(alpha_id=alpha.id).delete()  # del audit
    await alpha.delete()  # del alpha

    TaskClient.delete_task(task_id=alpha.task_id, credential=credential)  # del task
    return ResponseSchema(data=alpha)


@router.delete("", response_model=ResponseSchema[List[AlphaSchema]])
async def bulk_delete_alpha(
    request: AlphaIdListRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[AlphaSchema]]:
    alphas = await Alpha.filter(
        id__in=request.alpha_ids,
        creator=credential.user.id,
        catalog_id=None,
    )
    for alpha in alphas:
        audit = await Audit.get_or_none(alpha_id=alpha.id, status=AuditStatus.APPROVED)  # 查看是否提交到某个因子库， 如果审核通过了就不能删除
        if audit:
            raise HTTPExceptions.NO_PERMISSION

        await Performance.filter(alpha_id=alpha.id).delete()  # del performance
        await Audit.filter(alpha_id=alpha.id).delete()  # del audit
        await alpha.delete()  # del alpha

        TaskClient.delete_task(task_id=alpha.task_id, credential=credential)  # del task
    return ResponseSchema(data=[AlphaSchema.from_orm(alpha) for alpha in alphas])


@router.post("/notebook")
async def create_alpha_from_notebook() -> Any:
    return {"message": "pong"}


@router.get("/{alpha_id}/notebook", response_class=HTMLResponse)
async def get_user_alpha_shared_html_by_alpha_id(
    alpha_id: int,
    keep_button: Optional[bool] = True,
    credential: Credential = Depends(auth_required()),
) -> HTMLResponse:
    repo_ids = [member["repo_id"] for member in await Member.filter(user_id=credential.user.id).values("repo_id")]

    public_repo_ids = [repo.id for repo in await Repo.filter(is_public=True).only("id")]
    repo_ids = list(set(repo_ids + public_repo_ids))

    all_audit_alpha_ids = [audit["alpha_id"] for audit in await Audit.filter(repo_id__in=repo_ids).values("alpha_id")]

    alpha = await Alpha.get(id=alpha_id)
    if (alpha.creator != credential.user.id and alpha.id not in all_audit_alpha_ids) and alpha.catalog_id is None:
        # 非本人创建+非因子库成员+非系统因子
        raise HTTPExceptions.NO_PERMISSION

    if alpha is None:
        raise HTTPExceptions.NO_PERMISSION

    task = TaskClient.get_task_detail(task_id=alpha.task_id, credential=credential)
    shared_html = ShareClient.render_notebook(
        CreateNotebookShareRequest(
            name=alpha.alpha_id,
            notebook=task.notebook,
            keep_button=keep_button,
            keep_source=True,
        ),  # type: ignore
        credential=credential,
    )

    return HTMLResponse(content=shared_html)


@router.post("/backtest", response_model=ResponseSchema[AlphaSchema])
async def create_alpha_from_backtest(
    request: CreateAlphaFromBacktestRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[AlphaSchema]:
    backtest = await Backtest.get(id=request.backtest_id, creator=credential.user.id)
    backtest_task = TaskClient.get_task_detail(backtest.task_id, credential=credential)

    if backtest_task.status != TaskStatus.SUCCESS:  # 任务执行成功才能创建因子
        raise HTTPExceptions.INVALID_STATS

    task = TaskClient.create_task(
        TaskCreateRequest(
            task_type=TaskType.CRON,
            dag_id="alpha_daily",
            notebook=backtest_task.notebook,
        ),  # type: ignore
        credential,
    )

    alpha = await Alpha.create(
        name=request.name,
        creator=credential.user.id,
        column=backtest.column,
        alphas=backtest.alphas,
        parameter=backtest.parameter,
        catalog_id=None,  # 用户默认创建的分类为None, 只能管理员通过接口修改
        expression=backtest.expression,
        dependencies=backtest.dependencies,
        alpha_type=backtest.alpha_type,
        product_type=backtest.product_type,
        task_id=task.id,
        build_type=backtest.build_type,
    )
    alpha_id = f"alpha_{str(alpha.id).rjust(6, '0')}"
    await alpha.update_from_dict({"alpha_id": alpha_id, "column": alpha_id}).save()

    return ResponseSchema(data=AlphaSchema.from_orm(alpha))
