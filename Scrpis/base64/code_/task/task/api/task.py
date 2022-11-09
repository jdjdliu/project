from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Path
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema
from sdk.share import ShareClient
from sdk.share.schemas import CreateNotebookShareRequest, CreateShareRequest, ShareSchema
from sdk.task import TaskCreateRequest, TaskQueryRequest, TaskSchema, TaskUpdateRequest, TaskUpdateStatusRequest

from task.models import Task

router = APIRouter()


@router.get("/{task_id}", response_model=ResponseSchema[TaskSchema], summary="获取任务列表")
async def get_task_detail(
    task_id: UUID = Path(..., description="任务id"),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[TaskSchema]:
    task = await Task.get(id=task_id)
    return ResponseSchema(data=TaskSchema.from_orm(task))


@router.post("/query", summary="查询任务", response_model=ResponseSchema[List[TaskSchema]])
async def query_task(
    request: TaskQueryRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[TaskSchema]]:
    queryset = Task.filter(creator=credential.user.id)

    if request.dag_id:
        queryset = queryset.filter(dag_id=request.dag_id)

    if request.task_ids:
        queryset = queryset.filter(id__in=request.task_ids)

    tasks = [TaskSchema.from_orm(task) for task in await queryset]
    for task in tasks:
        task.notebook = None

    return ResponseSchema(data=tasks)


@router.post("", summary="新增任务", response_model=ResponseSchema[TaskSchema])
async def create_task(
    request: TaskCreateRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[TaskSchema]:
    task = await Task.create(**request.dict(), creator=credential.user.id)

    # if task.kind == TaskType.ONCE:
    #     dag_id = task.dag_id
    #     payload = {
    #         "dag_run_id": f"{dag_id}_{task.id}",
    #         # 一次性任务,创建之后直接执行
    #         "conf": task.config,
    #     }
    #     try:
    #         AIFlowAPI.dagsrun(dag_id=str(dag_id), payload=payload)
    #     except Exception as e:
    #         raise e

    return ResponseSchema(data=TaskSchema.from_orm(task))


@router.put("/{task_id}", response_model=ResponseSchema[TaskSchema], summary="更改任务")
async def update_task(
    request: TaskUpdateRequest,
    task_id: UUID = Path(..., description="任务id"),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[TaskSchema]:
    task = await Task.get(id=task_id, creator=credential.user.id)

    request.config.pop("task_id", None)

    await task.update_from_dict(
        {
            "is_active": task.is_active if request.is_active is None else request.is_active,
            "notebook": task.notebook if request.notebook is None else request.notebook,
            "config": {**task.config, **request.config},
        }
    ).save()

    return ResponseSchema(data=TaskSchema.from_orm(task))


@router.put("/{task_id}/status", response_model=ResponseSchema[TaskSchema], summary="更改任务")
async def update_task_status(
    request: TaskUpdateStatusRequest,
    task_id: UUID = Path(..., description="任务id"),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[TaskSchema]:
    task = await Task.get(id=task_id, creator=credential.user.id)
    await task.update_from_dict({"status": request.status}).save()

    return ResponseSchema(data=TaskSchema.from_orm(task))


@router.delete("/{task_id}", response_model=ResponseSchema[TaskSchema], summary="删除任务")
async def delete_task(
    task_id: UUID = Path(..., description="任务id"),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[TaskSchema]:
    task = await Task.get(id=task_id, creator=credential.user.id)
    await task.delete()
    return ResponseSchema(data=TaskSchema.from_orm(task))


@router.post("/{task_id}/share", response_model=ResponseSchema[ShareSchema])
async def create_share_item_by_alpha_id(
    request: CreateShareRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[ShareSchema]:
    task = await Task.get(id=request.task_id, creator=credential.user.id)

    share = ShareClient.create_notebook_html(
        CreateNotebookShareRequest(
            name=request.name,
            notebook=task.notebook,
            keep_log=request.keep_log,
            keep_source=request.keep_source,
            keep_button=request.keep_button,
        ),
        credential=credential,
    )
    return ResponseSchema(data=share)
