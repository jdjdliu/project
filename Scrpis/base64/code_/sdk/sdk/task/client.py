from typing import List, Type, Union
from uuid import UUID

from sdk.auth import Credential
from sdk.httputils import HTTPClient

from .schemas import TaskCreateRequest, TaskQueryRequest, TaskSchema, TaskUpdateRequest, TaskUpdateStatusRequest
from .settings import TASK_HOST


class TaskClient:

    HttpClient = HTTPClient(TASK_HOST)

    @classmethod
    def get_tasks(cls: Type["TaskClient"], request: TaskQueryRequest, credential: Credential) -> List[TaskSchema]:
        return [TaskSchema(**task) for task in cls.HttpClient.post("/api/task/task/query", payload=request.dict(), credential=credential)]

    @classmethod
    def get_task_detail(cls: Type["TaskClient"], task_id: Union[str, UUID], credential: Credential) -> TaskSchema:
        return TaskSchema(**cls.HttpClient.get(f"/api/task/task/{task_id}", credential=credential))

    @classmethod
    def create_task(cls: Type["TaskClient"], request: TaskCreateRequest, credential: Credential) -> TaskSchema:
        return TaskSchema(**cls.HttpClient.post("/api/task/task", payload=request.dict(), credential=credential))

    @classmethod
    def update_task(cls: Type["TaskClient"], request: TaskUpdateRequest, credential: Credential) -> TaskSchema:
        return TaskSchema(**cls.HttpClient.put(f"/api/task/task/{request.id}", payload=request.dict(), credential=credential))

    @classmethod
    def update_task_status(cls: Type["TaskClient"], request: TaskUpdateStatusRequest, credential: Credential) -> TaskSchema:
        return TaskSchema(**cls.HttpClient.put(f"/api/task/task/{request.id}/status", payload=request.dict(), credential=credential))

    @classmethod
    def delete_task(cls: Type["TaskClient"], task_id: Union[str, UUID], credential: Credential) -> TaskSchema:
        return TaskSchema(**cls.HttpClient.delete(f"/api/task/task/{task_id}", credential=credential))
