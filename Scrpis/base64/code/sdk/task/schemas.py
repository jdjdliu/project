from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from sdk.common import schemas

from .constants import TaskStatus, TaskType


class TaskSchema(
    schemas.UUIDIDMixin,
    schemas.CreatorMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    BaseModel,
):
    task_type: TaskType = Field(..., description="任务类型")
    dag_id: str = Field(..., description="任务id")

    is_active: bool = Field(..., description="是否启用任务")
    status: TaskStatus = Field(..., description="任务状态")

    notebook: Optional[str] = Field(..., description="notebook代码内容")
    config: Dict[Any, Any] = Field(..., description="用户配置")

    class Config:
        orm_mode = True
        use_enum_values = True


class TaskQueryRequest(BaseModel):
    dag_id: Optional[str] = Field(None, description="dag id")
    task_ids: Optional[List[UUID]] = Field(None, description="任务id列表")


class TaskCreateRequest(BaseModel):
    is_active: bool = Field(True, description="是否启用任务")
    notebook: str = Field(..., description="notebook代码内容")
    task_type: TaskType = Field(..., description="任务类型")
    dag_id: str = Field(..., description="任务id")
    config: Dict[Any, Any] = Field(default_factory=dict, description="用户配置")

    class Config:
        use_enum_values = True


class TaskUpdateRequest(
    schemas.UUIDIDMixin,
    BaseModel,
):
    is_active: bool = Field(True, description="是否启用任务")
    notebook: Optional[str] = Field(None, description="notebook代码内容")
    config: Dict[Any, Any] = Field(default_factory=dict, description="用户配置")


class TaskUpdateStatusRequest(
    schemas.UUIDIDMixin,
    BaseModel,
):
    status: TaskStatus = Field(..., description="任务状态")
