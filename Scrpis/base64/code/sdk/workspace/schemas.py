from pydantic import BaseModel, Field
from .constants import WorkSpaceStatus
from ..common.schemas import UpdatedAtMixin, CreatedAtMixin, IntIDMixin


class WorkspaceStatsSchema(BaseModel):
    id: int = Field(..., description="id")
    status: WorkSpaceStatus = Field(..., description="status")
    workspaceType: str = Field(default="", description="工作台类型")
    createTime: str = Field(..., description="createTime")
    updateTime: str = Field(..., description="updateTime")


class WorkspaceResourceSchema(IntIDMixin, BaseModel):
    cpu: float = Field(...)
    mem: str = Field(...)
    gpu: int = Field(...)
    bandWidth: int = Field(..., description="带宽")
    status: WorkSpaceStatus = Field(...)