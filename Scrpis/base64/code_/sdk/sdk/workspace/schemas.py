from pydantic import BaseModel, Field


class WorkspaceStatsSchema(BaseModel):
    id: int = Field(..., description="id")
    status: int = Field(..., description="status")
    createTime: str = Field(..., description="createTime")
    updateTime: str = Field(..., description="updateTime")
