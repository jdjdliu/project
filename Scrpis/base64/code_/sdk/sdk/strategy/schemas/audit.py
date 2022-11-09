from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field
from sdk.common import schemas

from ..constants import AuditStatus
from .strategy import StrategySchema


class AuditSchema(
    schemas.UUIDIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    strategy_id: UUID = Field(..., description="策略 ID")
    repo_id: UUID = Field(..., description="策略库 ID")

    status: AuditStatus = Field(..., description="审核状态")
    auditor: Optional[UUID] = Field(..., description="审核人")

    class Config:
        orm_mode = True
        use_enum_values = True


class AuditWithStrategySchema(BaseModel):
    audit: AuditSchema = Field(...)
    strategy: StrategySchema = Field(...)


class CreateAuditRequest(BaseModel):
    strategy_id: UUID = Field(..., description="策略 ID")
    repo_id: UUID = Field(..., description="策略库 ID")


class UpdateAuditRequest(
    schemas.UUIDIDMixin,
    BaseModel,
):
    status: AuditStatus = Field(..., description="审核状态")
