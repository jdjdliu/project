from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from sdk.common import schemas

from ..constants import AuditStatus
from .alpha import AlphaSchema


class AuditSchema(
    schemas.UUIDIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    alpha_id: int = Field(..., description="因子 ID")
    repo_id: UUID = Field(..., description="因子库 ID")

    status: AuditStatus = Field(..., description="审核状态")
    auditor: Optional[UUID] = Field(..., description="审核人")

    class Config:
        orm_mode = True
        use_enum_values = True


class AuditWithAlphaSchema(BaseModel):
    audit: AuditSchema = Field(...)
    alpha: AlphaSchema = Field(...)


class CreateAuditRequest(BaseModel):
    alpha_id: int = Field(..., description="因子 ID")
    repo_id: UUID = Field(..., description="因子库 ID")


class UpdateAuditRequest(
    schemas.UUIDIDMixin,
    BaseModel,
):
    status: AuditStatus = Field(..., description="审核状态")
