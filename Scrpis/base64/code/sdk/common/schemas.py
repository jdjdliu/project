from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class IntIDMixin(BaseModel):
    id: int = Field(pk=True)


class UUIDIDMixin(BaseModel):
    id: UUID = Field(pk=True)


class CreatorMixin(BaseModel):
    creator: UUID = Field(..., description="创建者")


class CreatedAtMixin(BaseModel):
    created_at: datetime = Field(..., description="创建时间")


class UpdatedAtMixin(BaseModel):
    updated_at: datetime = Field(..., description="最后更新时间")
