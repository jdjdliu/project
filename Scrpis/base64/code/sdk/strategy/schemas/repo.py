from typing import Optional

from pydantic import BaseModel, Field
from sdk.common import schemas

from .member import MemberSchema


class CreateRepoRequest(BaseModel):
    name: str = Field(..., description="策略库名")
    description: str = Field("", description="策略库描述")
    is_public: bool = Field(False, description="是否公开")


class UpdateRepoRequest(BaseModel):
    name: str = Field(..., description="策略库名")
    description: str = Field("", description="策略库描述")
    is_public: bool = Field(False, description="是否公开")


class RepoSchema(
    schemas.UUIDIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    name: str = Field(..., description="策略库名")
    description: str = Field(..., description="策略库描述")
    is_public: bool = Field(..., description="是否公开")

    class Config:
        orm_mode = True


class RepoWithMember(BaseModel):
    repo: RepoSchema = Field(..., description="策略库")
    member: Optional[MemberSchema] = Field(None, description="成员列表")
