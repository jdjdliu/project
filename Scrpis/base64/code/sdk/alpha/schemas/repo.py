from typing import Optional

from pydantic import BaseModel, Field

from sdk.common import schemas

from ..constants import AlphaType
from .member import MemberSchema


class CreateRepoRequest(BaseModel):
    name: str = Field(..., description="因子库名")
    description: str = Field("", description="因子库描述")
    is_public: bool = Field(False, description="是否公开")
    repo_type: AlphaType = Field(AlphaType.ALPHA, description="因子库类型")


class UpdateRepoRequest(BaseModel):
    name: str = Field(..., description="因子库名")
    description: str = Field("", description="因子库描述")
    is_public: bool = Field(False, description="是否公开")


class RepoSchema(
    schemas.UUIDIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    name: str = Field(..., description="因子库名")
    description: str = Field(..., description="因子库描述")
    is_public: bool = Field(..., description="是否公开")
    repo_type: AlphaType = Field("", description="因子库类型")

    class Config:
        orm_mode = True


class RepoWithMember(BaseModel):
    repo: RepoSchema = Field(..., description="因子库")
    member: Optional[MemberSchema] = Field(None, description="成员列表")
