from uuid import UUID

from pydantic import BaseModel, Field
from sdk.common import schemas

from ..constants import MemberRole


class CreateMemberRequest(BaseModel):
    user_id: UUID = Field(..., description="用户ID")
    repo_id: UUID = Field(..., description="因子库ID")
    role: MemberRole = Field(..., description="角色")


class DeleteMemberRequest(BaseModel):
    user_id: UUID = Field(..., description="用户ID")
    repo_id: UUID = Field(..., description="因子库ID")


class MemberSchema(
    schemas.UUIDIDMixin,
    schemas.CreatorMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    BaseModel,
):
    user_id: UUID = Field(..., description="用户ID")
    repo_id: UUID = Field(..., description="因子库ID")
    role: MemberRole = Field(..., description="角色")

    class Config:
        orm_mode = True
        use_enum_values = True
