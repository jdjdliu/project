from typing import Optional

from pydantic import BaseModel, Field
from sdk.common import schemas


class UserSchema(schemas.UUIDIDMixin, schemas.CreatedAtMixin, schemas.UpdatedAtMixin, BaseModel):
    username: str = Field(..., description="用户姓名")
    yst_code: Optional[str] = Field(None, description="一事通 ID")
    role_group_id: Optional[str] = Field(None, description="角色组 ID, 暂时没用")

    class Config:
        orm_mode = True
