from typing import Optional

from pydantic import BaseModel, Field
from sdk.common import schemas


class CatalogSchema(
    schemas.UUIDIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    name: str = Field(..., description="因子目录")
    description: Optional[str] = Field(..., description="因子目录描述")

    class Config:
        orm_mode = True
