from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from .alpha import AlphaSchema, IndexSchema
from .common import AlphaQuery
from .performance import IndexPerformanceSchema, PerformanceSchema


class DashboardQuery(AlphaQuery):
    repos: List[UUID] = Field(default_factory=list, description="因子库列表")
    catalogs: List[UUID] = Field(default_factory=list, description="因子目录列表")
    just_collection: bool = Field(default=False, description="是否只显示收藏")


class DashboardAlpha(BaseModel):
    alpha: AlphaSchema = Field(..., description="因子")
    is_collection: Optional[bool] = Field(description="是否收藏", default=False)
    performance: Optional[PerformanceSchema] = Field(..., description="绩效")


class IndexDashboardQuery(BaseModel):
    publisher: Optional[list] = Field(default=None, description="发布人")
    source_factor: Optional[list] = Field(default=None, description="因子来源")
    product_type: Optional[list] = Field(default=None, description="产品类型")
    stock_pool: Optional[list] = Field(default=None, description="股票池")
    weight_method: Optional[list] = Field(default=None, description="加权类型")
    just_collection: bool = Field(default=False, description="是否只显示收藏")
    catalogs: List[UUID] = Field(default_factory=list, description="因子目录列表")


class DashboardIndex(BaseModel):
    alpha: IndexSchema = Field(..., description="因子")
    is_collection: Optional[bool] = Field(description="是否收藏", default=False)
    performance: Optional[IndexPerformanceSchema] = Field(..., description="绩效")
