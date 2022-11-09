from typing import List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field

from sdk.alpha import (
    AlphaParameter,
    AlphaSchema,
    BacktestSchema,
    CatalogSchema,
    CollectionSchema,
    CompositionAlpha,
    CreateAlphaFromBacktestRequest,
    CreateBacktestByCodeRequest,
    CreateBacktestRequest,
    CreateIndexBacktestRequest,
    CreateIndexPerformanceSchema,
    CreateMemberRequest,
    CreatePerformanceSchema,
    CreateRepoRequest,
    DashboardAlpha,
    DashboardIndex,
    DashboardQuery,
    DeleteMemberRequest,
    IndexBacktestSchema,
    IndexDashboardQuery,
    IndexParameter,
    IndexPerformanceSchema,
    IndexSchema,
    MemberSchema,
    PerformanceSchema,
    RepoSchema,
    RepoWithMember,
    UpdateBacktestRequest,
    UpdateRepoRequest,
)
from sdk.task import TaskSchema

from .constants import ProductType

__all__ = [
    "IndexSchema",
    "IndexDashboardQuery",
    "IndexBacktestSchema",
    "DashboardIndex",
    "CreateIndexPerformanceSchema",
    "IndexPerformanceSchema",
    "AlphaParameter",
    "AlphaSchema",
    "AlphaIdListRequest",
    "BacktestSchema",
    "UpdateBacktestRequest",
    "CollectionSchema",
    "CompositionAlpha",
    "CreateBacktestByCodeRequest",
    "CreateIndexBacktestRequest",
    "CreateAlphaFromBacktestRequest",
    "CreateBacktestRequest",
    "CreateMemberRequest",
    "CreatePerformanceSchema",
    "CreateRepoRequest",
    "DashboardAlpha",
    "DashboardQuery",
    "DeleteMemberRequest",
    "MemberSchema",
    "PerformanceSchema",
    "IndexParameter",
    "RepoSchema",
    "RepoWithMember",
    "UpdateRepoRequest",
    # local
    "MyAlpha",
    "PredefinedMarket",
    "PredefinedCatalog",
    "PredefinedAlpha",
]


class MyBacktest(BaseModel):
    backtest: Union[IndexBacktestSchema, BacktestSchema] = Field(...)
    performance: Optional[Union[IndexPerformanceSchema, PerformanceSchema]] = Field(...)
    task: TaskSchema = Field(...)


class MyIndex(BaseModel):
    alpha: IndexSchema = Field(...)
    performance: Optional[IndexPerformanceSchema] = Field(...)
    task: Optional[TaskSchema] = Field(...)


class MyAlpha(BaseModel):
    alpha: AlphaSchema = Field(...)
    performance: Optional[PerformanceSchema] = Field(...)
    task: Optional[TaskSchema] = Field(...)


class PredefinedAlpha(BaseModel):
    key: str = Field(...)
    name: str = Field(...)


class PredefinedCatalog(BaseModel):
    name: str = Field(...)
    alphas: List[PredefinedAlpha] = Field(...)


class PredefinedMarket(BaseModel):
    name: str = Field(...)
    market: ProductType = Field(...)
    catalogs: List[PredefinedCatalog] = Field(default_factory=list)


class AlphaIdListRequest(BaseModel):
    alpha_ids: List[int] = Field(..., description="因子ID列表")


class AlphaCatalogResponse(BaseModel):
    catalog: CatalogSchema = Field(..., description="因子分类")
    alphas: List[AlphaSchema] = Field(..., description="因子列表")


class IndexCatalogResponse(BaseModel):
    catalog: CatalogSchema = Field(..., description="因子分类")
    alphas: List[IndexSchema] = Field(..., description="因子列表")


class BulkDeletBacktestRequest(BaseModel):
    backtest_ids: List[int] = Field(..., description="回测ID列表")


class IndexUsers(BaseModel):
    # users_ids: List[str] = Field(..., description="用户ID列表")
    users_ids: UUID = Field(..., description="用户ID列表")

    class Config:
        orm_mode = True


class AlphaInfoResponse(BaseModel):
    name: str = Field(...)
    alpha_id: str = Field(...)


class AlphaInfoResponseStrategy(BaseModel):
    name: str = Field(...)
    expression: str = Field(...)
