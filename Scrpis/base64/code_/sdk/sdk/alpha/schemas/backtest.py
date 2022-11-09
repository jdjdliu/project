import ast
import re
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field
from sdk.common import schemas

from ..constants import AlphaBuildType, AlphaType, ProductType
from .common import AlphaParameter, CompositionAlpha, IndexParameter


class BacktestSchema(
    schemas.IntIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    name: str = Field(..., description="回测名称")

    alphas: Optional[List[CompositionAlpha]] = Field(default_factory=list, description="因子列表")
    column: Optional[str] = Field("alpha", description="因子列名")
    expression: Optional[str] = Field(..., description="因子表达式")
    parameter: Optional[AlphaParameter] = Field(..., description="回测参数")
    dependencies: Optional[List[str]] = Field(..., description="因子依赖")

    alpha_type: AlphaType = Field(..., description="因子类型")
    product_type: ProductType = Field(..., description="资产类型")
    build_type: Optional[AlphaBuildType] = Field(description="因子构建类型", default=AlphaBuildType.WIZARD)

    task_id: UUID = Field(..., description="任务 ID")

    class Config:
        orm_mode = True
        use_enum_values = True


class CreateBacktestRequest(BaseModel):
    name: str = Field(..., description="因子名称")
    column: str = Field("alpha", description="因子列名")
    alphas: List[CompositionAlpha] = Field(default_factory=list, description="因子列表")
    parameter: AlphaParameter = Field(..., description="回测参数")

    alpha_type: AlphaType = Field(..., description="因子类型")
    product_type: ProductType = Field(..., description="资产类型")

    class Config:
        use_enum_values = True

    @property
    def expression(self: "CreateBacktestRequest") -> str:
        expression = " + ".join([f"({alpha.weight} * {alpha.alpha})" for alpha in self.alphas])
        return f"alpha = {expression}"

    @property
    def dependencies(self: "CreateBacktestRequest") -> List[str]:
        dependencies = []

        for node in ast.walk(ast.parse(self.expression)):
            if isinstance(node, ast.Name) and re.match(r"^alpha_\d{6}$", node.id) is not None:
                dependencies.append(node.id)

        return dependencies


class IndexBacktestSchema(
    schemas.IntIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    name: str = Field(..., description="回测名称")

    alphas: List[CompositionAlpha] = Field(default_factory=list, description="因子ID")
    column: Optional[str] = Field("alpha", description="因子列名")
    expression: Optional[str] = Field(..., description="因子表达式")
    parameter: Optional[IndexParameter] = Field(..., description="回测参数")
    dependencies: Optional[List[str]] = Field(..., description="因子依赖")

    alpha_type: AlphaType = Field(..., description="因子类型")
    product_type: ProductType = Field(..., description="资产类型")
    build_type: Optional[AlphaBuildType] = Field(description="因子构建类型", default=AlphaBuildType.WIZARD)

    task_id: UUID = Field(..., description="任务 ID")

    class Config:
        orm_mode = True
        use_enum_values = True


class CreateIndexBacktestRequest(BaseModel):
    name: str = Field(..., max_lebgth=30 , description="指数名称")
    description: Optional[str] = Field(default=None, description="指数描述")
    column: str = Field("index", description="指数列名")
    alphas: List[CompositionAlpha] = Field(default_factory=list, description="因子ID")
    parameter: IndexParameter = Field(..., description="回测参数")

    alpha_type: AlphaType = Field("INDEX", description="因子类型")
    product_type: ProductType = Field("STOCK", description="资产类型")

    class Config:
        use_enum_values = True

    @property
    def expression(self: "CreateIndexBacktestRequest") -> str:
        return f"index = {self.alphas}"

    @property
    def dependencies(self: "CreateIndexBacktestRequest") -> List:
        dependencies: List = []

        return dependencies


# 代码式构建
class CreateBacktestByCodeRequest(BaseModel):
    name: str = Field(..., description="任务名称")

    notebook: str = Field(..., description="notebook内容")
    expression: Optional[str] = Field(..., description="")
    parameter: BaseModel = Field(..., description="")

    alpha_type: AlphaType = Field(..., description="因子类型")
    product_type: ProductType = Field(..., description="资产类型")
    # build_type: AlphaBuildType = Field(..., description="因子构建类型")

    class Config:
        use_enum_values = True


class UpdateBacktestRequest(BaseModel):
    column: Optional[str] = Field(default=None)
    expression: Optional[str] = Field(default=None)
    parameter: Optional[AlphaParameter] = Field(default=None)

    class Config:
        use_enum_values = True
