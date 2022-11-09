from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic.fields import Field
from sdk.alpha.constants import ProductType
from sdk.common.schemas import CreatedAtMixin, CreatorMixin, UpdatedAtMixin, UUIDIDMixin


class CreateNotebookShareRequest(BaseModel):
    name: str
    notebook: str
    keep_source: bool = Field(True)
    keep_log: bool = Field(True)
    keep_button: bool = Field(True)


class ShareSchema(
    UUIDIDMixin,
    CreatorMixin,
    CreatedAtMixin,
    UpdatedAtMixin,
    BaseModel,
):
    name: str
    notebook: str
    html: str


class CreateShareRequest(BaseModel):
    name: str = Field(..., description="分享名称", max_length=64)
    task_id: UUID = Field(..., description="任务id")
    keep_log: bool = Field(..., description="保留绩效")
    keep_source: bool = Field(..., description="保留代码")
    keep_button: Optional[bool] = Field(default=True, description="保留克隆按钮")


class TemplateFileSchema(
    UUIDIDMixin,
    CreatedAtMixin,
    UpdatedAtMixin,
    BaseModel,
):
    userbox_id: int = Field(..., description="工作台ID")
    templates: str = Field(..., description="模板内容, JSON格式")

    class Config:
        use_enum_values = True
        orm_mode = True


class CreateTemplateFileRequest(BaseModel):
    name: str = Field(..., description="模板名称")
    product_type: ProductType = Field(..., description="模板标签")
    template_file: str = Field(..., description="模板文件")
    img: str = Field(..., description="模板图片")
    command: str = Field(..., description="模板命令")


# clone
class CreateCloneRequest(BaseModel):
    notebook: str = Field(..., description="notebook内容")
    name: str = Field(..., description="文件名")


class JupyterCloneSchema(CreatedAtMixin, BaseModel):
    content: Optional[str] = Field(..., description="notebook内容--base64")
    format: Optional[str] = Field(..., description="文件格式")
    last_modified: datetime = Field(..., description="文件最后修改时间")
    minetype: Optional[str] = Field(...)

    name: str = Field(..., description="文件名称")
    path: str = Field(..., description="文件路径")
    size: int = Field(..., description="文件大小")
    type: str = Field(..., description="文件类型")
    writeable: bool = Field(..., description="文件是否可写")


class JupyterCloneParams(BaseModel):
    content: Optional[str] = Field(..., description="notebook内容--base64")
    format: Optional[str] = Field(..., description="文件格式")

    name: str = Field(..., description="文件名称")
    path: str = Field(..., description="文件路径")
    type: str = Field(..., description="文件类型")


class CloneJupyterRequest(BaseModel):
    params: JupyterCloneParams = Field(..., description="参数")
    userbox_id: int = Field(..., description="工作台ID")
