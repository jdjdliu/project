from datetime import datetime
from typing import Any, List, Optional, Union

from pydantic import BaseModel, Field


class SetModuleCacheRequestSchema(BaseModel):
    key: str = Field(description="缓存 key")
    outputs_json: str = Field(description="缓存数据")
    is_papertrading: bool = Field(False, description="是否是模拟交易模式")


class GetModuleCacheRequestSchema(BaseModel):
    key: str = Field(description="缓存 key")
    is_papertrading: bool = Field(False, description="是否是模拟交易模式")


class GetModuleCacheResponseSchema(BaseModel):
    outputs_json: Optional[str] = Field(None, description="缓存数据")


class ModuleInterface(BaseModel):

    # Do not use this

    name: Optional[str] = Field(None, description="接口名称")
    type: str = Field("", description="接口类型")
    type_code: str = Field("", description="接口传参类型")
    type_name: Optional[str] = Field(None)
    desc: Optional[str] = Field(None, description="接口描述")
    specific_type_name: Optional[str] = Field(None)
    language: Optional[str] = Field("script.py", description="I.code, 代码脚本")
    auto_complete_type: Optional[str] = Field(None)

    optional: bool = Field(True, description="是否可选传入")
    default: Any = Field(None, description="默认值")

    multi: bool = Field(False, description="是否是 I.choice")
    values: List[Any] = Field(default_factory=lambda: [], description="I.choice, 可选值")
    max: float = Field(0.0, description="最大值")
    min: float = Field(0.0, description="最小值")
    can_set_liverun_param: Optional[bool] = Field(False, description="是否可以绑定实盘参数")


class ModuleDataSchema(BaseModel):
    friendly_name: str = Field(description="模块显示名称")
    create_type: Optional[str] = ""

    desc: Optional[str] = Field(None, description="模块描述")
    visible: bool = Field(False, description="是否可见")
    cacheable: bool = Field(False, description="是否可缓存")
    opensource: bool = Field(False, description="是否开源")

    arguments: Optional[str] = "()"
    interface: List[ModuleInterface] = Field(default_factory=lambda: [], description="接口列表")
    source_code: Union[str, List[List[str]]] = Field(default_factory=lambda: [], description="模块源码")
    source_deps: Union[str, List[str]] = Field(default_factory=lambda: [], description="")
    main_func: Optional[str] = Field(None, description="主函数")
    post_func: Optional[str] = Field(None, description="后处理函数")

    doc: str = Field("", description="模块文档")
    doc_url: Optional[str] = ""

    category: str = Field("用户模块", description="模块分类")
    custom_module: bool = Field(False, description="是否是用户自定义模块")

    serviceversion: int = Field(1, description="模块版本")


class ModuleMetadataSchema(BaseModel):
    name: str = Field(description="模块名称")
    version: int = Field(description="模块版本")
    owner: str = Field(description="模块创建者")
    shared: bool = Field(False, description="是否共享")
    data: ModuleDataSchema
    rank: Optional[int] = Field(None, description="模块评分")

    updated_at: datetime = Field(default_factory=datetime.now, description="最后更新时间")
