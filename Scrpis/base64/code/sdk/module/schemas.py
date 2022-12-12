from datetime import datetime
from typing import Any, List, Optional, Union
from ..common.schemas import UpdatedAtMixin
from pydantic import BaseModel, Field
from .settings import MAIN_FACTION, DEFAULT_POST_RUN


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


class SetModuleCacheRequestSchema(BaseModel):
    key: str = Field(description="缓存 key")
    outputs_json: str = Field(description="缓存数据")
    is_papertrading: bool = Field(False, description="是否是模拟交易模式")


class GetModuleCacheRequestSchema(BaseModel):
    key: str = Field(description="缓存 key")
    is_papertrading: bool = Field(False, description="是否是模拟交易模式")


class GetModuleCacheResponseSchema(BaseModel):
    outputs_json: Optional[str] = Field(None, description="缓存数据")


class CustomModuleDataSchema(BaseModel):
    main_func: str = Field(..., description="主函数")
    post_func: str = Field(..., description="")
    source_code: Union[str, List[List[str]]] = Field(default_factory=lambda: [], description="模块源码")
    source_deps: Union[str, List[str]] = Field(default_factory=lambda: [], description="")
    cacheable: bool = Field(default=True, description="是否使用缓存")
    opensource: bool = Field(default=True, description="是否开放源码")
    friendly_name: str = Field(..., description="")
    category: str = Field(..., description="所属分类")
    desc: str = Field(default="", description="文档")
    doc_url: str = Field(default="", description="文档url")
    interface: List = Field(default=[], description="")
    visible: bool = Field(default=True, description="是否对外部可见， 用户在ipython中是否能调用")
    create_type: str = Field(default="", description="判断是创建还是更新 update/create")


class CreateCustomModuleRequest(BaseModel):
    name: str = Field(..., description="名称")
    shared: bool = Field(default=True, description="模块是否公开")
    data: CustomModuleDataSchema = Field(...)
    rank: float = Field(default=None, description="评分")

    class Config:
        schema_extra = {
            "examples": {
                "name": "test1",
                "shared": True,
                "data": {
                    "main_func": MAIN_FACTION,
                    "post_func": DEFAULT_POST_RUN,
                    "source_code": "",
                    "source_deps": [],
                    "cacheable": True,
                    "opensource": True,
                    "friendly_name": "friendly_name",
                    "category": "用户模块",
                    "desc": "",
                    "doc_url": "",
                    "interface": [
                        {
                            "type_name": None,
                            "desc": "绩效归因",
                            "specific_type_name": None,
                            "name": "perf_attribution",
                            "optional": False,
                            "type": "通用",
                            "type_code": "output_port",
                        },
                        {
                            "type_name": None,
                            "desc": "回测详细数据",
                            "specific_type_name": "DataSource",
                            "name": "backtest_ds",
                            "optional": False,
                            "type": "通用",
                            "type_code": "input_port",
                            "default": None,
                        },
                        {
                            "type_name": "String",
                            "desc": "策略基准， 默认为沪深300",
                            "specific_type_name": None,
                            "can_set_liverun_param": None,
                            "type_code": "str",
                            "name": "benchmark",
                            "default": "000300.HIX",
                        },
                    ],
                    "visible": True,
                    "create_type": "create",  # 判断是创建还是更新
                },
                "rank": 0,
            }
        }


class CustomModuleSchema(UpdatedAtMixin, BaseModel):
    name: str = Field(..., description="名称")
    version: int = Field(..., description="版本")
    owner: str = Field(..., description="拥有者")
    shared: bool = Field(default=True, description="模块是否公开")
    data: CustomModuleDataSchema = Field(...)
    rank: float = Field(default=None, description="评分")
