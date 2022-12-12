from typing import List

from fastapi import APIRouter, Body, Depends
from sdk.auth import Credential, auth_required
from sdk.auth.schemas import UserSchema
from sdk.httputils import ResponseSchema
from userservice.schemas import CreateCustomModuleRequest, CustomModuleSchema
from userservice.utils.custom_module_manger import CustomModuleManager
from userservice.utils.module_manager import ModuleManager

router = APIRouter()


@router.get("", response_model=ResponseSchema[List])
def get_modules(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List]:
    return ResponseSchema(data=ModuleManager.get_modules_v2(credential))


@router.get("/module_is_valid/{module_name}", response_model=ResponseSchema)
def module_is_valid(
    module_name: str,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema:
    return ResponseSchema(data=CustomModuleManager.module_is_valid(module_name=module_name, credential=credential))


@router.get("/custom", response_model=ResponseSchema[List])
def list_custom_modules(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List]:
    return ResponseSchema(data=CustomModuleManager.list(credential))


@router.post("/custom", response_model=ResponseSchema[CustomModuleSchema])
def create_module(
    request: CreateCustomModuleRequest = Body(..., example=CreateCustomModuleRequest.Config.schema_extra["examples"]),
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[CustomModuleSchema]:
    data = dict(request.data)
    data["created_by"] = credential.user.username
    data["custom_module"] = True  # 是否为用户模块

    module = CustomModuleManager.create(
        name=request.name,
        shared=request.shared,
        data=data,
        credential=credential,
    )
    return ResponseSchema(data=CustomModuleSchema(**module))


@router.delete("/custom", response_model=ResponseSchema[str])
def delete_module(
    module_name: str,
    version: int,
    credential: Credential = Depends(auth_required()),
):
    pass
