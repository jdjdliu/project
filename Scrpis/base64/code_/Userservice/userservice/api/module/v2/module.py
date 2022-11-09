from typing import List

from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema

from userservice.utils.module_manager import ModuleManager

router = APIRouter()


@router.get("", response_model=ResponseSchema[List])
def get_modules(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List]:
    return ResponseSchema(data=ModuleManager.get_modules_v2(credential))
