from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema
from sdk.module import GetModuleCacheRequestSchema, GetModuleCacheResponseSchema, SetModuleCacheRequestSchema

from userservice.utils.module_cache import ModuleCacheManager

router = APIRouter()


@router.get("", response_model=ResponseSchema[GetModuleCacheResponseSchema])
def get_module_cache(
    request: GetModuleCacheRequestSchema = Depends(),
    credential: Credential = Depends(auth_required(allow_anonymous=False)),
) -> ResponseSchema[GetModuleCacheResponseSchema]:
    return ResponseSchema(data=ModuleCacheManager.get_module_cache(request, credential))


@router.post("", response_model=ResponseSchema)
def set_module_cache(
    request: SetModuleCacheRequestSchema,
    credential: Credential = Depends(auth_required(allow_anonymous=False)),
) -> ResponseSchema:
    return ResponseSchema(data=ModuleCacheManager.set_module_cache(request, credential))
