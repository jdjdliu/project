from typing import List

from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema
from userservice.models import User
from userservice.schemas import UserSchema

router = APIRouter()


@router.get("", response_model=ResponseSchema[List[UserSchema]])
async def get_all_users(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[UserSchema]]:
    # await sync_user_info()
    return ResponseSchema(data=[UserSchema.from_orm(user) for user in await User.all()])
