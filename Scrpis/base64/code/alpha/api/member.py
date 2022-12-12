from typing import List
from uuid import UUID

from alpha.constants import MemberRole
from alpha.models import Member, Repo
from alpha.schemas import (CreateMemberRequest, DeleteMemberRequest,
                           MemberSchema)
from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import ResponseSchema

router = APIRouter()


@router.get("", response_model=ResponseSchema[List[MemberSchema]])
async def get_member(
    repo_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[MemberSchema]]:
    member = await Member.get(repo_id=repo_id, user_id=credential.user.id)

    if member.role not in [MemberRole.OWNER, MemberRole.MANAGER]:
        raise HTTPExceptions.NO_PERMISSION  # 权限不足

    members = await Member.filter(repo_id=repo_id)
    return ResponseSchema(data=[MemberSchema.from_orm(member) for member in members])


@router.post("", response_model=ResponseSchema[MemberSchema])
async def create_member(
    request: CreateMemberRequest,
    credential: Credential = Depends(auth_required(True)),
) -> ResponseSchema[MemberSchema]:
    # Only <user> and <researcher> can be update
    if request.role not in [MemberRole.MANAGER, MemberRole.RESEARCHER]:
        raise HTTPExceptions.INVALID_MEMBER_TYPE

    await Repo.get(id=request.repo_id)

    current_user = await Member.get(repo_id=request.repo_id, user_id=credential.user.id)
    # TODO: request.user_id check, have to exist
    # only repo <owner> can add <manager>
    if not current_user.role == MemberRole.OWNER and request.role == MemberRole.MANAGER:
        raise HTTPExceptions.NO_PERMISSION

    # repo <owner> and <manager> can add <researcher>
    if current_user.role not in [MemberRole.OWNER, MemberRole.MANAGER]:
        raise HTTPExceptions.NO_PERMISSION

    repo_member = await Member.get_or_none(repo_id=request.repo_id, user_id=request.user_id)
    if repo_member:
        raise HTTPExceptions.EXISTED_NAME

    member = await Member.create(creator=credential.user.id, **request.dict())

    return ResponseSchema(data=MemberSchema.from_orm(member))


@router.delete("", response_model=ResponseSchema[MemberSchema])
async def delete_member(
    request: DeleteMemberRequest,
    credential: Credential = Depends(auth_required(True)),
) -> ResponseSchema[MemberSchema]:

    await Repo.get(id=request.repo_id)

    current_member = await Member.get(repo_id=request.repo_id, user_id=credential.user.id)
    repo_member = await Member.get(repo_id=request.repo_id, user_id=request.user_id)

    if repo_member.role == MemberRole.OWNER:  # can not delete owner
        if current_member.role == MemberRole.OWNER:
            raise HTTPExceptions.CUSTOM_ERROR("无法删除")
        else:
            raise HTTPExceptions.NO_PERMISSION

    # Only owner can delete manager
    if repo_member.role == MemberRole.MANAGER and not current_member.role == MemberRole.OWNER:
        raise HTTPExceptions.NO_PERMISSION

    # manager and owner can delete researcher
    if repo_member.role == MemberRole.RESEARCHER and current_member.role not in [MemberRole.MANAGER, MemberRole.OWNER]:
        raise HTTPExceptions.NO_PERMISSION

    await repo_member.delete()

    return ResponseSchema(data=MemberSchema.from_orm(repo_member))
