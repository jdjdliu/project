from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import ResponseSchema
from strategy.constants import MemberRole
from strategy.models import Member, Repo
from strategy.schemas import CreateRepoRequest, RepoSchema, RepoWithMember, UpdateRepoRequest
from tortoise.queryset import Q

router = APIRouter()


@router.get("", response_model=ResponseSchema[List[RepoWithMember]])
async def get_all_repos(
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[RepoWithMember]]:
    members_mapping = {member.repo_id: member for member in await Member.filter(user_id=credential.user.id)}
    repos = await Repo.filter(Q(id__in=list(members_mapping.keys())) | Q(is_public=True)).order_by("id")
    return ResponseSchema(data=[RepoWithMember(repo=repo, member=members_mapping.get(repo.id, None)) for repo in repos])


@router.get("/{repo_id}", response_model=ResponseSchema[RepoWithMember])
async def get_repo_detail(
    repo_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[RepoWithMember]:
    repo = await Repo.get(id=repo_id)
    member = await Member.filter(repo_id=repo_id, user_id=credential.user.id).first()  # frist

    if member is None and not repo.is_public:
        raise HTTPExceptions.NO_PERMISSION
    return ResponseSchema(data=RepoWithMember(repo=repo, member=member))


@router.post("", response_model=ResponseSchema[RepoSchema])
async def create_repo(
    request: CreateRepoRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[RepoSchema]:

    repo = await Repo.get_or_none(name=request.name, creator=credential.user.id)
    if repo:  # repo name has existed
        raise HTTPExceptions.EXISTED_NAME

    repo = await Repo.create(
        name=request.name,
        description=request.description,
        is_public=request.is_public,
        creator=credential.user.id,
    )

    # create member too
    await Member.create(user_id=credential.user.id, creator=credential.user.id, repo_id=repo.id, role=MemberRole.OWNER)

    return ResponseSchema(data=RepoSchema.from_orm(repo))


@router.put("/{repo_id}", response_model=ResponseSchema[RepoSchema])
async def update_repo(
    repo_id: UUID,
    request: UpdateRepoRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[RepoSchema]:
    repo = await Repo.get(id=repo_id)
    member = await Member.filter(repo_id=repo_id, user_id=credential.user.id).first()

    if member is None or member.role != MemberRole.OWNER:
        raise HTTPExceptions.NO_PERMISSION

    existed_repo = await Repo.filter(name=request.name).first()
    if existed_repo is not None and existed_repo.id != repo.id:
        raise HTTPExceptions.EXISTED_NAME

    # just owner can update repo
    await repo.update_from_dict({"name": request.name, "description": request.description, "is_public": request.is_public}).save()
    return ResponseSchema(data=RepoSchema.from_orm(repo))
