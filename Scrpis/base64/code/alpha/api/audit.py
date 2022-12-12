from typing import Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends

from alpha.models import Alpha, Audit, Member, Repo
from sdk.alpha import AlphaSchema, AuditSchema, AuditStatus, AuditWithAlphaSchema, CreateAuditRequest, MemberRole, UpdateAuditRequest
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import ResponseSchema

router = APIRouter()


@router.get("", response_model=ResponseSchema[List[AuditWithAlphaSchema]])
async def get_audit_and_alphas_in_repo(
    repo_id: UUID,
    status: Optional[AuditStatus] = None,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[AuditWithAlphaSchema]]:
    await Member.get(repo_id=repo_id, user_id=credential.user.id)

    queryset = Audit.filter(repo_id=repo_id)

    if status:
        queryset = queryset.filter(status=status)

    audits = await queryset

    alphas_mapping: Dict[int, Alpha] = {alpha.id: alpha for alpha in await Alpha.filter(id__in=[audit.alpha_id for audit in audits])}
    return ResponseSchema(
        data=[
            AuditWithAlphaSchema(
                audit=AuditSchema.from_orm(audit),
                alpha=AlphaSchema.from_orm(alphas_mapping[audit.alpha_id]),
            )
            for audit in audits
        ]
    )


@router.post("")
async def add_alpha_to_repo(
    request: CreateAuditRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[AuditSchema]:
    await Alpha.get(id=request.alpha_id)
    await Repo.get(id=request.repo_id)
    audit = await Audit.filter(alpha_id=request.alpha_id, repo_id=request.repo_id).first()

    if audit is None:
        audit = await Audit.create(
            alpha_id=request.alpha_id,
            repo_id=request.repo_id,
            creator=credential.user.id,
            status=AuditStatus.PENDING,
        )

    return ResponseSchema(data=AuditSchema.from_orm(audit))


@router.put("/{audit_id}")
async def update_audit_status(
    audit_id: UUID,
    request: UpdateAuditRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[AuditSchema]:
    audit = await Audit.get(id=audit_id)
    auditor = await Member.get(user_id=credential.user.id, repo_id=audit.repo_id)

    if auditor.role not in [MemberRole.OWNER, MemberRole.MANAGER]:
        raise HTTPExceptions.NO_PERMISSION

    await audit.update_from_dict({"status": request.status, "auditor": credential.user.id}).save()

    return ResponseSchema(data=AuditSchema.from_orm(audit))


@router.delete("/{audit_id}")
async def delete_audit(
    audit_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[AuditSchema]:
    audit = await Audit.get(id=audit_id, status=AuditStatus.REJECTED)
    auditor = await Member.get(user_id=credential.user.id, repo_id=audit.repo_id)

    if audit.status == AuditStatus.APPROVED:
        if auditor.role not in [MemberRole.OWNER, MemberRole.MANAGER]:
            raise HTTPExceptions.NO_PERMISSION
    elif auditor.role not in [MemberRole.OWNER, MemberRole.MANAGER] and audit.creator != credential.user.id:
        raise HTTPExceptions.NO_PERMISSION

    await audit.delete()

    return ResponseSchema(data=AuditSchema.from_orm(audit))
