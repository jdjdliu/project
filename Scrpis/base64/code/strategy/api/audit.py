from typing import Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import ResponseSchema
from sdk.strategy import AuditSchema, AuditStatus, AuditWithStrategySchema, CreateAuditRequest, MemberRole, StrategySchema, UpdateAuditRequest
from strategy.models import Audit, Member, Repo, Strategy

router = APIRouter()


@router.get("", response_model=ResponseSchema[List[AuditWithStrategySchema]])
async def get_audit_and_strategy_in_repo(
    repo_id: UUID,
    status: Optional[AuditStatus] = None,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[AuditWithStrategySchema]]:
    await Member.get(repo_id=repo_id, user_id=credential.user.id)

    queryset = Audit.filter(repo_id=repo_id)

    if status:
        queryset = queryset.filter(status=status)

    audits = await queryset

    strategies_mapping: Dict[int, Strategy] = {strategy.id: strategy for strategy in await Strategy.filter(id__in=[audit.strategy_id for audit in audits])}
    return ResponseSchema(
        data=[
            AuditWithStrategySchema(
                audit=AuditSchema.from_orm(audit),
                strategy=StrategySchema.from_orm(strategies_mapping[audit.strategy_id]),
            )
            for audit in audits
        ]
    )


@router.post("")
async def add_strategy_to_repo(
    request: CreateAuditRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[AuditSchema]:
    await Strategy.get(id=request.strategy_id)
    await Repo.get(id=request.repo_id)
    audit = await Audit.filter(strategy_id=request.strategy_id, repo_id=request.repo_id).first()

    if audit is None:
        audit = await Audit.create(
            strategy_id=request.strategy_id,
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
