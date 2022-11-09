from typing import List

from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema
from sdk.share.schemas import TemplateFileSchema
from userservice.models import TemplateFile

router = APIRouter()


@router.get("/{userbox_id}", response_model=ResponseSchema[TemplateFileSchema])
async def list_template_catalog(
    userbox_id: int,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[str]]:
    template = await TemplateFile.get(userbox_id=userbox_id)
    return ResponseSchema(data=TemplateFileSchema.from_orm(template))


# ! 只对工作人员开放， 用于添加模板
@router.post("", response_model=ResponseSchema[TemplateFileSchema])
async def create_template(
    request: TemplateFileSchema,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[TemplateFileSchema]:
    template_file = await TemplateFile.get_or_none(userbox_id=request.userbox_id)
    if template_file:
        template_file.update_from_dict({"templates": request.templates})
        await template_file.save()
    else:
        template_file = await TemplateFile.create(
            userbox_id=request.userbox_id,
            templates=request.templates,
        )

    return ResponseSchema(data=TemplateFileSchema.from_orm(template_file))
