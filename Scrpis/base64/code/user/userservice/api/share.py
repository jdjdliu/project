import datetime
import json
from uuid import UUID

from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema
from sdk.share import CreateNotebookShareRequest, ShareSchema
from starlette.responses import HTMLResponse, JSONResponse
from userservice.models import Share, ShareData
from userservice.utils.share import convert_to_html

router = APIRouter()


@router.get("/{share_id}", response_class=HTMLResponse)
async def get_shared_html_by_id(share_id: UUID) -> HTMLResponse:
    share = await Share.get(id=share_id)
    return HTMLResponse(share.html)


@router.get("/data/{share_data_id}", response_class=JSONResponse)
async def get_shared_data_by_id(share_data_id: UUID) -> JSONResponse:
    share = await ShareData.get(id=share_data_id)
    return JSONResponse(content=json.loads(share.data))


@router.post("", response_model=ResponseSchema[ShareSchema])
async def create_share(
    request: CreateNotebookShareRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[ShareSchema]:
    share = await Share.create(
        name=request.name,
        creator=credential.user.id,
        notebook=request.notebook,
        html=await convert_to_html(request),
    )
    return ResponseSchema(data=share)


@router.post("/render", response_model=ResponseSchema[str])
async def render_only(request: CreateNotebookShareRequest) -> ResponseSchema[str]:
    return ResponseSchema(data=await convert_to_html(request))
