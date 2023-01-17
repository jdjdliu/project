import base64
import json
import re
import time
from datetime import datetime
from urllib.parse import quote

import requests
from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.exception import HTTPExceptions
from sdk.httputils import ResponseSchema
from sdk.share.schemas import CreateCloneRequest, JupyterCloneParams, JupyterCloneSchema
from sdk.workspace import WorkSpaceClient, WorkSpaceStatus

from ..settings import WEB_HOST,GATEWAY_HOST,AIWE_HOST

router = APIRouter()


@router.post("", response_model=ResponseSchema)
async def create_clone_file(
    request: CreateCloneRequest,
    credential: Credential = Depends(auth_required()),
):
    workspace_stats = WorkSpaceClient.get_all_workspace_status(credential)

    enable_workspace_list = [workspace.id for workspace in workspace_stats if workspace.status == WorkSpaceStatus.RUNNING]
    workspaceType = [workspace.workspaceType for workspace in workspace_stats if workspace.status ==  WorkSpaceStatus.RUNNING]
    if not enable_workspace_list:
        raise HTTPExceptions.INVALID_WORKSPACE_STATS

    clone_file_to_notebook(request=request, enable_workspace=enable_workspace_list[0], credential=credential)
    return ResponseSchema(data={"id":enable_workspace_list[0],"workspaceType":workspaceType[0],"AIWE_HOST":AIWE_HOST})


def clone_file_to_notebook(
    request: CreateCloneRequest,
    enable_workspace: int,
    credential: Credential,
) -> None:
    content = base64.b64encode(base64.urlsafe_b64decode(request.notebook))
    j_content = json.loads(base64.b64decode(content))
    j_content["cells"][0]["output"] = []
    new_content = base64.b64encode(json.dumps(j_content).encode()).decode()

    name = re.sub(r"(__\d{4}-\d{4})|\.ipynb", "", request.name)
    params = JupyterCloneParams(
        content=new_content,
        format="base64",
        name=f"{name}__{datetime.now().strftime('%m%d-%H%M')}.ipynb",
        path=f"{name}__{datetime.now().strftime('%m%d-%H%M')}.ipynb",
        type="file",
    )

    cookies = {"AIWE_QUANTPLATFORM_TOKEN": credential.jwt}
    _s = requests.get(f"{GATEWAY_HOST}/user/{credential.user.id.hex}/{enable_workspace}/lab")
    xsrf = _s.headers["Set-Cookie"].split(";")[0].split("=")[1]
    url = f"{GATEWAY_HOST}/user/{credential.user.id.hex}/{enable_workspace}/api/contents/{quote(params.name, 'utf-8')}?{time.time():.0f}"
    headers = {"Authorization": credential.jwt, "X-XSRFToken": xsrf}
    cookies["_xsrf"] = xsrf
    requests.put(url=url, json=params.dict(), headers=headers, cookies=cookies)
