from uuid import UUID
from fastapi import APIRouter, Depends, Response
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema
from sdk.jwt import JWT
from sdk.exception import HTTPExceptions
from userservice.settings import WEB_HOST
from sdk.workspace import WorkSpaceClient
from sdk.workspace.constants import WorkSpaceStatus
from sdk.workspace.schemas import WorkspaceStatsSchema

router = APIRouter()


USERBOX_PATH = f"{WEB_HOST}/user/{{user_id_hex}}/{{workspace_type}}/lab"


@router.get("/status", response_model=ResponseSchema[int])
async def get_workspce_status(
    # user_id: UUID,
    workspace_type: str="期货",
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[int]:
    """
    NOT_START = 0
    RUNNING = 1
    STARING = 2
    """
    lab = None

    # if not user_id == credential.user.id:
    #     new_token = JWT.encode({"userId": user_id, "sub": user_id})
    #     credential = Credential.from_jwt(new_token)
    response.set_cookie(key="AIWE_QUANTPLATFORM_TOKEN", value=credential.jwt)

    userbox_status = WorkSpaceClient.get_all_workspace_status(credential=credential)
    for userbox in userbox_status:
        if workspace_type in userbox.workspaceType:
            lab = userbox
            break
            
    if not lab:
        raise HTTPExceptions.WORPSACE_NOT_FUND
    return ResponseSchema(data=lab.status)


@router.get("/path", response_model=ResponseSchema[str])
async def get_userbox_path(
    # user_id: UUID,
    response: Response,
    workspace_type: str="期货",
    restart: bool = False,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[str]:

    lab = None

    # if not user_id == credential.user.id:
    #     new_token = JWT.encode({"userId": user_id, "sub": user_id})
    #     credential = Credential.from_jwt(new_token)
    response.set_cookie(key="AIWE_QUANTPLATFORM_TOKEN", value=credential.jwt)

    userbox_status = WorkSpaceClient.get_all_workspace_status(credential=credential)

    for userbox in userbox_status:
        if workspace_type in userbox.workspaceType:
            lab = userbox
            # filte workspace type
            if not userbox.status == WorkSpaceStatus.RUNNING or restart:
                # start userbox
                source_types = WorkSpaceClient.get_all_resource(credential)
                source_type:WorkspaceResourceSchema = max(source_types, key=lambda x: x.cpu+int(x.mem.split('G')[0]))  # choose best source
                WorkSpaceClient.start_userbox(workspace_id=lab.id, source_id=source_type.id, credential=credential)
            break
    if not lab:
        raise HTTPExceptions.WORPSACE_NOT_FUND
    return ResponseSchema(data=USERBOX_PATH.format(user_id_hex=credential.user.id.hex, workspace_type=lab.id))