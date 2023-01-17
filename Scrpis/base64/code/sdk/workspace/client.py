from typing import List, Type

from sdk.auth import Credential
from sdk.httputils import HTTPClient

from ..jwt.settings import JWT_HEADER_NAME
from .schemas import WorkspaceStatsSchema, WorkspaceResourceSchema
from .settings import WORKSPACE_HOST


class WorkSpaceClient:

    HttpClient = HTTPClient(WORKSPACE_HOST)

    @classmethod
    def get_all_workspace_status(
        cls: Type["WorkSpaceClient"],
        credential: Credential,
    ) -> List[WorkspaceStatsSchema]:
        return [
            WorkspaceStatsSchema(**stats)
            for stats in cls.HttpClient.get(
                f"/api/workspaces/user/{credential.user.id.hex}/status",
                headers={
                    JWT_HEADER_NAME: credential.jwt,
                },
            )
        ]

    @classmethod
    def get_all_resource(
        cls: Type["WorkSpaceClient"],
        credential: Credential,
    ) -> List[WorkspaceResourceSchema]:
        response = [
            WorkspaceResourceSchema(**i)
            for i in cls.HttpClient.get(
                f"/api/workspaces/resources",
                headers={
                    JWT_HEADER_NAME: credential.jwt,
                },
            )
        ]
        return response

    @classmethod
    def start_userbox(
        cls: Type["WorkSpaceClient"],
        workspace_id: int,
        source_id: int,
        credential: Credential,
    ) -> str:  # 工作台类型, 1:股票, 2.指数, 4. 期货
        response = cls.HttpClient.post(
            f"/api/workspaces/{workspace_id}/user/{credential.user.id.hex}/restart",
            payload={"id": source_id},
            headers={
                JWT_HEADER_NAME: credential.jwt,
            },
        )
        return response