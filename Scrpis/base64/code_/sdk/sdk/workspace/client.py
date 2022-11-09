from typing import List, Type

from sdk.auth import Credential
from sdk.httputils import HTTPClient

from ..jwt.settings import JWT_HEADER_NAME
from .schemas import WorkspaceStatsSchema
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
