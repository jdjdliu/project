from typing import Type

from sdk.auth import Credential
from sdk.httputils import HTTPClient

from .schemas import CreateNotebookShareRequest, ShareSchema
from .settings import USERSERVICE_HOST


class ShareClient:

    HttpClient = HTTPClient(USERSERVICE_HOST)

    @classmethod
    def create_notebook_html(
        cls: Type["ShareClient"],
        params: CreateNotebookShareRequest,
        credential: Credential,
    ) -> ShareSchema:
        return cls.HttpClient.post("/api/userservice/share", payload=params.dict(), credential=credential)  # type: ignore

    @classmethod
    def render_notebook(
        cls: Type["ShareClient"],
        params: CreateNotebookShareRequest,
        credential: Credential,
    ) -> str:
        return cls.HttpClient.post("/api/userservice/share/render", payload=params.dict(), credential=credential)  # type: ignore
