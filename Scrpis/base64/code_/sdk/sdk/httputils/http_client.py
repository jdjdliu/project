from typing import Any, Dict, Optional

import orjson
import json
import requests as R

from sdk.auth import Credential
from sdk.exception import HTTPExceptions


class HTTPClient:
    def __init__(self: "HTTPClient", host: str, retry: int = 3) -> None:
        self.host = host
        self.retry = max(retry, 1)

    def get(
        self: "HTTPClient",
        path: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        credential: Optional[Credential] = None,
    ) -> Any:
        return self.__do_request("get", path=path, headers=headers, params=params, payload=payload, credential=credential)

    def post(
        self: "HTTPClient",
        path: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        credential: Optional[Credential] = None,
    ) -> Any:
        return self.__do_request("post", path=path, headers=headers, params=params, payload=payload, credential=credential)

    def put(
        self: "HTTPClient",
        path: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        credential: Optional[Credential] = None,
    ) -> Any:
        return self.__do_request("put", path=path, headers=headers, params=params, payload=payload, credential=credential)

    def patch(
        self: "HTTPClient",
        path: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        credential: Optional[Credential] = None,
    ) -> Any:
        return self.__do_request("patch", path=path, headers=headers, params=params, payload=payload, credential=credential)

    def delete(
        self: "HTTPClient",
        path: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        credential: Optional[Credential] = None,
    ) -> Any:
        return self.__do_request("delete", path=path, headers=headers, params=params, payload=payload, credential=credential)

    def __do_request(
        self: "HTTPClient",
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        credential: Optional[Credential] = None,
    ) -> Any:
        headers = headers or {}
        if credential is not None:
            headers["Authorization"] = credential.jwt

        headers["Content-Type"] = "application/json"

        exception = None
        for _ in range(self.retry):
            try:
                try:
                    data = orjson.dumps(payload)
                except Exception:  
                    # orjson can not handle numpy float
                    data = json.dumps(payload)

                response: R.Response = getattr(R, method.lower())(
                    self.host + path,
                    params=params,
                    headers=headers,
                    data=data,
                    timeout=60,
                )
                return self.__deal_with_response(response)
            except Exception as e:
                exception = e

        if exception is not None:
            raise exception

    def __deal_with_response(self: "HTTPClient", response: R.Response) -> Any:
        if response.status_code == 200:
            res = response.json().get("data")
            if res is None:
                res = response.json().get("body")
            return res
        elif response.status_code == 400:
            try:
                response_json: Dict[Any, Any] = response.json()
                exception = HTTPExceptions.get_exception_by_code(response_json.get("code", None))
                raise HTTPExceptions.CUSTOM_ERROR(response.text) if exception is None else exception
            except Exception:
                raise HTTPExceptions.UNKNOWN
        elif response.status_code == 401:
            raise HTTPExceptions.INVALID_TOKEN
        elif response.status_code == 422:
            raise HTTPExceptions.INVALID_PARAMS.clone().set_context(response.json())
        elif response.status_code == 500:
            raise HTTPExceptions.INTERNAL_SERVER_ERROR
        else:
            raise HTTPExceptions.UNKNOWN
