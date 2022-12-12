import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Type, Union

from dateutil.parser import isoparse

from sdk.common import base64
from sdk.crypto import get_cipher
from sdk.exception import HTTPExceptions

from .settings import JWT_PRIVATE_KEY, JWT_PUBLIC_KEY, JWT_SIGN_ALGO


class JWT:
    @classmethod
    def encode(
        cls: Type["JWT"],
        payload: Dict[str, Any],
        algorithm: str = JWT_SIGN_ALGO,
        cipher_params: Optional[Dict[str, str]] = None,
    ) -> str:
        cipher = get_cipher(algorithm)(**{"private_key": JWT_PRIVATE_KEY, **(cipher_params or {})})

        h = base64.urlsafe_b64encode(json.dumps({"typ": "JWT", "alg": algorithm}, ensure_ascii=False).encode()).decode("ascii").strip("=")
        p = base64.urlsafe_b64encode(json.dumps(payload, ensure_ascii=False, default=str).encode()).decode("ascii").strip("=")
        s = base64.urlsafe_b64encode(cipher.sign(f"{h}.{p}").hex().encode()).decode("ascii").strip("=")

        return f"{h}.{p}.{s}"

    @classmethod
    def decode(
        cls: Type["JWT"],
        jwt: str,
        algorithm: str = JWT_SIGN_ALGO,
        cipher_params: Optional[Dict[str, str]] = None,
        leeway: int = 0,
    ) -> Dict[str, Any]:
        header, payload, signature = cls.parse_jwt(jwt)

        cipher = get_cipher(algorithm)(**{"public_key": JWT_PUBLIC_KEY, **(cipher_params or {})})
        if cipher.verify(signature=bytes.fromhex(base64.urlsafe_b64decode(signature).decode()), message=f"{header}.{payload}"):
            payload_data: Dict[str, Any] = json.loads(base64.urlsafe_b64decode(payload).decode())

            cls._check_exp(exp=payload_data.get("exp", None), leeway=leeway)

            return payload_data

        raise HTTPExceptions.INVALID_TOKEN

    @staticmethod
    def parse_jwt(jwt: str) -> Tuple[str, str, str]:
        parts = jwt.split(".")

        if len(parts) != 3:
            raise HTTPExceptions.INVALID_TOKEN

        return parts[0], parts[1], parts[2]

    @classmethod
    def parse_jwt_to_obj(cls: Type["JWT"], jwt: str) -> Tuple[Dict[str, str], Dict[str, Any], str]:
        header, payload, signature = cls.parse_jwt(jwt)
        return (
            json.loads(base64.urlsafe_b64decode(header).decode()),
            json.loads(base64.urlsafe_b64decode(payload).decode()),
            signature,
        )

    @staticmethod
    def _check_exp(exp: Optional[Union[str, int, float]] = None, leeway: int = 0) -> None:
        if exp is None:
            return

        if isinstance(exp, str):
            expire_at = isoparse(exp)
        elif len(str(int(exp))) == 10:
            expire_at = datetime.fromtimestamp(exp)
        else:
            expire_at = datetime.fromtimestamp(exp / 1000)

        if (expire_at + timedelta(seconds=leeway)) > datetime.now():
            return

        raise HTTPExceptions.INVALID_TOKEN
