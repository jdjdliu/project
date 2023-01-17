from typing import Callable, Optional

from fastapi import Cookie, Header, HTTPException, status
from sdk.exception import HTTPExceptions
from sdk.jwt import JWT, JWT_COOKIE_NAME, JWT_HEADER_NAME, JWT_LEEWAY, JWT_PUBLIC_KEY, JWT_SIGN_ALGO

from .schemas import Credential
from .settings import LOGIN_URL

exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail={
        "code": HTTPExceptions.INVALID_TOKEN.code,
        "message": HTTPExceptions.INVALID_TOKEN.message,
        "login_url": LOGIN_URL,
    },
)


def auth_required(allow_anonymous: bool = False) -> Callable[[Optional[str], Optional[str]], Optional[Credential]]:
    def wrapper(
        header_jwt: Optional[str] = Header(None, alias=JWT_HEADER_NAME),
        cookie_jwt: Optional[str] = Cookie(None, alias=JWT_COOKIE_NAME),
    ) -> Optional[Credential]:
        jwt = header_jwt or cookie_jwt or None
        if not jwt and allow_anonymous:
            return None

        if not jwt:
            raise exception

        try:
            JWT.decode(jwt=jwt, leeway=JWT_LEEWAY, algorithm=JWT_SIGN_ALGO, cipher_params={"public_key": JWT_PUBLIC_KEY})

            return Credential.from_jwt(jwt)
        except Exception:
            raise exception

    return wrapper
