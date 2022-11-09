from datetime import datetime
from typing import Any, Dict, Optional, Type
from uuid import UUID

from pydantic import BaseModel, Field
from sdk.exception import HTTPExceptions
from sdk.jwt import JWT

from .settings import AUTH_TOKEN


class UserSchema(BaseModel):
    id: UUID = Field(..., description="用户ID")
    username: str = Field(..., description="用户姓名")


class Credential(BaseModel):
    user: UserSchema = Field(description="用户信息")

    jwt: str = Field(description="Bearer Token")

    iat: Optional[datetime] = Field(description="签发时间")
    exp: Optional[datetime] = Field(description="过期时间")
    iss: Optional[str] = Field(description="签发方")
    aud: Optional[str] = Field(description="接收方")
    jti: Optional[str] = Field(description="JWT ID")

    def is_anonymous(self: "Credential") -> bool:
        return self.user is None or self.jwt is None

    @classmethod
    def from_anonymous(cls: Type["Credential"]) -> "Credential":
        return cls()

    @classmethod
    def from_jwt(cls: Type["Credential"], jwt: str) -> "Credential":
        payload: Dict[str, Any] = JWT.parse_jwt_to_obj(jwt)[1]

        return Credential(
            user=UserSchema(id=payload["userId"], username=payload["sub"]),
            jwt=jwt,
            iat=payload.get("iat", None),
            exp=payload.get("exp", None),
            iss=payload.get("iss", None),
            aud=payload.get("aud", None),
            jti=payload.get("jti", None),
        )

    @classmethod
    def from_env(cls: Type["Credential"]) -> "Credential":
        token = AUTH_TOKEN
        if not token:
            raise HTTPExceptions.INVALID_TOKEN
        return cls.from_jwt(token)

    # @classmethod
    # def from_user(cls: Type["Credential"], user: dict) -> "Credential":
    #     now = datetime.now()

    #     payload = {
    #         "user": user,
    #         "iss": JWT_ISSUER,
    #         "iat": now,
    #         "exp": now + timedelta(hours=JWT_EXPIRE),
    #     }

    #     if JWT_ENABLE_JTI:
    #         payload["jti"] = str(uuid4())

    #     return Credential(
    #         user=user,
    #         token=jwt.encode(payload, key=JWT_SECRET, algorithm=JWT_SIGN_ALGO),
    #         iss=payload["iss"],
    #         iat=payload["iat"],
    #         exp=payload["exp"],
    #         jti=payload.get("jti", None),
    #     )
