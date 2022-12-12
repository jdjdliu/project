from typing import Final

from sdk.common import OSEnv

JWT_EXPIRE: Final = OSEnv.int("JWT_EXPIRE", 24 * 7, description="过期时间 (小时)")
JWT_ISSUER: Final = OSEnv.str("JWT_ISSUER", "bigquant", description="JWT 签发方")
JWT_ENABLE_JTI: Final = OSEnv.bool("JWT_ENABLE_JTI", True, description="是否启用 JWT ID 唯一标识")
JWT_SIGN_ALGO: Final = OSEnv.str("JWT_SIGN_ALGO", "SM3WithSM2", description="HASH 算法")
JWT_LEEWAY: Final = OSEnv.int("JWT_LEEWAY", 0, description="JWT 过期时间容错值")
JWT_HEADER_NAME: Final = OSEnv.str("JWT_HEADER_NAME", "Authorization", description="JWT Header Name")
JWT_COOKIE_NAME: Final = OSEnv.str("JWT_COOKIE_NAME", "AIWE_QUANTPLATFORM_TOKEN", description="JWT Cookie Name")

# FIXME: these two keys only for local dev test, do not use in production
JWT_PRIVATE_KEY: Final = OSEnv.str(
    "JWT_PRIVATE_KEY",
    default="3a46ff4e2dcf92957279679ce0e510ac60f6d46e65e394c2dc2e9c2ab16b2895",
    description="JWT 私钥",
)
JWT_PUBLIC_KEY: Final = OSEnv.str(
    "JWT_PUBLIC_KEY",
    default="04231875fa10d73322a5d49a241f332255ef348675f3e136c58644952073201049e5ee2f4c83c81c04e7bb5e4dbd773f63d6ef83a48355fb8563bf73f61fb74ac8",
    description="JWT 公钥",
)
