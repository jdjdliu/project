from sdk.common import OSEnv

USER_PROVIDER = OSEnv.str("USER_PROVIDER", "cmb", description="用户信息提供方")
AUTH_TOKEN = OSEnv.str("AUTH_TOKEN", default="", description="用户认证令牌 (环境变量)")
LOGIN_URL = OSEnv.str("LOGIN_URL", default="", description="登陆地址")
