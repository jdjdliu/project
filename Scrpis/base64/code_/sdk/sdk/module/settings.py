from sdk.common import OSEnv

USERSERVICE_HOST = OSEnv.str("USERSERVICE_HOST", default="http://userservice")
