from sdk.common import OSEnv

REDIS_URL = OSEnv.str("REDIS_URL", default="redis://localhost:6379")
REDIS_CLUSTER_MODE = OSEnv.bool("REDIS_CLUSTER_MODE", default=False)
