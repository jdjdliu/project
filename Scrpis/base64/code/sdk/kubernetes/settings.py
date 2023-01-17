from sdk.common import OSEnv

K8S_AUTH_HOST = OSEnv.str("K8S_AUTH_HOST", default="http://aipower3.cmbchina.cn", description="认证地址")
K8S_USERNAME = OSEnv.str("K8S_USERNAME", default="aipower-275766")
K8S_PASSWORD = OSEnv.str("K8S_PASSWORD", default="Aipower@2022")
K8S_API_KEY_REFRESH_INTERVAL = OSEnv.int("K8S_API_KEY_REFRESH_INTERVAL", default=3, description="api key 刷新过期")

K8S_HOST = OSEnv.str("K8S_HOST", default="http://localhost", description="K8S host")
K8S_API_KEY = OSEnv.json("K8S_API_KEY", default={}, description="K8S API Key")
K8S_API_KEY_PREFIX = OSEnv.json("K8S_API_KEY_PREFIX", default={}, description="K8S API Key Prefix")
K8S_ENABLE_REFRESH_TOKEN = OSEnv.bool("K8S_ENABLE_REFRESH_TOKEN", default=False)
K8S_NAMESPACE = OSEnv.str("K8S_NAMESPACE", default="bigquant", description="K8S 命名空间")
