from kubernetes import client as kube_client

from .refresh_token import refresh_api_key_hook
from .settings import K8S_API_KEY, K8S_API_KEY_PREFIX, K8S_HOST, K8S_ENABLE_REFRESH_TOKEN

configuration = kube_client.Configuration(
    host=K8S_HOST,
    api_key=K8S_API_KEY,
    api_key_prefix=K8S_API_KEY_PREFIX,
)
configuration.verify_ssl = False

if K8S_ENABLE_REFRESH_TOKEN:
    configuration.refresh_api_key_hook = refresh_api_key_hook
    refresh_api_key_hook(configuration)

client = kube_client.ApiClient(configuration=configuration)

api = kube_client.CoreV1Api(client)
