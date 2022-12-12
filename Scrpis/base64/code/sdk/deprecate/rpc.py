import warnings
from typing import Any, Dict

from .settings import AMQP_CONF


def invoke_micro_service(
    service: str,
    method: str,
    amqp_uri: str = AMQP_CONF,
    timeout: int = 60,
    call_async: bool = False,
    is_pickle: bool = False,
    **kwargs: Any,
) -> Any:
    warnings.warn("invoke_micro_service will be depericated, use http api as soon as possible.", DeprecationWarning)

    nameko_config: Dict[str, Any] = {"AMQP_URI": amqp_uri}
    if is_pickle:
        nameko_config.update({"serializer": "pickle", "ACCEPT": ["pickle", "json"]})
    # TODO: remove this. async更改为call_async，前者是保留关键字，兼容旧代码调用
    if "async" in kwargs:
        call_async = kwargs.pop("async")
    return invoke_micro_service2(
        service,
        method,
        nameko_config=nameko_config,
        timeout=timeout,
        call_async=call_async,
        **kwargs,
    )


def invoke_micro_service2(
    service: str,
    method: str,
    nameko_config: Dict[Any, Any] = {},
    timeout: int = 60,
    call_async: bool = False,
    **kwargs: Any,
) -> Any:
    from nameko.rpc import MethodProxy
    from nameko.standalone.rpc import ClusterRpcProxy

    nameko_timeout = timeout

    # TODO: remove this. async更改为call_async，前者是保留关键字，兼容旧代码调用
    if "async" in kwargs:
        call_async = kwargs.pop("async")

    with ClusterRpcProxy(nameko_config, timeout=nameko_timeout) as cluster_rpc:
        remote_service = cluster_rpc[service]
        remote_method = MethodProxy(remote_service.worker_ctx, remote_service.service_name, method.rstrip("/"), remote_service.reply_listener)
        if call_async:
            remote_method = remote_method.call_async
        return remote_method(**kwargs)
