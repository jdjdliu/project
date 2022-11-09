from typing import Dict, Literal, Type, Union, overload
from urllib.parse import urlparse

from redis import Redis, RedisCluster

from .settings import REDIS_CLUSTER_MODE


class RedisClient:
    __cache: Dict[str, Union[Redis, RedisCluster]] = {}

    @overload
    @classmethod
    def get_client(cls: Type["RedisClient"], uri: str, cluster_mode: Literal[False]) -> Redis:
        ...

    @overload
    @classmethod
    def get_client(cls: Type["RedisClient"], uri: str, cluster_mode: Literal[True]) -> RedisCluster:
        ...

    @classmethod
    def get_client(cls: Type["RedisClient"], uri: str, cluster_mode: bool = REDIS_CLUSTER_MODE) -> Union[Redis, RedisCluster]:
        if uri in cls.__cache:
            return cls.__cache[uri]

        url = urlparse(uri)

        client_class = RedisCluster if cluster_mode else Redis

        cls.__cache[uri] = client_class(
            host=url.hostname,
            port=url.port or 6379,
            username=url.username,
            password=url.password,
            decode_responses=True,
        )

        return cls.__cache[uri]
