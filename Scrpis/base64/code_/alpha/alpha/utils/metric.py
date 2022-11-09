from typing import Any, Callable, Dict


class Metric:
    def __init__(self: "Metric", key: str, name: str, getter: Callable[[Dict[Any, Any]], Any]) -> None:
        self.key = key
        self.name = name
        self.getter = getter

        self.hash_value = hash(self.key)

    def __hash__(self: "Metric") -> int:
        return self.hash_value


Metrics = [
    Metric("ic_mean", name="IC均值", getter=lambda d: d["ic_mean"]),
]
