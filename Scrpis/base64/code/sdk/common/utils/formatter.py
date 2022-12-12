from typing import Any, Dict, TypeVar

T = TypeVar("T")


def format_everything(config: Any, format_map: Dict[str, Any]) -> Any:
    if isinstance(config, dict):
        return {k: format_everything(v, format_map) for k, v in config.items()}
    if isinstance(config, list):
        return [format_everything(v, format_map) for v in config]
    if isinstance(config, str):
        return config.format_map(format_map)
    return config
