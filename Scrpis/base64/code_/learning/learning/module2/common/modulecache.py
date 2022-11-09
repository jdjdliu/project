import hashlib
import inspect
import json
from typing import Any

import numpy as np
import pandas as pd
from sdk.auth import Credential
from sdk.module import GetModuleCacheRequestSchema, ModuleClient, SetModuleCacheRequestSchema
from sklearn.base import BaseEstimator

try:
    # sklearn.externals.joblib is deprecated in 0.21 and will be removed in 0.23.
    from sklearn.externals.joblib import hashing
except Exception:
    # TODO: keep hashing only
    from joblib import hashing

from sdk.utils import BigLogger

from learning import settings
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.sourceinspector import bq_protected_inspect_getsource, bq_protected_is_cython_function, bq_protected_sig_for_cythonized
from learning.module2.common.utils import isinstance_with_name

log = BigLogger("modulecache")


def _cache_key_encoder(obj):
    if isinstance_with_name(obj, DataSource):
        return {"id": obj.id, "__class__": "DataSource"}
    if isinstance_with_name(obj, pd.DataFrame):
        return {"json_md5": obj.to_json(), "__class__": "DataFrame"}
    # if isinstance_with_name(obj, pd.Panel):
    #     return {'json_md5': obj.to_frame(False).to_json(), '__class__': 'Panel'}
    if inspect.isfunction(obj):
        return {"source": bq_protected_inspect_getsource(obj, True), "__class__": "FUNCTION"}
    if inspect.isclass(obj):
        return {"source": bq_protected_inspect_getsource(obj, True), "__class__": "CLASS"}
    if isinstance(obj, set):
        return {"json": json.dumps(sorted(obj)), "__class__": "set"}
    if isinstance_with_name(obj, np.ndarray):
        return {"md5": hashlib.md5(obj.tobytes()).hexdigest(), "__class__": "np.ndarray"}
    if isinstance_with_name(obj, Outputs):
        data = obj.__dict__.copy()
        del data["version"]
        return {"__dict__": data, "__class__": "Outputs", "version": obj.version}
    if bq_protected_is_cython_function(obj):
        return {"source": bq_protected_sig_for_cythonized(obj), "__class__": "cython_function_or_method"}
    if isinstance(obj, BaseEstimator):
        return {"md5": hashing.hash(obj), "__class__": "sklearn.base.BaseEstimator"}
    if "keras" in str(obj):  # keras import耗时太长，只有确认是keras相关的类后，才import
        from tensorflow.keras import callbacks, optimizers

        if isinstance(obj, optimizers.Optimizer):
            config = sorted(obj.get_config().items())
            return {"config": json.dumps(config), "__class__": obj.__class__}
        if isinstance(obj, callbacks.EarlyStopping):
            config = obj.__dict__.copy()
            config["monitor_op"] = str(config["monitor_op"])
            return {"config": json.dumps(sorted(config.items())), "__class__": obj.__class__}
    # TODO: refactor the code below
    from sdk.datasource import Transformer

    if isinstance_with_name(obj, Transformer):
        return {"repr": repr(obj), "__class__": "Transformer"}

    from zipline.utils.calendars.trading_calendar import TradingCalendar

    if isinstance_with_name(obj, TradingCalendar):
        return {"json_md5": obj.to_json(), "__class__": "TradingCalendar"}

    from functools import partial

    if isinstance(obj, partial):
        return {
            "__class__": "functools.partial",
            "func": obj.func,
            "args": obj.args,
            "keywords": obj.keywords,
        }

    raise TypeError("_cache_key_encoder: not supported type: %s" % type(obj))


def _cache_value_encoder(obj):
    if isinstance(obj, dict):
        return obj
    if isinstance_with_name(obj, DataSource):
        return {"id": obj.id, "__class__": "DataSource"}
    if isinstance_with_name(obj, pd.DataFrame):
        return {"json_md5": obj.to_json(), "__class__": "DataFrame"}
    # if isinstance_with_name(obj, pd.Panel):
    #     return {'json_md5': obj.to_frame(False).to_json(), '__class__': 'Panel'}
    if isinstance_with_name(obj, np.int64):
        return int(obj)
    if isinstance_with_name(obj, Outputs):
        data = obj.__dict__.copy()
        del data["version"]
        return {"__dict__": data, "__class__": "Outputs", "version": obj.version}
    if inspect.ismethod(obj) or inspect.isfunction(obj):
        return None
    raise TypeError("_cache_value_encoder: not supported type: %s" % type(obj))


def _cache_value_decoder(obj):
    if isinstance(obj, dict):
        cls = obj.get("__class__", None)
        if cls is None:
            pass
        elif cls == "DataSource":
            obj = DataSource(id=obj["id"])
        elif cls == "DataFrame":
            obj = pd.read_json(obj["json_md5"])
        # elif cls == 'Panel':
        #     obj = pd.read_json(obj['json_md5']).to_panel()
        elif cls == "Outputs":
            obj = Outputs(version=obj["version"], **obj["__dict__"])
        else:
            raise TypeError("_cache_value_decoder: not supported type: %s" % cls)
    return obj


def cache_key(name: str, version: str, kwargs: dict):
    if kwargs is None or settings.cache_disabled:
        return None
    kwargs_json = json.dumps(kwargs, sort_keys=True, default=_cache_key_encoder)
    # print(kwargs_json)
    kwargs_md5 = hashlib.md5(kwargs_json.encode("utf-8")).hexdigest()
    key = "%s.%s.%s" % (name, version, kwargs_md5)
    return key


def cache_set(key: str, outputs: Any) -> None:
    if settings.cache_disabled:
        return False
    if not isinstance_with_name(outputs, Outputs) and type(outputs) != Outputs:
        raise TypeError("cache_set, not supported type: %s" % (type(outputs)))
    # check if cache is disabled by outputs
    if not outputs.__dict__.get("__cacheable__", True):
        return False

    outputs_json = json.dumps(outputs, sort_keys=True, default=_cache_value_encoder)
    if len(outputs_json) > 1024 * 1024:
        log.warn("返回的Outputs实例(size=%s) 过大，将不会被缓存。" % (len(outputs_json)))
        return False

    try:
        ModuleClient.get_module_cache(
            params=SetModuleCacheRequestSchema(key=key, outputs_json=outputs_json, is_papertrading=settings.is_paper_trading),
            credential=Credential.from_env(),
        )
    except Exception as e:
        log.warn(e)


def cache_get(key: str) -> Any:
    if settings.cache_disabled:
        return None

    outputs_json = ModuleClient.get_module_cache(
        params=GetModuleCacheRequestSchema(key=key, is_papertrading=settings.is_paper_trading),
        credential=Credential.from_env(),
    ).outputs_json

    if not outputs_json:
        return None

    outputs = json.loads(outputs_json, object_hook=_cache_value_decoder)

    return outputs


if __name__ == "__main__":
    # test code
    pass
