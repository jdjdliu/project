import inspect
import logging
import os
import traceback
from sdk.utils import BigLogger

import logbook
from sdk.auth import current_user

from learning import settings
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.modulecache import cache_get, cache_key, cache_set
from learning.shared.utils import Stopwatch

# <ENTERPRISE_CODE>


def file_handler_log_formatter(record, handler):
    log = "[Module][{dt}][{level}][{username}] {msg}".format(
        dt=record.time,  # 当前时间
        level=record.level_name,  # 日志等级
        username=current_user(),  # 用户名
        msg=record.message,  # 日志内容
    )
    return log


try:
    # TODO log to kafka to es
    log_dir = "/tmp"
    # if not os.path.exists(log_dir):
    #     os.makedirs(log_dir)
    #     os.chmod(log_dir, mode=0o777)
    file_handler = logbook.TimedRotatingFileHandler(os.path.join(log_dir, "ipython.log"), date_format="%Y%m%d", bubble=False)

    file_handler.formatter = file_handler_log_formatter
except Exception as e:
    print("moduleinvoker: failed to set log files:", e)

log_file = logbook.Logger("moduleinvoker")
log = BigLogger("moduleinvoker")

PACKAGE_PATTERN = "learning.module2.modules.%s.%s"

# 自定义模块所在包路径
CUSTOM_MODULES_PACKAGE_PATTERN = "custom_modules.%s.%s"

META_ARGUMENT_PREFIX = "m_"
ARG_M_CACHED = "m_cached"
ARG_M_DEPENDENCIES = "m_deps"
ARG_M_SILENT = "m_silent"
ARG_M_LAZY_RUN = "m_lazy_run"
ARG_M_REMOTE = "m_remote"
ARG_M_RAISE_EXCEPTION = "m_raise_exception"
ARG_M_PARALLEL = "m_parallel"
ARG_M_ORIGIN_FUNC = "m_origin_func"
__known_meta_arguments = set(
    [ARG_M_RAISE_EXCEPTION, ARG_M_CACHED, ARG_M_DEPENDENCIES, ARG_M_PARALLEL, ARG_M_SILENT, ARG_M_LAZY_RUN, ARG_M_REMOTE, ARG_M_ORIGIN_FUNC]
)

VAR_BIGQUANT_CACHEABLE = "bigquant_cacheable"
_module_builtins = [
    "BigQuantModule",
    "bigquant_run",
    "bigquant_postrun",
    VAR_BIGQUANT_CACHEABLE,
    "bigquant_cache_key",
    "bigquant_public",
    "bigquant_deprecated",
    "bigquant_opensource",
    # 是否远程打包运行
    "bigquant_remoterun",
]


class ModuleInvoker:
    def __init__(self, pkg):
        self.pkg = pkg
        self.cls = None
        self.entry_func = None
        self.postrun = None
        self.cacheable = None
        self.cache_key = None
        self.is_public = None
        self.deprecated = None
        self.category = None
        self.rank = None
        self.friendly_name = None
        self.doc_url = None
        self.desc = None
        # 是否远程打包运算
        self.remoterun = None

    @staticmethod
    def load(name, version, kwargs={}, custom_module=False):
        package_name = PACKAGE_PATTERN % (name, version) if not custom_module else CUSTOM_MODULES_PACKAGE_PATTERN % (name, version)
        try:
            pkg = __import__(package_name, globals(), locals(), _module_builtins)
        except ImportError:
            pkg = __import__(CUSTOM_MODULES_PACKAGE_PATTERN % (name, version), globals(), locals(), _module_builtins)

        m = ModuleInvoker(pkg)
        vars = pkg.__dict__
        if vars.get("log"):
            vars.get("log").set_level(int(os.getenv("LOGGING_LEVEL", logging.INFO)) if not kwargs.get(ARG_M_SILENT, False) else logging.WARNING)
        m.cacheable = vars.get(VAR_BIGQUANT_CACHEABLE, True)
        m.postrun = vars.get("bigquant_postrun", None)
        m.cache_key = vars.get("bigquant_cache_key", None)
        m.is_public = vars.get("bigquant_public", True)
        m.deprecated = vars.get("bigquant_deprecated", None)
        # 是否远程打包运算
        m.remoterun = vars.get("bigquant_remoterun", None)

        if m.deprecated:
            m.deprecated = m.deprecated.replace("${MODULE_NAME}", name)
        m.opensource = vars.get("bigquant_opensource", False)
        m.author = vars.get("bigquant_author", None)
        m.category = vars.get("bigquant_category", 1000000)
        m.rank = vars.get("bigquant_rank", None)
        m.friendly_name = vars.get("bigquant_friendly_name", None)
        m.doc_url = vars.get("bigquant_doc_url", None)

        if "bigquant_run" in pkg.__dict__:
            m.entry_func = pkg.bigquant_run
            m.desc = vars["bigquant_run"].__doc__
        elif "BigQuantModule" in pkg.__dict__:
            m.cls = pkg.BigQuantModule
            m.entry_func = pkg.BigQuantModule.__init__
        else:
            raise Exception(
                "invalid module name: {}, version: {}, friendly_name: {}, either bigquant_run or BigQuantModule is required".format(
                    name, version, m.friendly_name
                )
            )

        return m


def _detect_cache_enabled(pkg, kwargs):
    if VAR_BIGQUANT_CACHEABLE not in pkg.__dict__:
        raise Exception("%s is required for module %s" % (VAR_BIGQUANT_CACHEABLE, pkg))
    if not pkg.__dict__[VAR_BIGQUANT_CACHEABLE]:
        return False

    if ARG_M_CACHED in kwargs:
        if not kwargs[ARG_M_CACHED]:
            return False
        del kwargs[ARG_M_CACHED]

    return True


def _module_run(module, kwargs):
    func = module.cls or module.entry_func

    func_kwargs = {k: v for k, v in kwargs.items() if not k.startswith(META_ARGUMENT_PREFIX)}
    if "m_meta_kwargs" in inspect.signature(func).parameters:
        meta_kwargs = {k: v for k, v in kwargs.items() if k.startswith(META_ARGUMENT_PREFIX)}
        func_kwargs["m_meta_kwargs"] = meta_kwargs
    outputs = func(**func_kwargs)
    if module.cls:
        outputs = outputs.run()

    return outputs


def _module_postrun(module, outputs):
    if module.postrun is None:
        return outputs

    return module.postrun(outputs)


def _invoke_with_cache(module, kwargs, module_cache_key, name=None, version=None, remote_run=None):
    remote_run = kwargs.get(ARG_M_REMOTE, False) if not remote_run else remote_run
    silent = kwargs.get(ARG_M_SILENT, False)
    parallel = kwargs.get(ARG_M_PARALLEL, False)
    if remote_run and not silent and not parallel:
        log.info("%s.%s 提交运行.." % (name, version))
    if module_cache_key:
        # # for test 2018-09-29
        # from learning.settings import is_paper_trading
        # if is_paper_trading and ((name == 'stock_ranker_train' and version in ['v5']) or
        #                          (name == 'stock_ranker_predict' and version in ['v5'])):
        #     pass
        # # # for test.
        # else:
        outputs = cache_get(module_cache_key)
        if outputs is not None:
            if not silent:
                log.info("命中缓存")
            elif remote_run:
                log.info("打包运行命中缓存")
            return outputs

    try:
        # 模块需要远程运行或者传入参数指定远程运行
        if remote_run:
            outputs = remote_module_invoke(name, version, kwargs)
            # outputs = _module_postrun(module, outputs)
            # return outputs
        else:
            outputs = _module_run(module, kwargs)
    except Exception:
        with file_handler:
            log.error("module name: {}, module version: {}, trackeback: {}".format(name, version, traceback.format_exc(limit=0)))
            log_file.error("module name: {}, module version: {}, trackeback: {}".format(name, version, traceback.format_exc()))
        raise
    if module_cache_key:
        # for test 2018-09-29 do not cache
        # from learning.settings import is_paper_trading
        # if is_paper_trading and ((name == 'stock_ranker_train' and version in ['v5']) or
        #                          (name == 'stock_ranker_predict' and version in ['v5'])):
        #     pass
        # # # for test.
        # else:
        cache_set(module_cache_key, outputs)

    return outputs


def module_invoke(name, version, custom_module, kwargs):
    # 模拟实盘，如若跑完了首个trade，后面的策略不跑了
    if os.environ.get("FIRST_TRADE_SUCCESS", "") == "True":
        return Outputs(
            algo_result=None,
            new_orders=None,
            raw_perf=None,
            positions=None,
            ohlc_data=None,
            context_outputs=None,
            read_raw_perf=None,
            pyfolio_full_tear_sheet=None,
            risk_analyze=None,
            factor_profit_analyze=None,
            data=None,
            raw_perfs=None,
            model_id=None,
            instruments=None,
            start_date=None,
            end_date=None,
            model=None,
            predictions=None,
            data_1=None,
        )

    for k in kwargs.keys():
        if k.startswith(META_ARGUMENT_PREFIX) and k not in __known_meta_arguments:
            raise Exception("未知的meta参数: %s" % k)

    if kwargs.get(ARG_M_LAZY_RUN, False):
        del kwargs[ARG_M_LAZY_RUN]
        lazy_run_data = {
            "name": name,
            "version": version,
            "kwargs": kwargs,
        }
        lazy_run_ds = DataSource.write_pickle(lazy_run_data, use_cache=True)

        log.info("延迟运行 %s.%s" % (name, version))
        import learning.module2.common.interface as I  # noqa

        return Outputs(**{I.port_name_lazy_run: lazy_run_ds})

    # add param custom_module
    module = ModuleInvoker.load(name, version, kwargs, custom_module)
    if module.deprecated:
        log.warning("此模块版本 M.%s.%s 已不再维护。你仍然可以使用，但建议升级到最新版本：%s" % (name, version, module.deprecated))

    cache_enabled = module.cacheable and kwargs.get(ARG_M_CACHED, True)
    kwargs_for_cache = module.cache_key(kwargs.copy()) if module.cache_key else kwargs
    module_cache_key = cache_key("M." + name, version, kwargs_for_cache) if cache_enabled else None
    remote_run = kwargs.get(ARG_M_REMOTE, False)

    sw = Stopwatch()

    # TODO more design
    if module.remoterun and not os.getenv("JOB_TYPE") and kwargs.get("n_gpus", 0) > 0 and settings.enable_gpu_remoterun:
        remote_run = True
    if name in settings.always_remoterun_modules and not os.getenv("JOB_TYPE"):
        remote_run = True

    if module.cacheable and not kwargs.get(ARG_M_SILENT, False) and not remote_run:
        log.info("%s.%s 开始运行.." % (name, version))

    outputs = _invoke_with_cache(module, kwargs, module_cache_key, name=name, version=version, remote_run=remote_run)
    if not remote_run:
        if (name == "cached" and version not in ["v1", "v2"]) or name in settings.has_postrun_params_modules:
            module.postrun = kwargs.get("post_run", None)
        outputs = _module_postrun(module, outputs)
    if module.cacheable and not remote_run:
        pass
    if not kwargs.get(ARG_M_SILENT, False):
        log.info("%s.%s 运行完成[%ss]." % (name, version, sw.elapsed))

    return outputs


def remote_module_invoke(name, version, kwargs):
    import datetime  # noqa
    import math  # noqa

    import pandas
    from sdk.utils import pickle

    pd = pandas  # noqa
    import numpy

    np = numpy  # noqa
    import random  # noqa

    from sdk.datasource import D, DataReader  # noqa

    import learning.api.tools as T  # noqa
    from learning.api import M  # noqa
    from learning.module2.common.data import DataSource, Outputs  # noqa

    # excluded_globals = [datetime, math, pandas, pd, numpy, np, random, DataReader, D, M, T, DataSource, Outputs]
    user_name = current_user()
    inputs_obj = {
        "name": name,
        "version": version,
        "kwargs": kwargs,
    }

    input_data = pickle.dumps(inputs_obj)
    input_ds = DataSource()
    with open(input_ds.open_temp_path(), "wb") as writer:
        writer.write(input_data)
    input_ds.close_temp_path()
    script_content = "python3 /home/bigquant/work/remote_run_worker.py $1 %s" % input_ds.id

    gpu = 0
    node_selector = None
    # HACK here: TODO: remove this hacking, better design for this
    if name in settings.enabled_gpu_remoterun_modules and kwargs.get("n_gpus", 0) > 0:
        if user_name in settings.enabled_gpu_remoterun_users or settings.enable_gpu_remoterun:
            gpu = kwargs["n_gpus"]
            node_selector = settings.gpu_node_selector_by_user.get(user_name, settings.gpu_node_selector)
    elif name in settings.always_remoterun_modules:
        gpu = kwargs["n_gpus"]

    # TODO: remove this
    from bigjupyteruserservice.client import BigJupyterUserClient

    response = BigJupyterUserClient.instance().start_job(
        user_name,
        script_content,
        files_dict={},
        job_type=BigJupyterUserClient.JobType.RemoteRun,
        job_tag=name[:16] + version,
        node_selector=node_selector,
        gpu=gpu,
        kernel_id=os.getenv("KERNEL_ID", None),
        logger=log,
    )

    if response.err_code != 0:
        log.error("job_id: {}, error msg:{}".format(response.job_id, response.err_msg))
        return None

    if response.err_msg:
        log.warn(response.err_msg)

    job_logger = BigLogger(f"{name}.{version}.{response.job_id[:8]}")
    read_log = not kwargs.get(ARG_M_SILENT, False)
    raise_exception_if_not_succeeded = kwargs.get(ARG_M_RAISE_EXCEPTION, True)
    # TODO: remove this
    BigJupyterUserClient.instance().watch_job_until_done(
        response.job_id,
        interva_seconds=10,
        read_log=read_log,
        logger=job_logger,
        on_log=None,
        raise_exception_if_not_succeeded=raise_exception_if_not_succeeded,
    )

    outputs = {}
    try:
        # TODO: remove this
        outputs = BigJupyterUserClient.instance().get_job_run_outputs(response.job_id)
    except Exception:
        log.error("job_id: {}, 获取打包运算返回结果失败".format(response.job_id))
        return None
    if outputs.get("err_code", 0) != 0:
        # raise Exception(outputs['err_msg'])
        log.error("job_id: {}, outputs error: {}".format(response.job_id, outputs["err_msg"]))

    return outputs.get("data", None)


def handle_the_pickle_of_remoterun_outputs(outputs_data):
    from sdk.utils import pickle

    from learning.module2.common.utils import isinstance_with_name

    iter_object = {}
    if hasattr(outputs_data, "__dict__"):
        iter_object = outputs_data.__dict__
    elif isinstance(outputs_data, dict) or isinstance(outputs_data, list):
        iter_object = outputs_data
    for x in iter_object:
        pickle_data = iter_object[x] if isinstance(iter_object, dict) else x
        try:
            if isinstance_with_name(pickle_data, Outputs):
                handle_the_pickle_of_remoterun_outputs(pickle_data)
            elif isinstance(pickle_data, list) or isinstance(pickle_data, dict):
                handle_the_pickle_of_remoterun_outputs(pickle_data)
            else:
                pickle.dumps(pickle_data)
        except Exception as e:
            # TODO: why catch this expection instead of fixing it, 现在出问题的主要是tensorflow.keras的layers
            if isinstance(iter_object, dict):
                iter_object[x] = f"cannot pickle this object: {pickle_data}, error: {e}"
            else:
                iter_object.remove(x)


def invoke_from_file(inputs_path, job_id):
    # TODO: remove this
    from bigjupyteruserservice.client import BigJupyterUserClient
    from sdk.utils import pickle

    from learning.api import M

    outputs = {}
    try:
        with open(inputs_path, "rb") as f:
            inputs = pickle.loads(f.read())

        module_run = M.m_get_module(inputs["name"]).m_get_version(inputs["version"])
        module_kwargs = inputs["kwargs"]
        module_kwargs[ARG_M_REMOTE] = False
        module_kwargs[ARG_M_CACHED] = False
        if module_kwargs.get(ARG_M_PARALLEL, False):
            # 不输出cached.v3开始运行
            module_kwargs[ARG_M_SILENT] = True

        data = module_run(**module_kwargs)

        outputs = {
            "err_code": 0,
            "data": data,
        }
        # 对于无法pickle的类型
        # 1. 如果需要的数据，在pickle里支持
        # 2. 对于不需要的数据，不应该放在output里
        # TODO: 可能会有出错的case待fix

    except Exception as e:
        outputs = {
            "err_code": 1,
            "err_msg": str(e),
        }
        traceback.print_exc()

    try:
        BigJupyterUserClient.instance().set_job_run_outputs(job_id, outputs)
    except Exception:
        # 无法pickle的类型填入 "不支持pickle" 的字符串
        handle_the_pickle_of_remoterun_outputs(outputs.get("data", {}))
        BigJupyterUserClient.instance().set_job_run_outputs(job_id, outputs)
