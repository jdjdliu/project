from sdk.utils import BigLogger  # noqa: F401

from joblib import Parallel, delayed

log = BigLogger("AI")


class ParallelEx(Parallel):
    def _print(self, msg, msg_args):
        """Display the message on log.info"""
        if not self.verbose:
            return
        msg = msg % msg_args
        log.info(f"[{self}]: {msg}")


def parallel_map(
    func,
    iterable,
    timeout=None,
    chunksize=1,
    max_workers=2,
    remote_run=False,
    use_yield=False,
    silent=False,
    backend="loky",
    verbose=60,
    raise_exception=False,
):
    from math import ceil

    import numpy as np

    from learning.module2.common.utils import check_user_max_workers
    from learning.toolimpl.remoterunfunc import _parallel_map_thread_run

    check_user_max_workers_result = check_user_max_workers(workers=max_workers, log=log, remote_run=remote_run)
    workers = check_user_max_workers_result.get("workers")
    remote_run = check_user_max_workers_result.get("remote_run")

    log.info(f"开始并行运算, remote_run={remote_run}, workers={workers} ..")
    if not remote_run:
        # 不分布式运行, 直接执行func
        results = ParallelEx(n_jobs=workers, timeout=timeout, batch_size=chunksize, backend=backend, verbose=verbose)(
            delayed(func)(**x) for x in iterable
        )
    else:
        chunked_iterable = (
            [x.tolist() for x in np.array_split(iterable, max(workers, ceil(len(iterable) / chunksize)))] if chunksize > 1 else iterable
        )
        results = ParallelEx(n_jobs=workers, timeout=timeout, backend=backend, verbose=verbose)(
            delayed(_parallel_map_thread_run)(func=func, kwargs=x, chunksize=chunksize, silent=silent, raise_exception=raise_exception)
            for x in chunked_iterable
        )
        if chunksize > 1:
            results = sum(results, [])
    if not use_yield:
        results = list(results)
    return results


def _picklable_func(func_dumps, *args, **kwargs):
    """
    picklable辅助函数
    """
    from sdk.utils import pickle

    func = pickle.loads(func_dumps)

    return func(*args, **kwargs)


def picklable(func):
    """
    使用pickle让函数可以序列化，可用于解决多进程调用函数内函数无法pickle问题
    """
    from functools import partial

    from sdk.utils import pickle

    return partial(_picklable_func, pickle.dumps(func))
