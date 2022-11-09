from functools import partial


def _parallel_map_remote_run_func(func, kwargs, chunksize):
    from learning.module2.common.data import Outputs

    if chunksize > 1:
        results = [func(**x) for x in kwargs]
        return Outputs(data=results)
    else:
        return Outputs(data=func(**kwargs))


def _parallel_map_thread_run(func, kwargs, chunksize, silent, raise_exception=True):
    from learning.api import M

    result = M.cached.v2(
        run=partial(_parallel_map_remote_run_func, func=func, chunksize=chunksize),
        kwargs={"kwargs": kwargs},
        m_cached=False,
        m_remote=True,
        m_silent=silent,
        m_parallel=True,
        m_raise_exception=raise_exception,
    )
    return result.data if result else None
