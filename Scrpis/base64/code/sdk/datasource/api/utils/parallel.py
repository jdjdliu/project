
import os
import datetime
import math
import multiprocessing
from multiprocessing.pool import ThreadPool
from contextlib import closing


def __map_func(args):
    func, i, item = args
    if isinstance(item, dict):
        return (i, func(**item))
    else:
        return (i, func(item))


def parallel_map(func, items, show_progress=False, processes_count=None,
                 chunk_size=None, use_threads=False, return_result=True):
    """
    并行计算（多进程版本）

    Args:
        func: 函数
        items: 数据列表
        show_progress: 是否显示进度
        processes_count: 进程数
        chunk_size: int chunk size
        use_threads: 是否使用线程

    Returns: List
    """

    processes_count = processes_count or multiprocessing.cpu_count() // 2
    if processes_count == 1:
        results = []
        for i, item in enumerate(items):
            if show_progress and (i + 1) % 10 == 0:
                print('%s, %s/%s ...' % (datetime.datetime.now(), i + 1, len(items)))
            results.append(func(item))
        if return_result:
            return results
    if not chunk_size:
        chunk_size = max(math.ceil(len(items) / processes_count / 10), 20)  # 进一位
    chunk_size = int(min(chunk_size, 40))
    assert isinstance(chunk_size, int)

    try:
        cpu_limit = int(os.getenv("CPU_LIMIT", 0))
    except:  # noqa
        cpu_limit = 0
    if cpu_limit and os.getenv("JPY_USER") and processes_count != cpu_limit:
        processes_count = cpu_limit

    args_list = [(func, i, item) for i, item in enumerate(items)]
    if use_threads:
        # TODO: ThreadPool crashes
        pool = ThreadPool(processes=processes_count)
        iter = pool.imap_unordered(__map_func, args_list, chunksize=chunk_size)
    else:
        with closing(multiprocessing.Pool(processes=processes_count)) as pool:
            iter = pool.imap_unordered(__map_func, args_list, chunksize=chunk_size)
    if show_progress:
        from tqdm import tqdm
        iter = tqdm(iter, total=len(args_list))
    if return_result:
        results = [None] * len(args_list)
        for result in iter:
            results[result[0]] = result[1]
        return results
