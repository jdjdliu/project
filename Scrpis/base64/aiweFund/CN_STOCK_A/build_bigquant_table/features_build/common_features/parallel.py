import datetime
import multiprocessing
from contextlib import closing
from multiprocessing.pool import ThreadPool


def __map_func(args):
    func, i, item = args
    return (i, func(item))


def map(func, items, show_progress=True, processes_count=8, chunksize=None, use_threads=False):
    '''
    并行计算（多进程版本）
    :param func: 函数
    :param items: 数据列表
    :param show_progress: 是否显示进度
    :param processes_count: 进程数
    :param chunksize: chunk size
    :return:
    '''
    processes_count = processes_count or multiprocessing.cpu_count()
    if processes_count == 1:
        results = []
        for i, item in enumerate(items):
            if show_progress:
                print('%s, %s/%s, %s ..' % (datetime.datetime.now(), i + 1, len(items), item))
            results.append(func(item))
        return results
    chunksize = int(chunksize or min(max(1, len(items) / multiprocessing.cpu_count() / 50), 20))

    if use_threads:
        # TODO: ThreadPool crashes
        pool = ThreadPool(processes=processes_count or multiprocessing.cpu_count())
        items = [(func, i, item) for i, item in enumerate(items)]
        iter = pool.imap_unordered(__map_func, items, chunksize=chunksize)
    else:
        with closing(multiprocessing.Pool(processes=processes_count or multiprocessing.cpu_count())) as pool:
            items = [(func, i, item) for i, item in enumerate(items)]
            iter = pool.imap_unordered(__map_func, items, chunksize=chunksize)

    if show_progress:
        from tqdm import tqdm
        iter = tqdm(iter, total=len(items))

    results = [None] * len(items)
    for result in iter:
        results[result[0]] = result[1]
    return results


if __name__ == '__main__':
    # test code
    def worker(i):
        import time
        time.sleep(0.1)
        # print(i)
        return i * i

    map(worker, range(0, 3), processes_count=2, use_threads=True)
