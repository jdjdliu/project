import asyncio
import time

# 我们通过async关键字定义一个协程,当然协程不能直接运行，需要将协程加入到事件循环loop中
async def do_some_work(x):
    print("waiting:", x)
    return "Done after {}s".format(x)

def callback(future):
    print("callback:",future.result())

start = time.time()

coroutine = do_some_work(2)
loop = asyncio.get_event_loop()        # asyncio.get_event_loop：创建一个事件循环
# 通过loop.create_task(coroutine)创建task,同样的可以通过 asyncio.ensure_future(coroutine)创建task
task = loop.create_task(coroutine)     # 创建任务, 不立即执行
# task = asyncio.ensure_future(coroutine)
task.add_done_callback(callback)
# 绑定回调，在task执行完成的时候可以获取执行的结果
loop.run_until_complete(task)         # 使用run_until_complete将协程注册到事件循环，并启动事件循环
print("Time:",time.time() - start)

''' 运行结果
waiting: 2
callback: Done after 2s
Time: 0.0010030269622802734
'''