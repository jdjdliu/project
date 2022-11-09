import os
import time
import random
import subprocess
import numpy as np

from collections import namedtuple
from sdk.utils import BigLogger

# log = logbook.Logger('device_manager')
log = BigLogger("device_manager")

gpu_info = namedtuple("gpu_info", ["index", "free_memory", "total_memory", "gpu_util"])
gpu_condition = namedtuple("gpu_info", ["gpu_list", "max_memory_limit", "min_memory_limit"])


def run_command(command_args, raise_error=True, returns_output=False, stdout=True, shell=False):
    command_args = [str(a) if a is not None else "" for a in command_args]

    def call():
        if not returns_output:
            if stdout:
                return subprocess.check_call(command_args, shell=shell)
            else:
                with open(os.devnull, "w") as fp:
                    return subprocess.check_call(command_args, stdout=fp, stderr=subprocess.STDOUT, shell=shell)
        else:
            return subprocess.check_output(command_args)

    if raise_error:
        output = call()
    else:
        try:
            output = call()
        except BaseException:
            output = None
    return output


def get_gpu_info():
    # content = os.popen(
    #     'nvidia-smi --query-gpu=index,memory.free,memory.total,utilization.gpu --format=csv,noheader').read()
    try:
        content = (
            run_command(
                ["bash", "-c", "nvidia-smi --query-gpu=index,memory.free,memory.total,utilization.gpu --format=csv,noheader"], returns_output=True
            )
            .decode()
            .strip()
        )
        gpu_list = []
        for line in content.strip().splitlines():
            items = line.strip().split(",")
            index = int(items[0])
            free_memory = float(items[1][:-4].strip())
            total_memory = float(items[2][:-4].strip())
            gpu_util = float(items[3][:-2].strip())
            gpu_list.append(gpu_info(index, free_memory, total_memory, gpu_util))
        return gpu_list
    except Exception:
        return []


def get_device_condition(user_name):
    # 返回可以使用的gpu id列表，和最大显存使用M, 最小需要显存
    condition = gpu_condition(None, None, 1024)
    return condition


def to_multi_gpu_model(model, n_gpus):
    import tensorflow.compat.v1.keras.backend as K
    from tensorflow.keras.layers import Input, Lambda, concatenate
    from tensorflow.keras.models import Model
    import tensorflow as tf

    def slice_batch(x, n_gpus, part):
        sh = K.shape(x)
        L = sh[0] / n_gpus
        begin = K.cast(part * L, dtype="int32")
        end = K.cast((part + 1) * L, dtype="int32")
        if part == n_gpus - 1:
            return x[begin:]
        return x[begin:end]

    # with tf.device('/cpu:0'):
    x = Input(model.input_shape[1:], name=model.input_names[0])
    towers = []
    for g in range(n_gpus):
        with tf.device("/gpu:" + str(g)):
            slice_g = Lambda(slice_batch, lambda shape: shape, arguments={"n_gpus": n_gpus, "part": g})(x)
            towers.append(model(slice_g))
    # with tf.device('/cpu:0'):
    merged = concatenate(towers, axis=0)
    return Model(inputs=[x], outputs=merged)


def _apply_for_device(n_gpus):
    gpu_info_list = get_gpu_info()
    user_name = os.environ.get("JPY_USER", None)
    condition = get_device_condition(user_name)

    if condition.gpu_list:
        constraint_gpu_list = set(int(x) for x in condition.gpu_list)
        gpu_info_list = [x for x in gpu_info_list if x.index in constraint_gpu_list]

    max_memory_limit = condition.max_memory_limit
    min_memory_limit = condition.min_memory_limit
    gpu_info_list.sort(key=lambda x: x.free_memory, reverse=True)
    if len(gpu_info_list) == 0 or n_gpus == 0:
        if len(gpu_info_list) == 0:
            log.info("没有gpu资源，将使用cpu计算")
        session_init()
        return []

    if min_memory_limit is not None:
        # 获取满足条件的gpu资源
        gpu_info_list = [x for x in gpu_info_list if x.free_memory > min_memory_limit]
    gpu_info_list = gpu_info_list[:n_gpus]
    if len(gpu_info_list) > 0:
        if len(gpu_info_list) < n_gpus:
            log.info("没有足够gpu资源，将使用%s个gpu进行计算" % len(gpu_info_list))
        if max_memory_limit:
            memory_fraction = max_memory_limit / gpu_info_list.total_memory
        else:
            memory_fraction = 1
        use_gpu_index = sorted([x.index for x in gpu_info_list])
    else:
        use_gpu_index = []
        memory_fraction = 1
        return None
    # if len(use_gpu_index) > 1:
    #     model = to_multi_gpu_model(model, len(use_gpu_index))
    session_init(gpu_list=use_gpu_index, memory_fraction=memory_fraction)
    return use_gpu_index


def apply_for_device(n_gpus):
    while True:
        use_gpu_index = _apply_for_device(n_gpus)
        if use_gpu_index is not None:
            return use_gpu_index
        log.info("目前没有符合条件的gpu资源，请等待！")
        time.sleep(30)


def set_global_seed(i=0):
    import tensorflow as tf

    tf.random.set_seed(i)
    np.random.seed(i)
    random.seed(i)


def session_init(gpu_list=None, allow_growth=True, memory_fraction=1):
    import tensorflow as tf
    import tensorflow.compat.v1.keras.backend as KTF

    config = tf.compat.v1.ConfigProto()
    if gpu_list:
        config.gpu_options.visible_device_list = ",".join([str(x) for x in gpu_list])
        # config.gpu_options.per_process_gpu_memory_fraction = memory_fraction
        config.gpu_options.allow_growth = allow_growth
        log.info("本次操作使用GPU设备：%s" % (",".join(["%s" % x for x in gpu_list])))
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = ""
        log.info("本次操作不使用GPU")
    config.allow_soft_placement = True
    sess = tf.compat.v1.Session(config=config)
    KTF.set_session(sess)
    set_global_seed(0)
    # sess.run(tf.global_variables_initializer())
    return sess


def simple_session_init(n_gpus):
    # 通过k8s来管理GPU，不需要在这里做过多的gpu资源的判断了,默认已经满足需求
    # import tensorflow as tf
    # import tensorflow.compat.v1.keras.backend as KTF
    # sess = tf.compat.v1.Session()
    # KTF.set_session(sess)
    # set_global_seed(0)
    # if n_gpus >= 2:
    #     model = to_multi_gpu_model(model, n_gpus)
    # return model

    # 还是自己来管理gpu资源，使用k8s进行管理会有些问题  2020-01-18
    if n_gpus <= 1:
        import tensorflow as tf

        tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
        sess = tf.compat.v1.Session()
        tf.compat.v1.keras.backend.set_session(sess)
        set_global_seed(0)
        return []
    else:
        return apply_for_device(n_gpus)
