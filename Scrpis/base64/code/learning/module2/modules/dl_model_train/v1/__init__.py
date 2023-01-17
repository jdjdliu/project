import os
import time
from datetime import datetime

from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I  # noqa
from sdk.utils.func import extend_class_methods
from learning.module2.common.utils import smart_list, smart_dict
import learning.module2.common.dlutils as DL
from learning.module2.common.dl_session_manager import simple_session_init, to_multi_gpu_model
from sdk.utils import BigLogger

# log = logbook.Logger('dl_model_train')
log = BigLogger("dl_model_train")

# 是否自动缓存结果
bigquant_cacheable = True

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = False
bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = r"深度学习\模型"
bigquant_friendly_name = "训练(深度学习)"
bigquant_doc_url = "https://bigquant.com/docs/"

# 是否打包运行
bigquant_remoterun = True

root_tensorboard_dir = "/var/app/log/biglab/tensorboardlog"

DEFAULT_CUSTOM_OBJECTS = """# 用户的自定义层需要写到字典中，比如
# {
#   "MyLayer": MyLayer
# }
bigquant_run = {
    
}
"""


def bigquant_run(
    input_model: I.port("模型结构"),  # noqa
    training_data: I.port("训练数据，pickle格式dict，包含x和y"),
    validation_data: I.port("验证数据，pickle格式dict，包含x和y", optional=True) = None,
    optimizer: I.choice(
        "优化器，optimizer，优化器使用的是使用默认参数，如果需要修改参数，可以使用自定义优化器",
        ["SGD", "RMSprop", "Adagrad", "Adadelta", "Adam", "Adamax", "Nadam", "TFOptimizer", DL.TEXT_USER],
    ) = "SGD",
    user_optimizer: I.code(
        "自定义优化器，示例：\nfrom tensorflow.keras import optimizers\nbigquant_run=optimizers.SGD(lr=0.01, clipvalue=0.5)", I.code_python
    ) = None,
    loss: I.choice(
        "目标函数，loss，目标函数/损失函数",
        [
            "mean_squared_error",
            "mean_absolute_error",
            "mean_absolute_percentage_error",
            "mean_squared_logarithmic_error",
            "squared_hinge",
            "hinge",
            "categorical_hinge",
            "binary_crossentropy",
            "logcosh",
            "categorical_crossentropy",
            "sparse_categorical_crossentrop",
            "kullback_leibler_divergence",
            "poisson",
            "cosine_proximity",
            DL.TEXT_USER,
        ],
    ) = "mean_squared_error",
    user_loss: I.code("自定义目标函数，示例：\nfrom tensorflow.keras import losses\nbigquant_run=losses.mean_squared_error", I.code_python) = None,
    metrics: I.str("评估指标，包含评估模型在训练和测试时的性能的指标，多个指标用英文逗号(,)分隔。示例：mse,accuracy") = None,
    batch_size: I.int("batch_size，进行梯度下降时每个batch包含的样本数。训练时一个batch的样本会被计算一次梯度下降，使目标函数优化一步。") = 32,
    epochs: I.int("epochs，训练终止时的epoch值，训练将在达到该epoch值时停止，当没有设置initial_epoch时，它就是训练的总轮数，否则训练的总轮数为epochs - inital_epoch") = 1,
    earlystop: I.code("提前终止训练，示例：\nbigquant_run=EarlyStopping(monitor='val_mse', min_delta=0.0001, patience=1)​)", I.code_python) = None,
    custom_objects: I.code("用户自定义层，字典形式，给出用户自定义层的键值对", I.code_python, default=DEFAULT_CUSTOM_OBJECTS, specific_type_name="字典") = {},
    n_gpus: I.int("gpu个数，本模块使用的gpu个数") = 0,
    verbose: I.choice("日志输出", ["0:不显示", "1:输出进度条记录", "2:每个epoch输出一行记录"]) = "2:每个epoch输出一行记录",
) -> [I.port("训练后的模型", "data"),]:
    """
    深度学习模型模型编译和训练。
    """
    import tensorflow.keras.backend as K

    def do_run(
        input_model, training_data, validation_data, optimizer, user_optimizer, loss, user_loss, metrics, batch_size, epochs, verbose, custom_objects
    ):
        optimizer = DL.smart_choice(optimizer, user_optimizer)
        from tensorflow.keras.callbacks import TensorBoard, EarlyStopping

        # {'class_name':'adam','config':{'lr':2e-4, 'epsilon':1e-5}} 支持这种自定义配置
        if isinstance(optimizer, dict):
            from tensorflow.keras import optimizers

            optimizer = optimizers.get(optimizer)
        loss = DL.smart_choice(loss, user_loss)
        earlystop_callback = earlystop
        if verbose and isinstance(verbose, str):
            verbose = int(verbose[0])

        use_gpu_index = simple_session_init(n_gpus)
        model_yaml = input_model.read_pickle()
        model = DL.model_from_yaml(model_yaml, custom_objects=custom_objects)
        # model = simple_session_init(model, n_gpus)

        if len(use_gpu_index) > 1:
            model = to_multi_gpu_model(model, len(use_gpu_index))
        model.compile(optimizer=optimizer, loss=loss, metrics=smart_list(metrics) or None)

        training_data = training_data.read_pickle()
        if validation_data:
            validation_data = validation_data.read_pickle()
            validation_data = (validation_data["x"], validation_data["y"])

        log.info("准备训练，训练样本个数：%s，迭代次数：%s" % (len(training_data["x"]), epochs))
        start_time = time.time()

        callbacks = []
        if os.path.exists(root_tensorboard_dir):
            summary_log_dir = "%s/%s" % (root_tensorboard_dir, datetime.now().strftime("%Y%m%d_%H%M%S"))
            tb = TensorBoard(log_dir=summary_log_dir)
            callbacks.append(tb)

        if earlystop_callback is not None:
            callbacks.append(earlystop_callback)

        history = model.fit(
            x=training_data["x"],
            y=training_data["y"],
            batch_size=batch_size,
            epochs=epochs,
            verbose=verbose,
            validation_data=validation_data,
            callbacks=callbacks if callbacks else None,
        )

        log.info("训练结束，耗时：%.2fs" % (time.time() - start_time))
        model_weights = model.get_weights()
        # clear session
        K.clear_session()

        ds = DataSource.write_pickle(
            {"model_graph": model_yaml, "model_weights": model_weights, "history": history.history, "custom_objects": custom_objects}
        )
        return ds

    # ds = fork_run(do_run, input_model, training_data, validation_data, optimizer,
    #              user_optimizer, loss, user_loss, metrics, batch_size, epochs, verbose)
    ds = do_run(
        input_model, training_data, validation_data, optimizer, user_optimizer, loss, user_loss, metrics, batch_size, epochs, verbose, custom_objects
    )

    return Outputs(data=ds)


def bigquant_postrun(outputs):
    from .plot import load_model, plot_result

    extend_class_methods(outputs, load_model=load_model)

    extend_class_methods(outputs, plot_result=plot_result)
    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
