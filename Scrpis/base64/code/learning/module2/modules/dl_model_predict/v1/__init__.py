from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I  # noqa
import learning.module2.common.dlutils as DL

# from learning.module2.common.utils import fork_run
from learning.module2.common.dl_session_manager import simple_session_init, to_multi_gpu_model


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
bigquant_friendly_name = "预测(深度学习)"
bigquant_doc_url = "https://bigquant.com/docs/"

# 是否打包运行
bigquant_remoterun = True


def bigquant_run(
    trained_model: I.port("模型"),  # noqa
    input_data: I.port("数据，pickle格式dict，包含x"),
    batch_size: I.int("batch_size，进行梯度下降时每个batch包含的样本数。训练时一个batch的样本会被计算一次梯度下降，使目标函数优化一步。") = 32,
    n_gpus: I.int("gpu个数，本模块使用的gpu个数") = 0,
    verbose: I.choice("日志输出", ["0:不显示", "1:输出进度条记录", "2:每个epoch输出一行记录"]) = "2:每个epoch输出一行记录",
) -> [I.port("预测结果", "data"),]:
    """
    深度学习模型预测。
    """
    import tensorflow.keras.backend as K

    def do_run(trained_model, input_data, batch_size, verbose):
        if verbose and isinstance(verbose, str):
            verbose = int(verbose[0])

        use_gpu_index = simple_session_init(n_gpus)
        model_dict = trained_model.read_pickle()
        model = DL.model_from_yaml(model_dict["model_graph"], model_dict.get("custom_objects", None))
        # model = simple_session_init(model, n_gpus)
        if len(use_gpu_index) > 1:
            model = to_multi_gpu_model(model, len(use_gpu_index))
        model.set_weights(model_dict["model_weights"])

        input_data = input_data.read_pickle()

        result = model.predict(x=input_data["x"], batch_size=batch_size, verbose=verbose)
        # clear session
        K.clear_session()
        ds = DataSource.write_pickle(result)
        return ds

    # ds = fork_run(do_run, trained_model, input_data, batch_size, verbose)
    ds = do_run(trained_model, input_data, batch_size, verbose)
    return Outputs(data=ds)


def bigquant_postrun(outputs):
    print(outputs.data)

    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
