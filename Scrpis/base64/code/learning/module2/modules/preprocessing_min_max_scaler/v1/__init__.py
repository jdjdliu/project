import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from learning.api import M
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

# from learning.module2.common.mlutils import transform

# log = logbook.Logger('preprocessing_min_max_scaler')
log = BigLogger("preprocessing_min_max_scaler")
bigquant_cacheable = True
# 模块接口定义
bigquant_category = r"机器学习\特征预处理"
bigquant_friendly_name = "特征缩放"
bigquant_doc_url = ""


def bigquant_run(
    training_ds: I.port("训练数据，如果传入，则需要特征指定输入", specific_type_name="DataSource", optional=True) = None,
    features: I.port("特征，用于训练", specific_type_name="列表|DataSource", optional=True) = None,
    model: I.port("模型，用于预测，如果不指定训练数据，则使用此模型预测", specific_type_name="DataSource", optional=True) = None,
    predict_ds: I.port("预测数据，如果不设置，则不做预测", specific_type_name="DataSource", optional=True) = None,
    feature_range_low: I.int("特征缩放下限，将特征缩放到执行范围之间") = 0,
    feature_range_high: I.int("特征缩放上限，将特征缩放到执行范围之间") = 1,
) -> [I.port("训练出来的模型", "output_model"), I.port("转换后训练集", "transform_trainds"), I.port("转换后预测集", "transform_predictds")]:
    """
    将特征值缩放到一定范围内。

    """
    if training_ds is None and model is None:
        raise Exception("训练数据和输入模型不能都为空。请输入训练数据training_ds和特征features或者已经训练好的模型model")

    if training_ds:
        if not (training_ds and features):
            log.error("training_ds不为空，准备训练，请输入参数features")
            raise Exception("请输入参数features")
        if model:
            log.warn("training_ds和model都不为空，准备重新训练")

        features = smart_list(features)
        feature_range = (feature_range_low, feature_range_high)
        train_outputs = M.cached.v2(
            run=_transform, kwargs=dict(training_ds=training_ds, train_parameters=dict(feature_range=feature_range), features=features), m_silent=True
        )
        model = train_outputs.model
        transform_trainds = train_outputs.transform_ds
    else:
        transform_trainds = None

    if predict_ds:
        transform_predictds = M.cached.v2(run=_transform, kwargs=dict(training_ds=predict_ds, model=model), m_silent=True).transform_ds
    else:
        transform_predictds = None

    outputs = Outputs(output_model=model, transform_trainds=transform_trainds, transform_predictds=transform_predictds)
    return outputs


def _transform(training_ds, train_parameters={}, features=None, model=None):
    df = training_ds.read_df()
    if not model:
        # log.warning("model为空，重新运行模型训练")
        model = MinMaxScaler(**train_parameters)
        model.fit(df[features])
    else:
        model_info = model.read_pickle()
        model = model_info["model"]
        features = model_info["features"]
    df[features] = model.transform(df[features])
    model_info = {
        "model": model,
        "features": features,
        "feature_range": model.feature_range,
    }
    model = DataSource.write_pickle(model_info)
    return Outputs(model=model, transform_ds=DataSource.write_df(df))


if __name__ == "__main__":
    # test code
    # 本代码由可视化策略环境自动生成 2018年10月30日 17:07
    # 本代码单元只能在可视化模式下编辑。您也可以拷贝代码，粘贴到新建的代码单元或者策略，然后修改。
    import datetime

    def m1_run_bigquant_run(input_1, input_2, input_3):
        now = pd.to_datetime("2018-01-01")
        data = []
        for i in range(0, 100):
            for j in range(0, 50):
                data.append(((now + datetime.timedelta(days=1)).strftime("%Y-%m-%d"), "%s.SZA" % j, i / (j + 1), i - j, (i + j) % 2))
        df = pd.DataFrame(data, columns=["date", "instrument", "f1", "f2", "label"])

        data_1 = DataSource.write_df(df)

        return Outputs(data_1=data_1)

    # 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
    def m1_post_run_bigquant_run(outputs):
        return outputs

    m1 = M.cached.v3(run=m1_run_bigquant_run, post_run=m1_post_run_bigquant_run, input_ports="", params="{}", output_ports="", m_cached=False)

    m3 = M.input_features.v1(
        features="""f1
        f2""",
        m_cached=False,
    )

    m2 = M.preprocessing_min_max_scaler.v1(
        training_ds=m1.data_1, features=m3.data, predict_ds=m1.data_1, feature_range_low=0, feature_range_high=1, m_cached=False
    )

    m3 = M.preprocessing_min_max_scaler.v1(predict_ds=m1.data_1, model=m2.output_model, m_cached=False)

    print(1)
