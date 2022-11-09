import pandas as pd
import pickle

from learning.module2.common.data import DataSource, Outputs


def prepare_data(df, features):
    try:
        if "label" in df.columns:
            return df[features].values, df["label"].values
        else:
            return df[features].values, None
    except KeyError:
        raise KeyError("部分特征没有在数据中，执行失败")


def str_split(s, sep=",", strip=True, remove_empty=True):
    s = s.split(sep)
    if strip:
        s = [item.strip() for item in s]
    if remove_empty:
        s = [item for item in s if item]
    return s


def score(outputs, data):
    model_info = pickle.loads(outputs.output_model.open_file(binary=True).read())
    df = data.read_df()
    xx, yy = prepare_data(df, model_info["features"])
    if yy is None:
        raise ValueError("输入数据中没有label, 无法调用score")
    score = model_info["model"].score(xx, yy)
    outputs.output_model.close_file()
    return score


def feature_gains(outputs):
    model_info = outputs.output_model.read_pickle()
    return model_info["feature_gains"]


def train(training_ds, features, train_algorithm, train_parameters={}):
    df = training_ds.read_df()
    train_data, train_label = prepare_data(df, features)

    model = train_algorithm(**train_parameters)
    model.fit(train_data, train_label)

    model_info = {"model": model, "features": features}
    model = DataSource.write_pickle(model_info)
    return Outputs(model=model)


def feature_train(training_ds, features, train_algorithm, train_parameters={}):
    df = training_ds.read_df()
    train_data, train_label = prepare_data(df, features)

    model = train_algorithm(**train_parameters)
    model.fit(train_data, train_label)

    feature_gains = pd.DataFrame(data={"feature": features, "gain": model.feature_importances_})
    feature_gains.sort_values("gain", inplace=True, ascending=False)

    model_info = {"model": model, "features": features, "feature_gains": feature_gains}
    model = DataSource.write_pickle(model_info)
    return Outputs(model=model)


def cluster_train(training_ds, features, train_algorithm, train_parameters={}):
    df = training_ds.read_df()
    train_data, train_label = prepare_data(df, features)

    model = train_algorithm(**train_parameters)
    model.fit(train_data, train_label)
    labels_ = model.labels_

    model_info = {"model": model, "features": features}
    model = DataSource.write_pickle(model_info)

    df["label"] = labels_
    return Outputs(model=model, transform_trainds=DataSource.write_df(df))


def transform(training_ds, train_algorithm, train_parameters={}, features=None, model=None):
    df = training_ds.read_df()
    if not model:
        # log.warning("model为空，重新运行模型训练")
        model = train_algorithm(**train_parameters)
        model.fit(df[features])
    else:
        model_info = model.read_pickle()
        model = model_info["model"]
        features = model_info["features"]
    df[features] = model.transform(df[features])
    model_info = {"model": model, "features": features}
    model = DataSource.write_pickle(model_info)
    return Outputs(model=model, transform_ds=DataSource.write_df(df))


def predict(predict_ds, model, key_cols, is_predict_proba=False):
    model_info = model.read_pickle()
    features = model_info["features"]
    df = predict_ds.read_df()
    predict_data, true_label = prepare_data(df, features)

    pred_label = model_info["model"].predict(predict_data)
    data = {"pred_label": pred_label}
    if is_predict_proba:
        # pred_prob = model_info['model'].predict_proba(predict_data).max(axis=1)
        # data['pred_prob'] = pred_prob
        pred_prob = model_info["model"].predict_proba(predict_data)
        for class_name, class_prob in zip(model_info["model"].classes_, pred_prob.T):
            data["".join(["classes_prob_", str(class_name)])] = class_prob

    pred_df = pd.DataFrame(data=data, index=df.index)
    key_cols = str_split(key_cols)
    if key_cols:
        pred_df[key_cols] = df[key_cols]
    if true_label is not None:
        pred_df["label"] = true_label
    predictions = DataSource.write_df(pred_df)
    return Outputs(predictions=predictions)


def cluster_predict(predict_ds, model, key_cols):
    model_info = model.read_pickle()
    features = model_info["features"]
    df = predict_ds.read_df()
    predict_data, true_label = prepare_data(df, features)

    pred_label = model_info["model"].fit_predict(predict_data)
    data = {"pred_label": pred_label}

    pred_df = pd.DataFrame(data=data, index=df.index)
    key_cols = str_split(key_cols)
    if key_cols:
        pred_df[key_cols] = df[key_cols]
    if true_label is not None:
        pred_df["label"] = true_label
    predictions = DataSource.write_df(pred_df)
    return Outputs(predictions=predictions)
