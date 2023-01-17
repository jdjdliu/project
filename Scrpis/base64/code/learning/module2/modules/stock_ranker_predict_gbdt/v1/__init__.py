import datetime

import pandas as pd

import learning.module2.common.interface as I
from learning.api import M
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_object
from sdk.utils import BigLogger

bigquant_cacheable = True
# log = logbook.Logger('stock_ranker_predict')
log = BigLogger("stock_ranker_predict")
bigquant_public = True


# 模块接口定义
bigquant_category = r"机器学习\排序"
bigquant_friendly_name = "StockRanker预测(GBDT)"
bigquant_doc_url = "https://bigquant.com/docs/develop/modules/stock_ranker_predict.html"


def __read_hdf(ds, start_date, end_date):
    from bigshared.common.dataframe import truncate

    df_list = [truncate(d, start_date, end_date, reset_index=False) for _, d in ds.iter_df()]
    if not df_list:
        return None
    return pd.concat(df_list, copy=False, ignore_index=True)


def get_bin_file(data_ds, features, start_date=None, end_date=None):
    import xgboost as xgb

    df = __read_hdf(data_ds, start_date, end_date)
    df.sort_values(["date", "instrument"], inplace=True)
    data = df[features].values
    if "label" in df.columns:
        label = df["label"].values
    else:
        label = None
    dm = xgb.DMatrix(data, label=label)
    dm.set_group(list(df.groupby("date").apply(len)))
    dm.feature_names = features
    bin_ds = DataSource()
    bin_ds.on_temp_path(dm.save_binary)
    return Outputs(bin_ds=bin_ds, rows=len(df))


class BigQuantModule:
    def __init__(
        self, model: I.port("模型", specific_type_name="字符串"), data: I.port("数据", specific_type_name="DataSource")  # noqa
    ) -> [I.port("预测结果", "predictions"), I.port("延迟运行，将当前模块打包，可以作为其他模块的输入，在其他模块里运行。启用需要勾选模块的 延迟运行 参数。", I.port_name_lazy_run),]:
        """

        股票排序机器学习模型(GBDT)预测。支持来自滚动运行输出的多个模型。

        """

        self.model_id = smart_object(model)
        self.data = data

    def run_simple(self):
        import xgboost as xgb

        model_info = DataSource(self.model_id).read_pickle()
        features = model_info["features"]
        model_ds = DataSource(model_info["model_id"])
        bst = xgb.Booster()
        model_ds.on_temp_path(bst.load_model)

        bin_data = M.cached.v2(run=get_bin_file, kwargs=dict(data_ds=self.data, features=features))

        if bin_data.rows == 0:
            log.warning("没有数据用于预测")
            return Outputs(predictions=None)

        dm = bin_data.bin_ds.on_temp_path(lambda x: xgb.DMatrix(x))

        data_df = pd.concat([d for _, d in self.data.iter_df()], copy=False, ignore_index=True)
        data_df.sort_values(["date", "instrument"], inplace=True)

        pred_score = bst.predict(dm)
        score_df = pd.DataFrame({"date": data_df.date, "instrument": data_df.instrument, "score": pred_score})

        def _calculate_position(df):
            df = df.sort_values("score", ascending=False)
            df["position"] = range(1, len(df) + 1)
            return df

        score_df = score_df.groupby("date", group_keys=None).apply(_calculate_position)
        score_df.sort_values(["date", "position"], inplace=True)
        score_df.reset_index(drop=True, inplace=True)
        predictions = DataSource.write_df(score_df)
        instruments = sorted(set(data_df.instrument))
        start_date = str(min(data_df.date))
        end_date = str(min(data_df.date))
        outputs = Outputs(predictions=predictions, instruments=instruments, start_date=start_date, end_date=end_date)
        return outputs

    def run_batch(self):
        last_train_end = None
        for m in reversed(self.model_id):
            m["predict_start_date"] = (pd.to_datetime(m["end_date"]) + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            m["predict_end_date"] = last_train_end
            last_train_end = m["end_date"]
        log.info("滚动预测 ..")
        rollings = []
        for m in self.model_id:
            if not m or not m["output"] or not m["output"].model:
                continue
            log.info("预测数据 [%s, %s]，模型训练数据 [%s, %s] .." % (m["predict_start_date"], m["predict_end_date"], m["start_date"], m["end_date"]))
            if m["predict_end_date"] is not None:
                expr = '"%s" <= date <= "%s"' % (m["predict_start_date"], m["predict_end_date"])
            else:
                expr = '"%s" <= date' % (m["predict_start_date"])
            m_rolling = M.filter.v3(input_data=self.data, expr=expr)
            if m_rolling.row_count < 0:
                log.warning("没有数据需要预测")
                continue
            item = M.stock_ranker_predict_gbdt.v1(model=m["output"].model, data=m_rolling.data)
            rollings.append(item)

        outputs = Outputs(rollings=rollings)
        predictions = [r.predictions.read_df() for r in rollings if r.predictions is not None]
        if predictions:
            predictions = pd.concat(predictions, copy=False, ignore_index=True)
            outputs.start_date = predictions.date.min().strftime("%Y-%m-%d")
            outputs.end_date = predictions.date.max().strftime("%Y-%m-%d")
            outputs.instruments = sorted(set(predictions.instrument))
            outputs.predictions = DataSource.write_df(predictions)
        else:
            outputs.predictions = None
        return outputs

    def run(self):
        if not self.model_id:
            log.warn("预测失败：模型为None")
            return Outputs(predictions=None)
        if isinstance(self.model_id, str):
            return self.run_simple()
        return self.run_batch()


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # test code
    pass
