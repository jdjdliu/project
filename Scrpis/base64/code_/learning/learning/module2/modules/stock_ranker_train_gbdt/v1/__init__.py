import learning.module2.common.interface as I
import pandas as pd
from learning.api import M
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

# log = logbook.Logger('stock_ranker_train')
log = BigLogger("stock_ranker_train")
bigquant_cacheable = True
# bigquant_public = False


LEARNING_ALGORITHMS = {
    "rank": "rank:pairwise",
    "排序": "rank:pairwise",
}


# 模块接口定义
bigquant_category = r"机器学习\排序"
bigquant_friendly_name = "StockRanker训练(GBDT)"
bigquant_doc_url = "https://bigquant.com/docs/develop/modules/stock_ranker_train.html"


def __read_hdf(ds, start_date, end_date):
    from sdk.datasource.extensions.bigshared.dataframe import truncate

    df_list = [truncate(d, start_date, end_date, reset_index=False) for _, d in ds.iter_df()]
    if not df_list:
        return None
    return pd.concat(df_list, copy=False, ignore_index=True)


def get_bin_file(data_ds, features, start_date=None, end_date=None):
    import xgboost as xgb

    df = __read_hdf(data_ds, start_date, end_date)
    df.sort_values(["date", "instrument"], inplace=True)
    data = df[features].values
    label = None
    if "label" in df.columns:
        label = df["label"].values
    dm = xgb.DMatrix(data, label=label)
    dm.set_group(list(df.groupby("date").apply(len)))
    dm.feature_names = features
    bin_ds = DataSource()
    bin_ds.on_temp_path(dm.save_binary)
    return Outputs(bin_ds=bin_ds, rows=len(df))


class BigQuantModule:
    def __init__(
        self,
        training_ds: I.port("训练数据，需要包含所有用到的特征数据，包括基础特征和衍生特征", specific_type_name="DataSource"),
        features: I.port("特征列表", specific_type_name="列表|DataSource"),
        learning_algorithm: I.choice("学习算法，机器学习优化算法", ["排序"]) = "排序",
        number_of_leaves: I.int("叶节点数量：每棵树最大叶节点数量。一般情况下，叶子节点越多，则模型越复杂，表达能力越强，过拟合的可能性也越高", min=1) = 30,
        minimum_docs_per_leaf: I.int("每叶节点最小样本数：每个叶节点最少需要的样本数量，一般值越大，泛化性性越好", min=1) = 30,
        number_of_trees: I.int("树的数量：一般情况下，树越多，则模型越复杂，表达能力越强，过拟合的可能性也越高", min=1) = 100,
        learning_rate: I.float("学习率：学习率如果太大，可能会使结果越过最优值，如果太小学习会很慢", 0.0, 1.0) = 0.1,
        max_bins: I.int("特征值离散化数量：一般情况下，max_bins越大，则学的越细，过拟合的可能性也越高", min=1) = 256,
        rolling_input: I.doc("滚动运行参数，接收来自滚动运行的输入，用于训练数据过滤", specific_type_name="dict") = None,
    ) -> [
        I.port("模型", "model"),
        I.port("特征贡献", "feature_gains", optional=True),
        I.port("延迟运行，将当前模块打包，可以作为其他模块的输入，在其他模块里运行。启用需要勾选模块的 延迟运行 参数。", I.port_name_lazy_run),
    ]:
        """

        StockRanker排序学习模型(GBDT)训练。StockRanker属于集成学习，模型由多棵决策树组成，所有树的结论累加起来做为最终决策分数。

        """
        self.training_ds = training_ds
        self.features = smart_list(features)
        self.number_of_leaves = number_of_leaves
        self.minimum_docs_per_leaf = minimum_docs_per_leaf
        self.number_of_trees = number_of_trees
        self.learning_rate = learning_rate
        if learning_algorithm not in LEARNING_ALGORITHMS:
            raise Exception("未知的学习算法：%s" % learning_algorithm)
        self.learning_algorithm = LEARNING_ALGORITHMS[learning_algorithm]
        self.__max_bins = max_bins

        if rolling_input:
            self.__start_date = rolling_input["start_date"]
            self.__end_date = rolling_input["end_date"]
        else:
            self.__start_date = None
            self.__end_date = None

    def run(self):
        import xgboost as xgb

        train_bin_data = M.cached.v2(
            run=get_bin_file, kwargs=dict(data_ds=self.training_ds, features=self.features, start_date=self.__start_date, end_date=self.__end_date)
        )

        log.info("准备训练, 行数%s" % train_bin_data.rows)
        if train_bin_data.rows == 0:
            log.warning("没有数据用于训练")
            return Outputs(model_id=None, model=None)

        train_dm = train_bin_data.bin_ds.on_temp_path(lambda x: xgb.DMatrix(x))
        xgb_param = {}
        xgb_param["eta"] = self.learning_rate
        xgb_param["objective"] = self.learning_algorithm
        xgb_param["eval_metric"] = "ndcg@5"
        xgb_param["silent"] = 0
        xgb_param["lambda"] = 0
        xgb_param["tree_method"] = "exact"
        xgb_param["gamma"] = 0.0001
        xgb_param["max_bin"] = self.__max_bins
        xgb_param["max_leaves"] = self.number_of_leaves
        xgb_param["subsample"] = 0.8
        xgb_param["max_leaf_nodes"] = self.minimum_docs_per_leaf
        xgb_param["colsample_bylevel"] = 0.8 if len(self.features) > 4 else 1.0
        bst = xgb.train(
            params=xgb_param, dtrain=train_dm, num_boost_round=self.number_of_trees, early_stopping_rounds=None, evals=[], verbose_eval=1000000
        )

        bst.feature_names = self.features
        feature_gains = pd.DataFrame(list(bst.get_fscore().items()), columns=["feature", "gain"])
        feature_gains.sort_values("gain", ascending=False, inplace=True)
        feature_gains.reset_index(drop=True, inplace=True)
        feature_gains = DataSource.write_df(feature_gains)

        model_ds = DataSource()
        model_ds.on_temp_path(bst.save_model)
        model_info = {"features": self.features, "model_id": model_ds.id}
        model_ds = DataSource.write_pickle(model_info)
        outputs = Outputs(model_id=model_ds.id, model=DataSource.write_pickle(model_ds.id), feature_gains=feature_gains)
        return outputs


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # test code
    pass
