import pandas as pd
import learning.module2.common.dlutils as DL
import learning.api.tools as T


def load_model(outputs):
    model_dict = outputs.data.read_pickle()
    model = DL.model_from_yaml(model_dict["model_graph"])
    model.set_weights(model_dict["model_weights"])
    return model


def plot_result(outputs):
    model_dict = outputs.data.read_pickle()
    history_data = model_dict["history"]
    df = pd.DataFrame(history_data)
    for name, display_name in [["loss", "Loss"], ["acc", "准确率"]]:
        keys = [x for x in df.columns if name in x]
        if keys:
            T.plot(df[keys], title=display_name)
