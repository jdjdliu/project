import numpy as np
import pandas as pd

import learning.api.tools as T


def plot_label_counts(outputs):
    df = outputs.data.read_df()
    if outputs.cast_label_int:
        label_counts = sorted(df["label"].value_counts().to_dict().items())
        df = pd.DataFrame(label_counts, columns=["label", "count"]).set_index("label")
        T.plot(df, title="数据标注分布", double_precision=0, chart_type="column")
    else:
        bin_counts = np.histogram(df["label"], bins=20)
        label_counts = pd.DataFrame(data=list(bin_counts)).transpose()
        label_counts.columns = ["count", "label"]
        T.plot(
            label_counts,
            x="label",
            y=["count"],
            chart_type="column",
            title="label",
            options={"series": [{"pointPadding": 0, "groupPadding": 0, "pointPlacement": "between"}]},
        )
