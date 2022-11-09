import learning.module2.common.dlutils as DL


def plot(outputs):
    model_yaml = outputs.data.read_pickle()

    model = DL.model_from_yaml(model_yaml)

    from tensorflow.keras.utils.vis_utils import model_to_dot

    dot = model_to_dot(model).create(prog="dot", format="svg")
    from IPython.display import display, SVG

    display(SVG(dot))
