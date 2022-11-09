from learning.module2.common.data import Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
import learning.module2.common.dlutils as DL


# 是否自动缓存结果
bigquant_cacheable = False

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = False
bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = r"深度学习\常用层"
bigquant_friendly_name = "输入层(Input)"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    shape: I.str("shape, 输入张量形状，用英文逗号(,)分隔的整数列表，例如 32,24，表示输入数据是一系列的 32x24的矩阵") = None,
    batch_shape: I.str(
        "batch_shape, A shape tuple (integer), including the batch size. For instance, `batch_shape=10,32` indicates that the expected input will be batches of 10 32-dimensional vectors. `batch_shape=None,32` indicates batches of an arbitrary number of 32-dimensional vectors."
    ) = None,
    dtype: I.str("数据类型, The data type expected by the input, as a string (`float32`, `float64`, `int32`...)") = "float32",
    sparse: I.bool("sparse, A boolean specifying whether the placeholder to be created is sparse.") = False,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入", optional=True) = None,
) -> [I.port("输出", "data"),]:
    """
    张量输入层
    """

    shape = smart_list(shape, sep=",", cast_type=int, drop_empty_lines=True) or None
    batch_shape = smart_list(batch_shape, sep=",", cast_type=int, drop_empty_lines=True) or None
    dtype = dtype or "float32"
    name = name or DL.make_name()

    from tensorflow.keras.layers import Input

    if shape:
        input = Input(shape=shape, name=name, dtype=dtype, sparse=sparse, tensor=inputs)
    elif batch_shape:
        input = Input(batch_shape=batch_shape, name=name, dtype=dtype, sparse=sparse, tensor=inputs)

    return Outputs(data=input)


if __name__ == "__main__":
    # 测试代码
    pass
