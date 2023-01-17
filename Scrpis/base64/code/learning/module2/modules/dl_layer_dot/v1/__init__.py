from learning.module2.common.data import Outputs
import learning.module2.common.interface as I
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
bigquant_category = r"深度学习\融合层"
bigquant_friendly_name = "Dot层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    input1: I.port("输入1"),
    input2: I.port("输入2"),
    axes: I.str("axes，执行乘法的轴，用英文逗号(,)分隔的整数列表，例如 32,24"),
    normalize: I.bool("normalize，是否沿执行成绩的轴做L2规范化，如果设为True，那么乘积的输出是两个样本的余弦相似性。") = False,
    name: DL.PARAM_NAME = None,
) -> [I.port("输出", "data"),]:
    """
    点积层，属于融合层。计算两个tensor中样本的张量乘积。例如，如果两个张量a和b的shape都为（batch_size, n），则输出为形如（batch_size,1）的张量，结果张量每个batch的数据都是a[i,:]和b[i,:]的矩阵（向量）点积。
    """

    from tensorflow.keras.layers import Dot

    layer = Dot(axes=axes, normalize=normalize)
    layer = DL.post_build_layer(layer, name, [input1, input2])

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
