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
bigquant_category = r"深度学习\常用层"
bigquant_friendly_name = "Masking层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    mask_value: I.float("mask_value") = 0.0, name: DL.PARAM_NAME = None, inputs: I.port("输入") = None
) -> [I.port("输出", "data"),]:
    """
    屏蔽层。使用给定的值对输入的序列信号进行“屏蔽”，用以定位需要跳过的时间步。对于输入张量的时间步，即输入张量的第1维度（维度从0开始算，见例子），如果输入张量在该时间步上都等于mask_value，则该时间步将在模型接下来的所有层（只要支持masking）被跳过（屏蔽）。如果模型接下来的一些层不支持masking，却接受到masking过的数据，则抛出异常。
    """

    from tensorflow.keras.layers import Masking

    layer = Masking(mask_value=mask_value)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
