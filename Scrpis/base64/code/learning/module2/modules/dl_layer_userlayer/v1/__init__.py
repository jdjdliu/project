from learning.module2.common.data import Outputs
import learning.module2.common.interface as I
import learning.module2.common.dlutils as DL
from learning.module2.common.utils import smart_dict


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
bigquant_category = r"深度学习\自定义层"
bigquant_friendly_name = "自定义层"
bigquant_doc_url = "https://bigquant.com/docs/"


DEFAULT_FUNCTION = """from tensorflow.keras.layers import Layer

class UserLayer(Layer):

    def __init__(self):
        self.output_dim = 123
        super(UserLayer, self).__init__()

    def build(self, input_shape):
        # Create a trainable weight variable for this layer.
        self.kernel = self.add_weight(name='kernel',
                                      shape=(input_shape[1], self.output_dim),
                                      initializer='uniform',
                                      trainable=True)
        super(UserLayer, self).build(input_shape)  # Be sure to call this somewhere!

    def call(self, x):
        import tensorflow.keras.backend as K
        return K.dot(x, self.kernel)

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.output_dim)

# 必须也将 UserLayer 赋值给 bigquant_run
bigquant_run = UserLayer
"""


def bigquant_run(
    layer_class: I.code("用户层定义", I.code_python, default=DEFAULT_FUNCTION),
    input1: I.port("输入1") = None,
    input2: I.port("输入2", optional=True) = None,
    input3: I.port("输入3", optional=True) = None,
    params: I.code("用户层输入参数，字典形式，给出参数的值。比如{'param1':1,'param2':2}", default="{}", specific_type_name="字典") = {},
    name: DL.PARAM_NAME = None,
) -> [I.port("输出", "data"),]:
    """
        对于简单的定制操作，我们或许可以通过使用layers.core.Lambda层来完成。但对于任何具有可训练权重的定制层，你应该自己来实现。
    这里是一个层应该具有的框架结构，要定制自己的层，你需要实现下面三个方法：
     - build(input_shape)：这是定义权重的方法，可训练的权应该在这里被加入列表`self.trainable_weights中。其他的属性还包括self.non_trainabe_weights（列表）和self.updates（需要更新的形如（tensor, new_tensor）的tuple的列表）。你可以参考BatchNormalization层的实现来学习如何使用上面两个属性。这个方法必须设置self.built = True，可通过调用super([layer],self).build()实现
     - call(x)：这是定义层功能的方法，除非你希望你写的层支持masking，否则你只需要关心call的第一个参数：输入张量
     - compute_output_shape(input_shape)：如果你的层修改了输入数据的shape，你应该在这里指定shape变化的方法，这个函数使得Keras可以做自动shape推断

    """

    layer = layer_class(**smart_dict(params))
    layer = DL.post_build_layer(layer, name, DL.drop_none_input([input1, input2, input3]))

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
