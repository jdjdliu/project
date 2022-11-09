from tensorflow.keras.layers import Dense, Layer


class LayerNormalization(Layer):
    def __init__(self, eps=1e-6, **kwargs):
        self.eps = eps
        super(LayerNormalization, self).__init__(**kwargs)

    def get_config(self):
        config = {"eps": self.eps}

        base_config = super(LayerNormalization, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))

    def build(self, input_shape):
        from tensorflow.keras.initializers import Ones, Zeros

        self.gamma = self.add_weight(name="gamma", shape=input_shape[-1:], initializer=Ones(), trainable=True)
        self.beta = self.add_weight(name="beta", shape=input_shape[-1:], initializer=Zeros(), trainable=True)
        super(LayerNormalization, self).build(input_shape)

    def call(self, x):
        import tensorflow.keras.backend as K

        mean = K.mean(x, axis=-1, keepdims=True)
        std = K.std(x, axis=-1, keepdims=True)
        return self.gamma * (x - mean) / (std + self.eps) + self.beta

    def compute_output_shape(self, input_shape):
        return input_shape
