# 作者     ：tianfw
# 创建时间 ：2020/3/11 17:24
# IDE      : PyCharm
import os
import sys

sys.path.append(os.path.abspath("../../gmssl"))

from ...gmssl import func, sm2


class SM2:
    def __init__(self, public_key, private_key):
        # if private_key[1] > "8":
        #    private_key = private_key + "00"
        self.sm2_crypt = sm2.CryptSM2(public_key=public_key, private_key=private_key)

    # 签名
    def sign(self, data):
        random_hex_str = func.random_hex(self.sm2_crypt.para_len)  # 随机数
        sign = self.sm2_crypt.sign(data, random_hex_str)  #  16进制
        return sign.upper()

    # 验签
    def verify(self, data, sign):
        r = self.sm2_crypt.verify(sign, data)
        return r

    # 加密，结果为C1C3C2
    def encrypt(self, data):
        enc_data = self.sm2_crypt.encrypt(data).hex()
        return enc_data.upper()

    # 解密,enc_data需要为C1C3C2
    def decrypt(self, enc_data):
        r = self.sm2_crypt.decrypt(enc_data)
        return r
