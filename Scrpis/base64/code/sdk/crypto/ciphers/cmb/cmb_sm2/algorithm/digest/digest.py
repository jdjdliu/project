# 作者     ：wujm
# 创建时间 ：2019/7/17 11:32
# IDE      : PyCharm
import os
import sys

sys.path.append(os.path.abspath("../../gmssl"))
sys.path.append(os.path.abspath("../../utils"))

import hashlib

from ...gmssl import func, sm3
from ...utils import com

# 算法摘要


class Digest:
    # sm3算法摘要,带公钥和userid参与,userid默认为1234567812345678
    # data  Hex  pkValue Hex
    def union_sm3_e(self, data, pkValue, userid="1234567812345678"):
        sm2_par_dig = "FFFFFFFEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00000000FFFFFFFFFFFFFFFC28E9FA9E9D9F5E344D5A9E4BCF6509A7F39789F515AB8F92DDBCBD414D940E9332C4AE2C1F1981195F9904466A39C9948FE30BBFF2660BE1715A4589334C74C7BC3736A2F4F6779C59BDCEE36B692153D0A9877CC62A474002DF32E52139F0A0"
        userid_len = len(userid)
        tmpId = userid
        userid_bitlen = userid_len << 3
        tmp1 = hex((userid_bitlen >> 8) & 0xFF).replace("0x", "")
        if tmp1 == "0":
            tmp1 = "00"
        tmp2 = hex(userid_bitlen & 0xFF).replace("0x", "")
        tmpId = com.str_to_hex(tmpId)
        tmp3 = tmp1 + tmp2 + tmpId + sm2_par_dig + pkValue
        tmpdata = com.aschex_to_bcdhex(tmp3)
        # 先对公钥及userId做一次SM3摘要
        e = sm3.sm3_hash(func.bytes_to_list(tmpdata))
        # 然后对上一步摘要结果和data 组成的串再进行一次SM3摘要
        # e = e + common.str_to_hex(data)
        e = e + data
        tmpdata2 = com.aschex_to_bcdhex(e)
        e = sm3.sm3_hash(func.bytes_to_list(tmpdata2))
        return e.upper()

    def union_sm3_ewithoutData(self, pkValue, userid=b"1234567812345678"):
        sm2_par_dig = "FFFFFFFEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00000000FFFFFFFFFFFFFFFC28E9FA9E9D9F5E344D5A9E4BCF6509A7F39789F515AB8F92DDBCBD414D940E9332C4AE2C1F1981195F9904466A39C9948FE30BBFF2660BE1715A4589334C74C7BC3736A2F4F6779C59BDCEE36B692153D0A9877CC62A474002DF32E52139F0A0"
        userid_len = len(userid)
        tmpId = userid
        userid_bitlen = userid_len << 3
        tmp1 = hex((userid_bitlen >> 8) & 0xFF).replace("0x", "").zfill(2)
        tmp2 = hex(userid_bitlen & 0xFF).replace("0x", "").zfill(2)

        tmpId = tmpId.hex().upper()
        pkValue = pkValue.hex().upper()
        tmp3 = tmp1 + tmp2 + tmpId + sm2_par_dig + pkValue
        tmpdata = com.aschex_to_bcdhex(tmp3)
        # 先对公钥及userId做一次SM3摘要
        e = sm3.sm3_hash(func.bytes_to_list(tmpdata))
        return e.upper()

    # md5算法摘要
    def md5(self, data):
        m = hashlib.md5()
        tmp = com.aschex_to_bcdhex(data)
        m.update(tmp)
        hash = m.hexdigest()
        return hash.upper()

    # sha1算法摘要
    def sha1(self, data):
        m = hashlib.sha1()
        tmp = com.aschex_to_bcdhex(data)
        m.update(tmp)
        hash = m.hexdigest()
        return hash.upper()

    # sha256算法摘要
    def sha256(self, data):
        m = hashlib.sha256()
        tmp = com.aschex_to_bcdhex(data)
        m.update(tmp)
        hash = m.hexdigest()
        return hash.upper()

    # sha384算法摘要
    def sha384(self, data):
        m = hashlib.sha384()
        tmp = com.aschex_to_bcdhex(data)
        m.update(tmp)
        hash = m.hexdigest()
        return hash.upper()

    # sha512算法摘要
    def sha512(self, data):
        m = hashlib.sha512()
        tmp = com.aschex_to_bcdhex(data)
        m.update(tmp)
        hash = m.hexdigest()
        return hash.upper()


if __name__ == "__main__":
    pkValue = "B9C9A6E04E9C91F7BA880429273747D7EF5DDEB0BB2FF6317EB00BEF331A83081A6994B8993F3F5D6EADDDB81872266C87C018FB4162F5AF347B483E24620207"
    d = Digest()
    key = "3031323334353637383941424344454646454443424139383736353433323130"
    data = "313233343536"

    x = d.CMBSM3Digest(com.aschex_to_bcdhex(data))
    h = d.CMBSM3HMAC(com.aschex_to_bcdhex(key), com.aschex_to_bcdhex(data))
    print(x)
    print(h)
