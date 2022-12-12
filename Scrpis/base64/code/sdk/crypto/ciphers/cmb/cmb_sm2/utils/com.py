# 作者     ：wujm
# 创建时间 ：2019/7/15 14:24
# IDE      : PyCharm
import binascii
import os
import shutil

"""
Delete folder content
path: folder path
"""


def delFile(path):
    shutil.rmtree(path)
    os.makedirs(path)


# byte转16进制
def bcdhex_to_aschex(data):
    output = binascii.hexlify(data)
    return output


# 16进制转byte
def aschex_to_bcdhex(data):
    output = binascii.unhexlify(data)
    return output


# 字符串 转16进制 字符串
def str_to_hex(s):
    return "".join([hex(ord(c)).replace("0x", "") for c in s])


# 字符串(包含gbk中文) 转16进制 字符串
def gbkstr_to_hex(s):
    s = s.encode("gbk")
    s = bcdhex_to_aschex(s).decode()
    return s


# 字符串(包含utf-8中文) 转16进制 字符串
def utfstr_to_hex(s):
    s = s.encode("utf-8")
    s = bcdhex_to_aschex(s).decode()
    return s


# 16进制字符串 转 gbk字符串
def hex_to_gbkstr(s):
    # s = s.encode('gbk')
    s = aschex_to_bcdhex(s).decode("gbk")
    return s


# 16进制字符串 转 utf-8字符串
def hex_to_utfstr(s):
    # s = s.encode('utf-8')
    s = aschex_to_bcdhex(s).decode("utf-8")
    return s


# list转bytes
list_to_bytes = lambda data: b"".join([bytes((i,)) for i in data])

# bytes转list
bytes_to_list = lambda data: [i for i in data]

# 异或
xor = lambda a, b: list(map(lambda x, y: x ^ y, a, b))


# 填充 4字节明文长度+明文+补位‘0’，data需为16进制数据
def pad_len(block, data):
    datalen = str_to_hex(str(len(data) // 2).zfill(4))
    data = datalen + data
    l = (block - len(data) % block) // 2
    data = data + l * "30"
    return data


# 填充 3字节明文长度+明文+补位 0X00 ，data需为16进制数据
def pad_len_2(block, data):
    datalen = str_to_hex(str(len(data) // 2).zfill(3))
    data = datalen + data
    l = (block - len(data) % block) // 2
    data = data + l * "00"
    return data


# 填充0x00(不强制填充)，data需为16进制数据
def pad_00(block, data):
    if len(data) % block != 0:
        l = (block - len(data) % block) // 2
        data = data + l * "00"
    return data


# 填充0x00(强制填充，满足分段也至少填充一次)，data需为16进制数据
def pad_00_force(block, data):
    l = (block - len(data) % block) // 2
    data = data + l * "00"
    return data


# 填充0x80（不强制填充,），data需为16进制数据
def pad_80(block, data):
    if len(data) % block != 0:
        l = (block - len(data) % block) // 2 - 1
        data = data + "80" + l * "00"
    return data


# 填充0x80（强制填充,满足分段也至少填充一次），data需为16进制数据
def pad_80_force(block, data):
    l = (block - len(data) % block) // 2 - 1
    data = data + "80" + l * "00"
    return data


# 去填充 4字节明文长度+明文+补位‘0’
def un_pad_len(data):
    datalen = int(aschex_to_bcdhex(data[0:8]).decode()) * 2
    data = data[8 : 8 + datalen]
    return data


# 去填充 3字节明文长度+明文+补位 0X00
def un_pad_len_2(data):
    datalen = int(aschex_to_bcdhex(data[0:6]).decode()) * 2
    data = data[6 : 6 + datalen]
    return data


# 去填充0x00
def un_pad_00(data):
    data = data.split("00", 1)[0]
    return data


# 去填充0x80
def un_pad_80(data):
    if data[-2:] == "80":
        data = data[0:-2]
    else:
        index = data.rfind("8000")
        if index > 0:
            data = data[0:index]
        else:
            pass
    return data


# C1C2C3转C1C3C2
def C1C2C3_to_C13C2(data):
    datalen = len(data)
    C1 = data[0:128]
    C2 = data[128 : datalen - 64]
    C3 = data[datalen - 64 :]
    return C1 + C3 + C2


# C1C3C2转C1C2C3
def C1C3C2_to_C12C3(data):
    C1 = data[0:128]
    C3 = data[128 : 128 + 64]
    C2 = data[128 + 64 :]
    return C1 + C2 + C3


# 16进制字符串异或
def stringxor(str1, str2):  # 传入两个16进制字符串，并返回它们的异或结果，返回字符串结果（16进制）
    l = len(str1)
    byte1 = int(str1, 16)
    byte2 = int(str2, 16)
    result = hex(byte1 ^ byte2).replace("0x", "").upper().zfill(l)
    return result


# 判断字符串是否是十六进制
def strisHex(str):
    for i in str:
        if ("0" > i or "9" < i) and ("A" > i or "F" < i) and ("a" > i or "f" < i) or len(str) % 2 != 0:
            return 0
        else:
            return 1


"""
from Crypto.Cipher import ARC4
def myRC4(data,key):
    rc41 = ARC4.new(key)
    encrypted = rc41.encrypt(data)
    return bcdhex_to_aschex(encrypted)

print(myRC4("12345678".encode(),"89C8296425458954E9BAB35710325EC7".encode()))
"""


def UnionRSToSM2SigDer(rs):
    trlen = 0
    tslen = 0
    len = 0
    tmpbuf = bytes_to_list(rs)

    r = tmpbuf[0:32]
    s = tmpbuf[32:64]
    if (r[0] & 0x80) != 0:
        derBuf = [0x00] + r
        trlen = 33
    else:
        derBuf = r
        trlen = 32

    tr = [0x02] + [trlen] + derBuf
    trlen = trlen + 2

    if (s[0] & 0x80) != 0:
        derBuf = [0x00] + s
        tslen = 33
    else:
        derBuf = s
        tslen = 32

    ts = [0x02] + [tslen] + derBuf
    tslen = tslen + 2

    derBuf = [0x30] + [trlen + tslen] + tr + ts
    len = 2 + trlen + tslen
    signatureDer = list_to_bytes(derBuf)
    return signatureDer


def UnionGetRSFromSM2SigDer(signatureDer):
    derBuf = bytes_to_list(signatureDer)
    if len(derBuf) < 70:
        return None
    trlen = 0
    tslen = 0
    offset = 2
    if derBuf[0] == 0x30:
        length = derBuf[1]
        if len(derBuf[2:]) != length:
            return None
        else:
            if derBuf[2] != 0x02:
                return None
            offset += 1
            if (derBuf[offset] == 0x21) and (derBuf[offset + 1] == 0x00) and (derBuf[offset + 2] & 0x80) != 0:
                offset += 2
                r = derBuf[offset : offset + 32]
            elif derBuf[offset] == 0x20:
                offset += 1
                r = derBuf[offset : offset + 32]
            else:
                return None
            offset += 32
            if derBuf[offset] != 0x02:
                return None
            offset += 1
            if (derBuf[offset] == 0x21) and (derBuf[offset + 1] == 0x00) and (derBuf[offset + 2] & 0x80) != 0:
                offset += 2
                s = derBuf[offset : offset + 32]
            elif derBuf[offset] == 0x20:
                offset += 1
                s = derBuf[offset : offset + 32]
            else:
                return None
        return list_to_bytes(r + s)
    else:
        return None
