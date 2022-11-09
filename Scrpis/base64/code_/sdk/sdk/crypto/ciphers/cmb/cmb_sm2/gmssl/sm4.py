# -*-coding:utf-8-*-
import copy
import threading

from .func import bytes_to_list, get_uint32_be, list_to_bytes, padding, put_uint32_be, rotl, unpadding, xor

# Expanded SM4 box table
SM4_BOXES_TABLE = [
    0xD6,
    0x90,
    0xE9,
    0xFE,
    0xCC,
    0xE1,
    0x3D,
    0xB7,
    0x16,
    0xB6,
    0x14,
    0xC2,
    0x28,
    0xFB,
    0x2C,
    0x05,
    0x2B,
    0x67,
    0x9A,
    0x76,
    0x2A,
    0xBE,
    0x04,
    0xC3,
    0xAA,
    0x44,
    0x13,
    0x26,
    0x49,
    0x86,
    0x06,
    0x99,
    0x9C,
    0x42,
    0x50,
    0xF4,
    0x91,
    0xEF,
    0x98,
    0x7A,
    0x33,
    0x54,
    0x0B,
    0x43,
    0xED,
    0xCF,
    0xAC,
    0x62,
    0xE4,
    0xB3,
    0x1C,
    0xA9,
    0xC9,
    0x08,
    0xE8,
    0x95,
    0x80,
    0xDF,
    0x94,
    0xFA,
    0x75,
    0x8F,
    0x3F,
    0xA6,
    0x47,
    0x07,
    0xA7,
    0xFC,
    0xF3,
    0x73,
    0x17,
    0xBA,
    0x83,
    0x59,
    0x3C,
    0x19,
    0xE6,
    0x85,
    0x4F,
    0xA8,
    0x68,
    0x6B,
    0x81,
    0xB2,
    0x71,
    0x64,
    0xDA,
    0x8B,
    0xF8,
    0xEB,
    0x0F,
    0x4B,
    0x70,
    0x56,
    0x9D,
    0x35,
    0x1E,
    0x24,
    0x0E,
    0x5E,
    0x63,
    0x58,
    0xD1,
    0xA2,
    0x25,
    0x22,
    0x7C,
    0x3B,
    0x01,
    0x21,
    0x78,
    0x87,
    0xD4,
    0x00,
    0x46,
    0x57,
    0x9F,
    0xD3,
    0x27,
    0x52,
    0x4C,
    0x36,
    0x02,
    0xE7,
    0xA0,
    0xC4,
    0xC8,
    0x9E,
    0xEA,
    0xBF,
    0x8A,
    0xD2,
    0x40,
    0xC7,
    0x38,
    0xB5,
    0xA3,
    0xF7,
    0xF2,
    0xCE,
    0xF9,
    0x61,
    0x15,
    0xA1,
    0xE0,
    0xAE,
    0x5D,
    0xA4,
    0x9B,
    0x34,
    0x1A,
    0x55,
    0xAD,
    0x93,
    0x32,
    0x30,
    0xF5,
    0x8C,
    0xB1,
    0xE3,
    0x1D,
    0xF6,
    0xE2,
    0x2E,
    0x82,
    0x66,
    0xCA,
    0x60,
    0xC0,
    0x29,
    0x23,
    0xAB,
    0x0D,
    0x53,
    0x4E,
    0x6F,
    0xD5,
    0xDB,
    0x37,
    0x45,
    0xDE,
    0xFD,
    0x8E,
    0x2F,
    0x03,
    0xFF,
    0x6A,
    0x72,
    0x6D,
    0x6C,
    0x5B,
    0x51,
    0x8D,
    0x1B,
    0xAF,
    0x92,
    0xBB,
    0xDD,
    0xBC,
    0x7F,
    0x11,
    0xD9,
    0x5C,
    0x41,
    0x1F,
    0x10,
    0x5A,
    0xD8,
    0x0A,
    0xC1,
    0x31,
    0x88,
    0xA5,
    0xCD,
    0x7B,
    0xBD,
    0x2D,
    0x74,
    0xD0,
    0x12,
    0xB8,
    0xE5,
    0xB4,
    0xB0,
    0x89,
    0x69,
    0x97,
    0x4A,
    0x0C,
    0x96,
    0x77,
    0x7E,
    0x65,
    0xB9,
    0xF1,
    0x09,
    0xC5,
    0x6E,
    0xC6,
    0x84,
    0x18,
    0xF0,
    0x7D,
    0xEC,
    0x3A,
    0xDC,
    0x4D,
    0x20,
    0x79,
    0xEE,
    0x5F,
    0x3E,
    0xD7,
    0xCB,
    0x39,
    0x48,
]

# System parameter
SM4_FK = [0xA3B1BAC6, 0x56AA3350, 0x677D9197, 0xB27022DC]

# fixed parameter
SM4_CK = [
    0x00070E15,
    0x1C232A31,
    0x383F464D,
    0x545B6269,
    0x70777E85,
    0x8C939AA1,
    0xA8AFB6BD,
    0xC4CBD2D9,
    0xE0E7EEF5,
    0xFC030A11,
    0x181F262D,
    0x343B4249,
    0x50575E65,
    0x6C737A81,
    0x888F969D,
    0xA4ABB2B9,
    0xC0C7CED5,
    0xDCE3EAF1,
    0xF8FF060D,
    0x141B2229,
    0x30373E45,
    0x4C535A61,
    0x686F767D,
    0x848B9299,
    0xA0A7AEB5,
    0xBCC3CAD1,
    0xD8DFE6ED,
    0xF4FB0209,
    0x10171E25,
    0x2C333A41,
    0x484F565D,
    0x646B7279,
]

SM4_ENCRYPT = 0
SM4_DECRYPT = 1

stream_key = []  # 列表的列表，里面存放每次流密码加密时的密钥
outData = []  # 存放密文


def XOR(M, i):
    global outData
    m = i * 16
    n = (i + 1) * 16
    outData += xor(M[m:n], stream_key[i + 1])


class CryptSM4(object):
    def __init__(self, mode=SM4_ENCRYPT):
        self.sk = [0] * 32
        self.mode = mode

    # Calculating round encryption key.
    # args:    [in] a: a is a 32 bits unsigned value;
    # return: sk[i]: i{0,1,2,3,...31}.
    @classmethod
    def _round_key(cls, ka):
        b = [0, 0, 0, 0]
        a = put_uint32_be(ka)
        b[0] = SM4_BOXES_TABLE[a[0]]
        b[1] = SM4_BOXES_TABLE[a[1]]
        b[2] = SM4_BOXES_TABLE[a[2]]
        b[3] = SM4_BOXES_TABLE[a[3]]
        bb = get_uint32_be(b[0:4])
        rk = bb ^ (rotl(bb, 13)) ^ (rotl(bb, 23))
        return rk

    # Calculating and getting encryption/decryption contents.
    # args:    [in] x0: original contents;
    # args:    [in] x1: original contents;
    # args:    [in] x2: original contents;
    # args:    [in] x3: original contents;
    # args:    [in] rk: encryption/decryption key;
    # return the contents of encryption/decryption contents.
    @classmethod
    def _f(cls, x0, x1, x2, x3, rk):
        # "T algorithm" == "L algorithm" + "t algorithm".
        # args:    [in] a: a is a 32 bits unsigned value;
        # return: c: c is calculated with line algorithm "L" and nonline algorithm "t"
        def _sm4_l_t(ka):
            b = [0, 0, 0, 0]
            a = put_uint32_be(ka)
            b[0] = SM4_BOXES_TABLE[a[0]]
            b[1] = SM4_BOXES_TABLE[a[1]]
            b[2] = SM4_BOXES_TABLE[a[2]]
            b[3] = SM4_BOXES_TABLE[a[3]]
            bb = get_uint32_be(b[0:4])
            c = bb ^ (rotl(bb, 2)) ^ (rotl(bb, 10)) ^ (rotl(bb, 18)) ^ (rotl(bb, 24))
            return c

        return x0 ^ _sm4_l_t(x1 ^ x2 ^ x3 ^ rk)

    def set_key(self, key, mode):
        key = bytes_to_list(key)
        MK = [0, 0, 0, 0]
        k = [0] * 36
        MK[0] = get_uint32_be(key[0:4])
        MK[1] = get_uint32_be(key[4:8])
        MK[2] = get_uint32_be(key[8:12])
        MK[3] = get_uint32_be(key[12:16])
        k[0:4] = xor(MK[0:4], SM4_FK[0:4])
        for i in range(32):
            k[i + 4] = k[i] ^ (self._round_key(k[i + 1] ^ k[i + 2] ^ k[i + 3] ^ SM4_CK[i]))
            self.sk[i] = k[i + 4]
        self.mode = mode
        if mode == SM4_DECRYPT:
            for idx in range(16):
                t = self.sk[idx]
                self.sk[idx] = self.sk[31 - idx]
                self.sk[31 - idx] = t

    def one_round(self, sk, in_put):
        out_put = []
        ulbuf = [0] * 36
        ulbuf[0] = get_uint32_be(in_put[0:4])
        ulbuf[1] = get_uint32_be(in_put[4:8])
        ulbuf[2] = get_uint32_be(in_put[8:12])
        ulbuf[3] = get_uint32_be(in_put[12:16])
        for idx in range(32):
            ulbuf[idx + 4] = self._f(ulbuf[idx], ulbuf[idx + 1], ulbuf[idx + 2], ulbuf[idx + 3], sk[idx])

        out_put += put_uint32_be(ulbuf[35])
        out_put += put_uint32_be(ulbuf[34])
        out_put += put_uint32_be(ulbuf[33])
        out_put += put_uint32_be(ulbuf[32])
        return out_put

    def crypt_ecb(self, input_data, paddingFlag=1):
        # SM4-ECB block encryption/decryption
        input_data = bytes_to_list(input_data)
        if self.mode == SM4_ENCRYPT:
            if paddingFlag == 1:
                input_data = padding(input_data)
        length = len(input_data)
        i = 0
        output_data = []
        while length > 0:
            output_data += self.one_round(self.sk, input_data[i : i + 16])
            i += 16
            length -= 16
        if self.mode == SM4_DECRYPT:
            if paddingFlag == 1:
                output_data = unpadding(output_data)
                if output_data is None:
                    return None
                else:
                    return list_to_bytes(output_data)
        return list_to_bytes(output_data)

    def crypt_cbc(self, iv, input_data):
        # SM4-CBC buffer encryption/decryption
        i = 0
        output_data = []
        tmp_input = [0] * 16
        iv = bytes_to_list(iv)
        if self.mode == SM4_ENCRYPT:
            input_data = padding(bytes_to_list(input_data))
            length = len(input_data)
            while length > 0:
                tmp_input[0:16] = xor(input_data[i : i + 16], iv[0:16])
                output_data += self.one_round(self.sk, tmp_input[0:16])
                iv = copy.deepcopy(output_data[i : i + 16])
                i += 16
                length -= 16
            return list_to_bytes(output_data)
        else:
            length = len(input_data)
            while length > 0:
                output_data += self.one_round(self.sk, input_data[i : i + 16])
                output_data[i : i + 16] = xor(output_data[i : i + 16], iv[0:16])
                iv = copy.deepcopy(input_data[i : i + 16])
                i += 16
                length -= 16
            output_data = unpadding(output_data)
            if output_data is None:
                return None
            else:
                return list_to_bytes(output_data)

    def crypt_cfb(self, iv, input_data, cryptMode=SM4_ENCRYPT):
        times = 0
        i = 0
        output_data = bytes_to_list(b"")
        if cryptMode == SM4_ENCRYPT:
            input_data = padding(bytes_to_list(input_data))
            length = len(input_data)
            while length > 0:
                if times == 0:
                    tmpData = bytes_to_list(self.crypt_ecb(iv, 0))
                    times = 1
                else:
                    tmpData = bytes_to_list(self.crypt_ecb(output_data[i - 16 : i], 0))
                output_data += xor(tmpData, input_data[i : i + 16])
                i += 16
                length -= 16
            return list_to_bytes(output_data)
        else:
            self.mode = SM4_ENCRYPT
            length = len(input_data)
            while length > 0:
                if times == 0:
                    tmpData = bytes_to_list(self.crypt_ecb(iv, 0))
                    times = 1
                else:
                    tmpData = bytes_to_list(self.crypt_ecb(input_data[i - 16 : i], 0))
                output_data += xor(tmpData, input_data[i : i + 16])
                length -= 16
                i += 16
            self.mode = SM4_DECRYPT
            output_data = unpadding(output_data)
            if output_data is None:
                return None
            else:
                return list_to_bytes(output_data)

    def ofb_Encrypt(self, iv, M):
        global stream_key
        global outData
        M = bytes_to_list(M)
        M_value = padding(M)  # padding函数可以补齐
        # print('待加密补位后的明文的十进制列表', M_value)

        # 产生密钥流  流多少次把M加密完
        stream_num = int(len(M_value) / 16)

        stream_key.append(iv)
        for i in range(stream_num):
            temp = self.one_round(self.sk, stream_key[i])
            stream_key.append(temp)
            t = threading.Thread(
                target=XOR,
                args=[
                    M_value,
                    i,
                ],
            )
            t.start()
            t.join()
        output_data = list_to_bytes(outData)
        stream_key = []
        outData = []
        return output_data

    def ofb_Decrypt(self, iv, C):
        global stream_key
        global outData
        C_value = bytes_to_list(C)
        stream_num = int(len(C_value) / 16)
        iv = padding(bytes_to_list(iv))
        iv = iv[0:16]
        stream_key.append(iv)
        for i in range(stream_num):
            temp = self.one_round(self.sk, stream_key[i])
            stream_key.append(temp)
            t = threading.Thread(
                target=XOR,
                args=[
                    C_value,
                    i,
                ],
            )
            t.start()
            t.join()
        output_data = unpadding(outData)
        outData = []
        stream_key = []
        if output_data is None:
            return None
        else:
            return list_to_bytes(output_data)

    def crypt_ctr(self, input_data, flag=0):
        output_data = []
        if flag == 0:
            input_data = padding(bytes_to_list(input_data))
        times = (int)(len(input_data) / 16)
        counter = [0] * 16
        for i in range(0, times):
            temp = bytes_to_list(self.one_round(self.sk, counter))
            # 返回结果与input_data异或
            output_data += xor(temp, input_data[i * 16 : (i + 1) * 16])
            for j in range(0, 15):
                counter[15 - j] += 1
                if counter[15 - j] != 0:
                    break
        if flag == 0:
            return list_to_bytes(output_data)
        else:
            output_data = unpadding(output_data)
            if output_data is None:
                return None
            else:
                return list_to_bytes(output_data)
