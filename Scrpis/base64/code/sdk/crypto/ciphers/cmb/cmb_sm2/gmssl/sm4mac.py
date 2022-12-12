import copy

from .func import bytes_to_list, list_to_bytes, padding, unpadding, xor
from .sm4 import SM4_ENCRYPT, CryptSM4

paddingwith0 = lambda data, block=16: data + [0 for _ in range(16 - len(data) % block)]
paddingwith80 = lambda data, block=16: data + [0x80] + [0 for _ in range(15 - len(data) % block)]


class MacSM4(object):
    def __init__(self, key, cryptMode):
        self.sm4Key = CryptSM4()
        self.sm4Key.set_key(key, cryptMode)

    # paddingFlag  0:填充0x00 1：强制填充0x00 2:填充0x80 3: 强制填充0x80
    def genMac(self, input_data, paddingFlag):
        i = 0
        length = len(input_data)
        if paddingFlag == 0 or paddingFlag == 2:
            if length % 16 != 0:
                if paddingFlag == 0:
                    input_data = paddingwith0(bytes_to_list(input_data))
                elif paddingFlag == 2:
                    input_data = paddingwith80(bytes_to_list(input_data))
            else:
                input_data = bytes_to_list(input_data)
        else:
            if paddingFlag == 1:
                input_data = paddingwith0(bytes_to_list(input_data))
            elif paddingFlag == 3:
                input_data = paddingwith80(bytes_to_list(input_data))
        tmp_input = input_data[0:16]
        length = len(input_data)
        while length > 0:
            if i != 0:
                tmp_input = xor(output_data, input_data[i : i + 16])
            output_data = self.sm4Key.crypt_ecb(list_to_bytes(tmp_input), 0)
            i += 16
            length -= 16
        return list_to_bytes(output_data)

    def genPosmac(self, input_data, paddingFlag):
        length = len(input_data)
        num = (int)(length / 16)
        i = 0
        tmpData = [0] * 16
        if paddingFlag == 0 or paddingFlag == 2:
            if length % 16 != 0:
                if paddingFlag == 0:
                    data = paddingwith0(bytes_to_list(input_data))
                elif paddingFlag == 2:
                    data = paddingwith80(bytes_to_list(input_data))
            else:
                data = bytes_to_list(input_data)
        else:
            if paddingFlag == 1:
                data = paddingwith0(bytes_to_list(input_data))
            elif paddingFlag == 3:
                data = paddingwith80(bytes_to_list(input_data))
        length = len(data)
        while length > 0:
            tmpData = xor(tmpData, bytes_to_list(data[i : i + 16]))
            i += 16
            length -= 16
        macData = list_to_bytes(tmpData)
        buf = macData.hex().upper().encode("utf8")
        tmpData = bytes_to_list(buf)
        macData = self.sm4Key.crypt_ecb(list_to_bytes(tmpData[0:16]), 0)
        macData = xor(bytes_to_list(macData), tmpData[16:32])
        mac = self.sm4Key.crypt_ecb(macData, 0)
        return list_to_bytes(mac)
