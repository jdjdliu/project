from random import choice

xor = lambda a, b: list(map(lambda x, y: x ^ y, a, b))

rotl = lambda x, n: ((x << n) & 0xFFFFFFFF) | ((x >> (32 - n)) & 0xFFFFFFFF)

get_uint32_be = lambda key_data: ((key_data[0] << 24) | (key_data[1] << 16) | (key_data[2] << 8) | (key_data[3]))

put_uint32_be = lambda n: [((n >> 24) & 0xFF), ((n >> 16) & 0xFF), ((n >> 8) & 0xFF), ((n) & 0xFF)]

padding = lambda data, block=16: data + [(16 - len(data) % block) for _ in range(16 - len(data) % block)]

unpadding = lambda data: data[: -data[-1]] if (data[-1] <= 16 and data[-1] > 0) else None

list_to_bytes = lambda data: b"".join([bytes((i,)) for i in data])

bytes_to_list = lambda data: [i for i in data]

random_hex = lambda x: "".join([choice("0123456789abcdef") for _ in range(x)])


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
