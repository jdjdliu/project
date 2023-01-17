# 作者     ：tianfw
# 创建时间 ：2020/3/21
# IDE      : PyCharm
import datetime
import os

from .algorithm.digest.digest import Digest
from .algorithm.sm2.sm2_helper import bytes_to_int, getCurvePoint, getCurvePointByt, gett, key_pair_add, sm2_fromvkgetpk, sm2_key_pair_gen
from .algorithm.sm2.unionSm2 import SM2
from .gmssl import func, sm3, sm4, sm4mac
from .log.unionLog import CMBSMErrorLog, CMBSMLog, log_end, log_init
from .SMCryptException import SMCryptException
from .utils import com


def CMBSMGetVersion():
    CMBSMLog("---------CMBSMGetVersion start-------------")
    access_start = datetime.datetime.now()
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSMGetVersion" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSMGetVersion end-------------")
    return "KeYou-1.1-py3.X"


def CMBSMSetLog(dir):
    log_init(dir)


def CMBSMLogEnd():
    log_end()


def CMBSM2Encrypt(pubkey, msg):
    CMBSMLog("---------CMBSM2Encrypt start-------------")
    access_start = datetime.datetime.now()
    if pubkey is None or msg is None:
        CMBSMErrorLog(-10400, "CMBSM2Encrypt", "Illegal Argument: msg  or pubkey cannot be None.")
    if not isinstance(pubkey, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Encrypt", "Illegal Argument: pubkey must be bytes!")
    if not isinstance(msg, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Encrypt", "Illegal Argument: msg must be bytes!")
    if len(msg) == 0:
        CMBSMErrorLog(-10415, "CMBSM2Encrypt", "Illegal Argument: The size of msg too small.")
    if len(pubkey) != 65:
        CMBSMErrorLog(-10417, "CMBSM2Encrypt", "Illegal Argument: SM2 public key error, must be 65 bytes.")
    pk = pubkey.hex().upper()
    if pk[0:2] != "04":
        CMBSMErrorLog(-10403, "CMBSM2Encrypt", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")

    CMBSMLog("pubkey is:" + pk)
    sm2 = SM2(pk[2:], "")
    encData = sm2.encrypt(msg)
    CMBSMLog("encData is:" + encData)
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2Encrypt" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM2Encrypt end-------------")
    return com.aschex_to_bcdhex("04" + encData)


def CMBSM2Decrypt(privkey, msg):
    CMBSMLog("---------CMBSM2Decrypt start-------------")
    access_start = datetime.datetime.now()
    if privkey is None or msg is None:
        CMBSMErrorLog(-10400, "CMBSM2Decrypt", "Illegal Argument: cannot be None.")
    if not isinstance(privkey, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Decrypt", "Illegal Argument: privkey must be bytes!")
    if not isinstance(msg, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Decrypt", "Illegal Argument: msg must be bytes!")
    if len(msg) < 97:
        CMBSMErrorLog(-10419, "CMBSM2Decrypt", "Illegal Argument: SM2 cipher text error, must be more than 97 bytes.")
    if len(privkey) != 32:
        CMBSMErrorLog(-10418, "CMBSM2Decrypt", "Illegal Argument: SM2 private key error, must be 32 bytes.")
    vk = privkey.hex().upper()
    CMBSMLog("msg is:" + msg.hex().upper())
    sm2 = SM2("", vk)
    data = sm2.decrypt(msg)
    try:
        data.decode()
    except:
        CMBSMErrorLog(-10201, "CMBSM2Decrypt", " Failed to decrypt data using SM2 private key.")
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2Decrypt" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM2Decrypt end-------------")
    return data


# 密钥协商#
def CMBSM2Exchange_a1():
    CMBSMLog("----------CMBSM2Exchange_a1 start----------")
    access_start = datetime.datetime.now()
    private_key, public_key = sm2_key_pair_gen()
    dict = {"rA": b"", "RA": b""}
    dict["RA"] = com.aschex_to_bcdhex(public_key)
    dict["rA"] = com.aschex_to_bcdhex(private_key)
    if len(dict["rA"]) != 32 or len(dict["RA"]) != 64:
        CMBSMErrorLog(-10209, "CMBSM2Exchange_a1", "Key Agreement_A--The initiaor of the key exchange failed to initiate the key exchange.")

    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2Exchange_a1" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("----------CMBSM2Exchange_a1 end----------")
    return dict


def CMBSM2Exchange_b2(RA, pubkey_A, pubkey_B, privkey_B, ida, idb, K_len):
    CMBSMLog("----------CMBSM2Exchange_b2 start----------")
    access_start = datetime.datetime.now()
    if RA is None or pubkey_A is None or pubkey_B is None or privkey_B is None or ida is None or idb is None:
        CMBSMErrorLog(-10400, "CMBSM2Exchange_b2", "RA or pubkey_A or pubkey_B or privkey_B or ida or idb can not be none.")
    if not isinstance(RA, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: RA must be bytes!")
    if not isinstance(pubkey_A, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: pubkey_A must be bytes!")
    if not isinstance(pubkey_B, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: pubkey_B must be bytes!")
    if not isinstance(privkey_B, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: privkey_B must be bytes!")
    if not isinstance(ida, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: ida must be bytes!")
    if not isinstance(idb, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: idb must be bytes!")
    if not isinstance(K_len, int):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: K_len must be int!")

    if len(RA) != 64:
        CMBSMErrorLog(-10426, "CMBSM2Exchange_b2", "Illegal Argument: SM2Exchange RA error, must be 64 bytes.")
    if len(pubkey_A) != 65:
        CMBSMErrorLog(-10426, "CMBSM2Exchange_b2", "Illegal Argument: SM2 public key error, must be 65 bytes.")
    if pubkey_A[0] != 0x04:
        CMBSMErrorLog(-10403, "CMBSM2Exchange_b2", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")
    if len(privkey_B) != 32:
        CMBSMErrorLog(-10418, "CMBSM2Exchange_b2", "Illegal Argument: SM2 private key error, must be 32 bytes.")
    if len(pubkey_B) != 65:
        CMBSMErrorLog(-10417, "CMBSM2Exchange_b2", "SM2Exchange pubkey_B is not 65 byte.")
    if pubkey_B[0] != 0x04:
        CMBSMErrorLog(-10403, "CMBSM2Exchange_b2", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")
    if len(ida) <= 0:
        CMBSMErrorLog(-10425, "CMBSM2Exchange_b2", "the length of ida is less than 0 byte.")
    if len(idb) <= 0:
        CMBSMErrorLog(-10425, "CMBSM2Exchange_b2", "the length of idb is less than 0 byte.")
    if K_len <= 0:
        CMBSMErrorLog(-10425, "CMBSM2Exchange_b2", "the length of K_len is less than 0 byte.")

    dict = {"K": b"", "RB": b"", "RV": b"", "SB": b""}
    # 先生成B的随机数RB
    dictB = CMBSM2Exchange_a1()
    dict["RB"] = dictB["RA"]
    xA = pubkey_A[1:33]
    yA = pubkey_A[33:]
    xB = pubkey_B[1:33]
    yB = pubkey_B[33:]
    x1 = RA[0:32]
    y1 = RA[32:]
    x2 = dict["RB"][0:32]
    y2 = dict["RB"][32:]
    # x2 = com.aschex_to_bcdhex("ACC27688A6F7B706098BC91FF3AD1BFF7DC2802CDB14CCCCDB0A90471F9BD707")
    # y2 = com.aschex_to_bcdhex("2FEDAC0494B2FFC4D6853876C79B8F301C6573AD0AA50F39FC87181E1A1B46FE")

    rB = dictB["rA"]
    # rB = com.aschex_to_bcdhex("7E07124814B309489125EAED101113164EBF0F3458C5BD88335C1F9D596243D6")

    # 生成tB
    tB = gett(privkey_B, x2, rB)
    # print("tB:" + tB.hex().upper())

    # 计算椭圆曲线点[_x1]RA
    xA0, yA0 = getCurvePoint(x1, y1)
    # print("xA0:"+xA0.hex().upper())
    # print("yA0:" + yA0.hex().upper())

    # 计算两个椭圆曲线点相加  PA+椭圆曲线点[_x1]RA
    xA1, yA1 = key_pair_add(xA, yA, xA0, yA0)
    # print("xA1:" + xA0.hex().upper())
    # print("yA1:" + yA0.hex().upper())
    # 计算RV
    dict["RV"] = getCurvePointByt(bytes_to_int(tB), xA1, yA1)
    # print("RV:" + dict['RV'].hex().upper())

    # 计算ZA和ZB
    digest = Digest()
    # print(pubkey_A.hex().upper())
    ZA = com.aschex_to_bcdhex(digest.union_sm3_ewithoutData(pubkey_A[1:], ida))
    ZB = com.aschex_to_bcdhex(digest.union_sm3_ewithoutData(pubkey_B[1:], idb))
    # print("ZA: "+ZA.hex().upper())
    # print("ZB: "+ZB.hex().upper())
    # 计算共享密钥 xv||yv||ZA||ZB
    k = dict["RV"] + ZA + ZB
    KB = sm3.sm3_kdf(k.hex().upper().encode("utf-8"), K_len)
    # print("KB: "+KB[0:32].upper())
    dict["K"] = com.aschex_to_bcdhex(KB.upper())

    # 计算SB   xv||ZA||ZB||x1||y1||x2||y2
    hash = dict["RV"][0:32] + ZA + ZB + x1 + y1 + x2 + y2
    e = sm3.sm3_hash(func.bytes_to_list(hash))
    hash = com.aschex_to_bcdhex("02") + dict["RV"][32:] + com.aschex_to_bcdhex(e.upper())
    e = sm3.sm3_hash(func.bytes_to_list(hash))
    # print('SB: ' +e.upper())
    dict["SB"] = com.aschex_to_bcdhex(e.upper())

    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2Exchange_b2" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("----------CMBSM2Exchange_b2 end----------")
    return dict


def CMBSM2Exchange_a3(RA, rA, RB, pubkey_A, privkey_A, pubkey_B, ida, idb, SB, K_len):
    CMBSMLog("----------CMBSM2Exchange_a3 start----------")
    access_start = datetime.datetime.now()
    if (
        RA is None
        or rA is None
        or RB is None
        or pubkey_A is None
        or pubkey_B is None
        or privkey_A is None
        or ida is None
        or idb is None
        or SB is None
    ):
        CMBSMErrorLog(-10400, "CMBSM2Exchange_a3", "RA or rA or RB or pubkey_A or pubkey_B or privkey_A or ida or idb or SB can not be none.")

    if not isinstance(RA, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: RA must be bytes!")
    if not isinstance(rA, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: rA must be bytes!")
    if not isinstance(RB, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: RB must be bytes!")
    if not isinstance(pubkey_A, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: pubkey_A must be bytes!")
    if not isinstance(privkey_A, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: privkey_A must be bytes!")
    if not isinstance(pubkey_B, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: pubkey_B must be bytes!")
    if not isinstance(ida, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: ida must be bytes!")
    if not isinstance(idb, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: idb must be bytes!")
    if not isinstance(SB, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: SB must be bytes!")
    if not isinstance(K_len, int):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: K_len must be int!")

    if len(RA) != 64:
        CMBSMErrorLog(-10426, "CMBSM2Exchange_a3", "Illegal Argument: SM2Exchange RA error, must be 64 bytes.")
    if len(rA) != 32:
        CMBSMErrorLog(-10428, "CMBSM2Exchange_a3", "Illegal Argument:the length of rA must be 32 byte.")
    if len(RB) != 64:
        CMBSMErrorLog(-10427, "CMBSM2Exchange_a3", "Illegal Argument: SM2Exchange RB error, must be 64 bytes.")
    if len(pubkey_A) != 65:
        CMBSMErrorLog(-10417, "CMBSM2Exchange_a3", "the length of SM2Exchange pubkey_A is not 65 byte")
    if pubkey_A[0] != 0x04:
        CMBSMErrorLog(-10403, "CMBSM2Exchange_a3", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")
    if len(pubkey_B) != 65:
        CMBSMErrorLog(-10417, "CMBSM2Exchange_a3", "the length of SM2Exchange pubkey_B is not 65 byte")
    if pubkey_B[0] != 0x04:
        CMBSMErrorLog(-10403, "CMBSM2Exchange_a3", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")
    if len(privkey_A) != 32:
        CMBSMErrorLog(-10418, "CMBSM2Exchange_a3", "the length of SM2Exchange privkey_A is not 32 byte")
    if len(ida) <= 0:
        CMBSMErrorLog(-10425, "CMBSM2Exchange_a3", "the length of ida is less than 0 byte")
    if len(idb) <= 0:
        CMBSMErrorLog(-10425, "CMBSM2Exchange_a3", "the length of idb is less than 0 byte")
    if K_len <= 0:
        CMBSMErrorLog(-10425, "CMBSM2Exchange_a3", "the length of K_len is less than 0 byte")
    if len(SB) != 32:
        CMBSMErrorLog(-10430, "CMBSM2Exchange_a3", "Illegal Argument: SM2Exchange SB error, must be 32 bytes.")

    dict = {"K": b"", "SA": b""}
    x1 = RA[0:32]
    y1 = RA[32:]
    x2 = RB[0:32]
    y2 = RB[32:]
    xB = pubkey_B[1:33]
    yB = pubkey_B[33:]

    # 生成tA
    tA = gett(privkey_A, x1, rA)
    # print("tA:" + tA.hex().upper())
    # 计算椭圆曲线点 [_x2]RB
    xB0, yB0 = getCurvePoint(x2, y2)
    # print("xB0: "+xB0.hex().upper())
    # print("yB0: " + yB0.hex().upper())

    # 计算两个椭圆曲线点相加  PB+椭圆曲线点[_x2]RB
    xB1, yB1 = key_pair_add(xB, yB, xB0, yB0)
    # print("xB1:" + xB1.hex().upper())
    # print("yB1:" + yB1.hex().upper())

    # 计算RU
    RU = getCurvePointByt(bytes_to_int(tA), xB1, yB1)
    # print("RU:" + RU.hex().upper())

    # 计算ZA和ZB
    digest = Digest()
    # print(pubkey_A.hex().upper())
    ZA = com.aschex_to_bcdhex(digest.union_sm3_ewithoutData(pubkey_A[1:], ida))
    ZB = com.aschex_to_bcdhex(digest.union_sm3_ewithoutData(pubkey_B[1:], idb))
    # print("ZA: " + ZA.hex().upper())
    # print("ZB: " + ZB.hex().upper())

    # 计算共享密钥 xu||yu||ZA||ZB:
    k = RU + ZA + ZB
    KB = sm3.sm3_kdf(k.hex().upper().encode("utf-8"), K_len)
    # print("KB: " + KB[0:32].upper())
    dict["K"] = com.aschex_to_bcdhex(KB.upper())

    # 计算S1   0x02||yu||hash（xu||ZA||ZB||x1||y1||x2||y2）
    hash = RU[0:32] + ZA + ZB + x1 + y1 + x2 + y2
    e1 = sm3.sm3_hash(func.bytes_to_list(hash))
    hash = com.aschex_to_bcdhex("02") + RU[32:] + com.aschex_to_bcdhex(e1.upper())
    e = sm3.sm3_hash(func.bytes_to_list(hash))
    # print('S1: ' + e.upper())
    S1 = com.aschex_to_bcdhex(e.upper())
    if S1 != SB:
        CMBSMErrorLog(-10212, "CMBSM2Exchange_a3", "Key Agreement_A--HASH-SB is not equal to the HASH-s1,verify the key agreement is not correct.")

    # 计算SA  0x03||yv||hash(xu||ZA||ZB||x1||y1||x2||y2):
    hash = com.aschex_to_bcdhex("03") + RU[32:] + com.aschex_to_bcdhex(e1.upper())
    SA = sm3.sm3_hash(func.bytes_to_list(hash))
    # print("SA: "+SA.upper())
    dict["SA"] = com.aschex_to_bcdhex(SA)

    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2Exchange_a3" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("----------CMBSM2Exchange_a3 end----------")
    return dict


def CMBSM2Exchange_b4(RA, RB, pubkey_A, pubkey_B, RV, ida, idb, SA):
    CMBSMLog("----------CMBSM2Exchange_b4 start----------")
    access_start = datetime.datetime.now()
    if RA is None or RB is None or pubkey_A is None or pubkey_B is None or RV is None or ida is None or idb is None or SA is None:
        CMBSMErrorLog(-10400, "CMBSM2Exchange_b4", "RA or RB or pubkey_A or pubkey_B or RV or ida or idb or SA can not be none.")
    if not isinstance(RA, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: RA must be bytes!")
    if not isinstance(RB, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: RB must be bytes!")
    if not isinstance(pubkey_A, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: pubkey_A must be bytes!")
    if not isinstance(pubkey_B, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: pubkey_B must be bytes!")
    if not isinstance(RV, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: RV must be bytes!")
    if not isinstance(ida, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: ida must be bytes!")
    if not isinstance(idb, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: idb must be bytes!")
    if not isinstance(SA, bytes):
        CMBSMErrorLog(-10001, "CMBSM2Exchange_b2", "Illegal Argument: SA must be bytes!")

    if len(RA) != 64:
        CMBSMErrorLog(-10426, "CMBSM2Exchange_b4", "Illegal Argument: SM2Exchange RA error, must be 64 bytes.")
    if len(RB) != 64:
        CMBSMErrorLog(-10427, "CMBSM2Exchange_b4", "Illegal Argument: SM2Exchange RB error, must be 64 bytes.")
    if len(pubkey_A) != 65:
        CMBSMErrorLog(-10417, "CMBSM2Exchange_b4", "SM2Exchange pubkey_A is not 65 byte.")
    if pubkey_A[0] != 0x04:
        CMBSMErrorLog(-10403, "CMBSM2Exchange_b4", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")
    if len(pubkey_B) != 65:
        CMBSMErrorLog(-10417, "CMBSM2Exchange_b4", "SM2Exchange pubkey_B is not 65 byte")
    if pubkey_B[0] != 0x04:
        CMBSMErrorLog(-10403, "CMBSM2Exchange_b4", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")
    if len(RV) != 64:
        CMBSMErrorLog(-10431, "CMBSM2Exchange_b4", "Illegal Argument: SM2Exchange RV error, must be 64 bytes.")
    if len(SA) != 32:
        CMBSMErrorLog(-10429, "CMBSM2Exchange_b4", "Illegal Argument: SM2Exchange SA error, must be 32 bytes.")
    if len(ida) <= 0:
        CMBSMErrorLog(-10425, "CMBSM2Exchange_b4", "the length of ida is less than 0 byte")
    if len(idb) <= 0:
        CMBSMErrorLog(-10425, "CMBSM2Exchange_b4", "the length of idb is less than 0 byte")

    # 计算ZA和ZB
    digest = Digest()
    # print(pubkey_A.hex().upper())
    ZA = com.aschex_to_bcdhex(digest.union_sm3_ewithoutData(pubkey_A[1:], ida))
    ZB = com.aschex_to_bcdhex(digest.union_sm3_ewithoutData(pubkey_B[1:], idb))
    # print("ZA: " + ZA.hex().upper())
    # print("ZB: " + ZB.hex().upper())

    # Hash(xv||ZA||ZB||x1||y1||x2||y2)
    hash = RV[0:32] + ZA + ZB + RA + RB
    e = sm3.sm3_hash(func.bytes_to_list(hash))
    # print("hash :" + e.upper())
    # 计算S2
    hash = com.aschex_to_bcdhex("03") + RV[32:] + com.aschex_to_bcdhex(e.upper())
    S2 = sm3.sm3_hash(func.bytes_to_list(hash))
    # print("S2 :" + S2.upper())

    if S2 != SA.hex():
        CMBSMErrorLog(-10213, "CMBSM2Exchange_b4", "Key Agreement_B--The receiver failed to verify the correctness of the key exchange.")

    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2Exchange_b4" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("----------CMBSM2Exchange_b4 end----------")


def CMBSM2SignWithSM3(privkey, msg, userid="1234567812345678"):
    CMBSMLog("---------CMBSM2SignWithSM3 start-------------")
    access_start = datetime.datetime.now()
    if privkey is None or msg is None:
        CMBSMErrorLog(-10400, "CMBSM2SignWithSM3", "Illegal Argument: pubkey or privkey or msg cannot be None!")
    if not isinstance(privkey, bytes):
        CMBSMErrorLog(-10001, "CMBSM2SignWithSM3", "Illegal Argument: privkey must be bytes!")
    if not isinstance(msg, bytes):
        CMBSMErrorLog(-10001, "CMBSM2SignWithSM3", "Illegal Argument: msg must be bytes!")

    if len(msg) == 0:
        CMBSMErrorLog(-10415, "CMBSM2SignWithSM3", "Illegal Argument: The size of msg too small.")

    if len(privkey) != 32:
        CMBSMErrorLog(-10418, "CMBSM2SignWithSM3", "Illegal Argument: SM2 private key error, must be 32 bytes.")

    if userid is None or len(userid) == 0:
        userid = "1234567812345678"

    pk = "04" + sm2_fromvkgetpk(privkey)
    # print(pk.upper())
    vk = privkey.hex().upper()
    data = msg.hex().upper()
    CMBSMLog("pubkey is:" + pk.upper())
    CMBSMLog("userid is:" + userid)

    # 先计算sm3withsm2
    digest = Digest()
    hash = digest.union_sm3_e(data, pk[2:], userid)
    CMBSMLog("sm3withpk is:" + hash)
    sm2 = SM2(pk[2:], vk)
    sign = sm2.sign(com.aschex_to_bcdhex(hash))
    CMBSMLog("sign is:" + sign)
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2Decrypt" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM2SignWithSM3 end-------------")
    return com.aschex_to_bcdhex(sign)


def CMBSM2VerifyWithSM3(pubkey, msg, signature, userid="1234567812345678"):
    CMBSMLog("---------CMBSM2VerifyWithSM3 start-------------")
    access_start = datetime.datetime.now()
    if pubkey is None or msg is None or signature is None:
        CMBSMErrorLog(-10400, "CMBSM2VerifyWithSM3", "Illegal Argument: pubkey or privkey or msg or signature can not be None!")
    if not isinstance(pubkey, bytes):
        CMBSMErrorLog(-10001, "CMBSM2VerifyWithSM3", "Illegal Argument: pubkey must be bytes!")
    if not isinstance(msg, bytes):
        CMBSMErrorLog(-10001, "CMBSM2VerifyWithSM3", "Illegal Argument: msg must be bytes!")
    if not isinstance(signature, bytes):
        CMBSMErrorLog(-10001, "CMBSM2VerifyWithSM3", "Illegal Argument: signature must be bytes!")

    if len(msg) == 0:
        CMBSMErrorLog(-10415, "CMBSM2VerifyWithSM3", "Illegal Argument: The size of msg too small.")
    if len(signature) == 0:
        CMBSMErrorLog(-10405, "CMBSM2VerifyWithSM3", "Illegal Argument: The size of signature too small.")
    if len(pubkey) != 65:
        CMBSMErrorLog(-10417, "CMBSM2VerifyWithSM3", "Illegal Argument: SM2 public key error, must be 65 bytes.")
    if len(signature) != 64:
        CMBSMErrorLog(-10001, "CMBSM2VerifyWithSM3", "Illegal Argument: signature length error,must be 64!")

    if userid is None or len(userid) == 0:
        userid = "1234567812345678"
    pk = pubkey.hex().upper()
    if pk[0:2] != "04":
        CMBSMErrorLog(-10403, "CMBSM2VerifyWithSM3", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")

    data = msg.hex().upper()
    sign = signature.hex().upper()
    CMBSMLog("pubkey is:" + pk)
    CMBSMLog("userid is:" + userid)
    # 先计算sm3withsm2
    digest = Digest()
    hash = digest.union_sm3_e(data, pk[2:], userid)
    CMBSMLog("sm3withsm2 is:" + hash)
    CMBSMLog("signature is:" + sign)
    sm2 = SM2(pk[2:], "")
    ret = sm2.verify(com.aschex_to_bcdhex(hash), sign)
    if ret:
        CMBSMLog("verify success!")
    else:
        CMBSMLog("verify failed!")
        CMBSMErrorLog(
            -10203, "CMBSM2VerifyWithSM3", "Failed to verify data using SM2 public key. case by : It's just a failure to check, not an exception."
        )
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000

    CMBSMLog("CMBSM2VerifyWithSM3" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM2VerifyWithSM3 end-------------")
    if ret:
        return 0
    else:
        return -1


def CMBSM2SignWithSM3ASN1(privkey, msg, userid="1234567812345678") -> bytes:
    CMBSMLog("---------CMBSM2SignWithSM3 start-------------")
    access_start = datetime.datetime.now()
    if privkey is None or msg is None:
        CMBSMErrorLog(-10400, "CMBSM2SignWithSM3", "Illegal Argument: pubkey or privkey or msg cannot be None!")
    if not isinstance(privkey, bytes):
        CMBSMErrorLog(-10001, "CMBSM2SignWithSM3", "Illegal Argument: privkey must be bytes!")
    if not isinstance(msg, bytes):
        CMBSMErrorLog(-10001, "CMBSM2SignWithSM3", "Illegal Argument: msg must be bytes!")

    if len(msg) == 0:
        CMBSMErrorLog(-10415, "CMBSM2SignWithSM3", "Illegal Argument: The size of msg too small.")

    if len(privkey) != 32:
        CMBSMErrorLog(-10418, "CMBSM2SignWithSM3", "Illegal Argument: SM2 private key error, must be 32 bytes.")

    if userid is None or len(userid) == 0:
        userid = "1234567812345678"

    pk = "04" + sm2_fromvkgetpk(privkey)
    vk = privkey.hex().upper()
    data = msg.hex().upper()
    CMBSMLog("pubkey is:" + pk.upper())
    CMBSMLog("userid is:" + userid)

    # 先计算sm3withsm2
    digest = Digest()
    hash = digest.union_sm3_e(data, pk[2:], userid)
    CMBSMLog("sm3withpk is:" + hash)
    sm2 = SM2(pk[2:], vk)
    sign = sm2.sign(com.aschex_to_bcdhex(hash))
    CMBSMLog("sign is:" + sign)
    # 将sign进行der编码
    dersign = com.UnionRSToSM2SigDer(com.aschex_to_bcdhex(sign))
    CMBSMLog("der sign is:" + dersign.hex().upper())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2Decrypt" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM2SignWithSM3 end-------------")
    return dersign


def CMBSM2VerifyWithSM3ASN1(pubkey, msg, signature, userid="1234567812345678"):
    CMBSMLog("---------CMBSM2VerifyWithSM3 start-------------")
    access_start = datetime.datetime.now()
    if pubkey is None or msg is None or signature is None:
        CMBSMErrorLog(-10400, "CMBSM2VerifyWithSM3", "Illegal Argument: pubkey or privkey or msg or signature can not be None!")
    if not isinstance(pubkey, bytes):
        CMBSMErrorLog(-10001, "CMBSM2VerifyWithSM3", "Illegal Argument: pubkey must be bytes!")
    if not isinstance(msg, bytes):
        CMBSMErrorLog(-10001, "CMBSM2VerifyWithSM3", "Illegal Argument: msg must be bytes!")
    if not isinstance(signature, bytes):
        CMBSMErrorLog(-10001, "CMBSM2VerifyWithSM3", "Illegal Argument: signature must be bytes!")

    if len(msg) == 0:
        CMBSMErrorLog(-10415, "CMBSM2VerifyWithSM3", "Illegal Argument: The size of msg too small.")
    if len(signature) == 0:
        CMBSMErrorLog(-10405, "CMBSM2VerifyWithSM3", "Illegal Argument: The size of signature too small.")
    if len(pubkey) != 65:
        CMBSMErrorLog(-10417, "CMBSM2VerifyWithSM3", "Illegal Argument: SM2 public key error, must be 65 bytes.")

    if len(signature) != 64:
        CMBSMLog("der signature is:" + signature.hex().upper())
        tmpSign = com.UnionGetRSFromSM2SigDer(signature)
        if tmpSign == None:
            CMBSMErrorLog(-10501, "CMBSM2VerifyWithSM3", "DER encoded data encoding or decoding error.")
    else:
        tmpSign = signature

    if userid is None or len(userid) == 0:
        userid = "1234567812345678"
    pk = pubkey.hex().upper()
    if pk[0:2] != "04":
        CMBSMErrorLog(-10403, "CMBSM2VerifyWithSM3", "Illegal Argument: SM2 public key error, must be 65 bytes and in the format 04||X||Y.")

    data = msg.hex().upper()
    sign = tmpSign.hex().upper()
    CMBSMLog("pubkey is:" + pk)
    CMBSMLog("userid is:" + userid)
    # 先计算sm3withsm2
    digest = Digest()
    hash = digest.union_sm3_e(data, pk[2:], userid)
    CMBSMLog("sm3withsm2 is:" + hash)
    CMBSMLog("signature is:" + sign)
    sm2 = SM2(pk[2:], "")
    ret = sm2.verify(com.aschex_to_bcdhex(hash), sign)
    if ret:
        CMBSMLog("verify success!")
    else:
        CMBSMLog("verify failed!")
        CMBSMErrorLog(
            -10203, "CMBSM2VerifyWithSM3", "Failed to verify data using SM2 public key. case by : It's just a failure to check, not an exception."
        )
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000

    CMBSMLog("CMBSM2VerifyWithSM3" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM2VerifyWithSM3 end-------------")
    if ret:
        return 0
    else:
        return -1


# 产生密钥对 #
def CMBSM2KeyGen():
    CMBSMLog("----------CMBSM2KeyGen start----------")
    access_start = datetime.datetime.now()
    private_key, public_key = sm2_key_pair_gen()
    dict = {"publickey": b"", "privatekey": b""}
    dict["publickey"] = com.aschex_to_bcdhex("04" + public_key)
    dict["privatekey"] = com.aschex_to_bcdhex(private_key)
    CMBSMLog("publickey is: " + dict["publickey"].hex().upper())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM2KeyGen" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("----------CMBSM2KeyGen end----------")
    return dict


def CMBSM3Digest(data):
    CMBSMLog("----------CMBSM3Digest start----------")
    access_start = datetime.datetime.now()
    if data is None:
        CMBSMErrorLog(-10400, "CMBSM3Digest", "Illegal Argument: cannot be None.")
    if not isinstance(data, bytes):
        CMBSMErrorLog(-10001, "CMBSM3Digest", "Illegal Argument: data must be bytes!")
    if len(data) == 0:
        CMBSMErrorLog(-10415, "CMBSM3Digest", "Illegal Argument: The size of msg too small.")
    e = sm3.sm3_hash(func.bytes_to_list(data))
    CMBSMLog("hash is: " + e.upper())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM3Digest" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("----------CMBSM3Digest end----------")
    return com.aschex_to_bcdhex(e.upper())


def CMBSM3FileDigest(file):
    CMBSMLog("----------CMBSM3FileDigest start----------")
    access_start = datetime.datetime.now()
    if file is None:
        CMBSMErrorLog(-10400, "CMBSM3FileDigest", "Illegal Argument: file cannot be None.")
    if not isinstance(file, str):
        CMBSMErrorLog(-10001, "CMBSM3FileDigest", "Illegal Argument: file must be str!")
    if not os.path.exists(file):
        CMBSMErrorLog(-10109, "CMBSM3FileDigest", "Not Found file." + file)
    if len(file) > 260:
        CMBSMErrorLog(-10108, "CMBSM3FileDigest", "Excepted file length.Path: " + file)
    if not os.path.isfile(file):
        CMBSMErrorLog(-10109, "CMBSM3FileDigest", "Excepted file,actual directory. Path: " + file)
    if not os.access(file, os.R_OK):
        CMBSMErrorLog(-10104, "CMBSM3FileDigest", "Path: " + file)

    with open(file, "rb") as f:
        data = f.read()

    e = sm3.sm3_hash(func.bytes_to_list(data))
    CMBSMLog("hash is: " + e.upper())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM3FileDigest" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("----------CMBSM3FileDigest end----------")
    return com.aschex_to_bcdhex(e.upper())


# key : bytes
# in_msg: bytes
def CMBSM3HMAC(key, in_msg):
    CMBSMLog("----------CMBSM3HMAC start----------")
    access_start = datetime.datetime.now()
    if key is None or in_msg is None:
        CMBSMErrorLog(-10400, "CMBSM3HMAC", "Illegal Argument: key or in_msg cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM3HMAC", "Illegal Argument: key must be bytes!")

    if not isinstance(in_msg, bytes):
        CMBSMErrorLog(-10001, "CMBSM3HMAC", "Illegal Argument: in_msg must be bytes!")
    if len(in_msg) == 0:
        CMBSMErrorLog(-10415, "CMBSM3HMAC", "Illegal Argument: The size of msg too small.")

    if len(key) != 64:
        CMBSMErrorLog(-10423, "CMBSM3HMAC", " Illegal Argument: SM3_HMAC key error, must be 64 bytes")

    ipad = [0x36 for i in range(64)]
    opad = [0x5C for i in range(64)]

    # 如果key的长度大于64，则对key做sm3摘要
    #    if len(key) > 64:
    #        key = bytes.fromhex(sm3.sm3_hash(func.bytes_to_list(key)))

    for i in range(len(key)):
        ipad[i] = ipad[i] ^ key[i]
        opad[i] = opad[i] ^ key[i]
    h1 = sm3.sm3_hash(list(bytes(ipad) + in_msg))
    hash = sm3.sm3_hash(opad + list(bytes.fromhex(h1))).upper()
    CMBSMLog("HMAC is: " + hash)
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM3HMAC" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("----------CMBSM3HMAC end----------")
    return com.aschex_to_bcdhex(hash)


def CMBSM4EncryptWithECB(key, input):
    CMBSMLog("---------CMBSM4EncryptWithECB start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4EncryptWithECB", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithECB", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithECB", "Illegal Argument: input must be bytes!")
    if len(input) == 0:
        CMBSMErrorLog(-10408, "CMBSM4EncryptWithECB", "Illegal Argument: The plaintext data length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4EncryptWithECB", "Illegal Argument: SM4 secret key error, must be 16 bytes.")

    # 1. ECB
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_ENCRYPT)
    encData = sm4_d.crypt_ecb(input)
    CMBSMLog("encData is:" + encData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4EncryptWithECB" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4EncryptWithECB end-------------")
    return encData


def CMBSM4DecryptWithECB(key, input):
    CMBSMLog("---------CMBSM4DecryptWithECB start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4DecryptWithECB", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithECB", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithECB", "Illegal Argument: input must be bytes!")
    if len(input) == 0 or len(input) % 16 != 0:
        CMBSMErrorLog(-10409, "CMBSM4DecryptWithECB", "Illegal Argument: The cipher text length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4DecryptWithECB", "Illegal Argument: SM4 secret key error, must be 16 bytes.")

    # 1. ECB
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_DECRYPT)
    CMBSMLog("input is:" + input.hex())
    plainData = sm4_d.crypt_ecb(input)
    if plainData is None:
        CMBSMErrorLog(-10205, "CMBSM4DecryptWithECB", "Illegal Argument: unPack plainData PKCS#7 error.")
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4DecryptWithECB" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4DecryptWithECB end-------------")
    return plainData


def CMBSM4EncryptWithCBC(key, iv, input):
    CMBSMLog("---------CMBSM4EncryptWithCBC start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4EncryptWithCBC", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithCBC", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithCBC", "Illegal Argument: input must be bytes!")
    if len(input) == 0:
        CMBSMErrorLog(-10408, "CMBSM4EncryptWithCBC", "Illegal Argument: The plaintext data length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4EncryptWithCBC", "Illegal Argument: SM4 secret key error, must be 16 bytes.")
    if iv == None or len(iv) != 16:
        CMBSMErrorLog(-10411, "CMBSM4EncryptWithCBC", "Illegal Argument: The size of IV error, must be 16 bytes.")
    if not isinstance(iv, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithCBC", "Illegal Argument:  iv must be bytes!")
    # 1. CBC
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_ENCRYPT)
    encData = sm4_d.crypt_cbc(iv, input)
    CMBSMLog("encData is:" + encData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4EncryptWithCBC" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4EncryptWithCBC end-------------")
    return encData


def CMBSM4DecryptWithCBC(key, iv, input):
    CMBSMLog("---------CMBSM4DecryptWithCBC start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4DecryptWithCBC", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithCBC", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithCBC", "Illegal Argument: input must be bytes!")
    if len(input) == 0 or len(input) % 16 != 0:
        CMBSMErrorLog(-10409, "CMBSM4DecryptWithCBC", "Illegal Argument: The cipher text length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4DecryptWithCBC", "Illegal Argument: SM4 secret key error, must be 16 bytes.")
    if iv == None or len(iv) != 16:
        CMBSMErrorLog(-10411, "CMBSM4DecryptWithCBC", "Illegal Argument: The size of IV error, must be 16 bytes.")
    # 1. CBC
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_DECRYPT)
    CMBSMLog("input is:" + input.hex())
    CMBSMLog("iv is:" + iv.hex())
    plainData = sm4_d.crypt_cbc(iv, input)
    if plainData is None:
        CMBSMErrorLog(-10205, "CMBSM4DecryptWithCBC", "Illegal Argument: unPack plainData PKCS#7 error.")
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4DecryptWithCBC" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4DecryptWithCBC end-------------")
    return plainData


def CMBSM4EncryptWithCFB(key, iv, input):
    CMBSMLog("---------CMBSM4EncryptWithCFB start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4EncryptWithCFB", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithCFB", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithCFB", "Illegal Argument: input must be bytes!")

    if len(input) == 0:
        CMBSMErrorLog(-10408, "CMBSM4EncryptWithCFB", "Illegal Argument: The plaintext data length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4EncryptWithCFB", "Illegal Argument: SM4 secret key error, must be 16 bytes.")
    if iv == None or len(iv) != 16:
        CMBSMErrorLog(-10411, "CMBSM4EncryptWithCFB", "Illegal Argument: The size of IV error, must be 16 bytes.")
    if not isinstance(iv, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithCFB", "Illegal Argument:  iv must be bytes!")
    # 1. CFB
    CMBSMLog("iv is:" + iv.hex())
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_ENCRYPT)
    encData = sm4_d.crypt_cfb(iv, input)
    CMBSMLog("encData is:" + encData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4EncryptWithCFB" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4EncryptWithCFB end-------------")
    return encData


def CMBSM4DecryptWithCFB(key, iv, input):
    CMBSMLog("---------CMBSM4DecryptWithCFB start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4DecryptWithCFB", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithCFB", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithCFB", "Illegal Argument: input must be bytes!")
    if len(input) == 0 or len(input) % 16 != 0:
        CMBSMErrorLog(-10409, "CMBSM4DecryptWithCFB", "Illegal Argument: The cipher text length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4DecryptWithCFB", "Illegal Argument: SM4 secret key error, must be 16 bytes.")
    if iv == None or len(iv) != 16:
        CMBSMErrorLog(-10411, "CMBSM4DecryptWithCFB", "Illegal Argument: The size of IV error, must be 16 bytes.")
    # 1. CFB
    CMBSMLog("input is:" + input.hex())
    CMBSMLog("iv is:" + iv.hex())
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_ENCRYPT)
    plainData = sm4_d.crypt_cfb(iv, input, sm4.SM4_DECRYPT)
    if plainData is None:
        CMBSMErrorLog(-10205, "CMBSM4DecryptWithCFB", "Illegal Argument: unPack plainData PKCS#7 error.")
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4DecryptWithCFB" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4DecryptWithCFB end-------------")
    return plainData


def CMBSM4EncryptWithOFB(key, iv, input):
    CMBSMLog("---------CMBSM4EncryptWithOFB start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4EncryptWithOFB", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithOFB", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithOFB", "Illegal Argument: input must be bytes!")

    if len(input) == 0:
        CMBSMErrorLog(-10408, "CMBSM4EncryptWithOFB", "Illegal Argument: The plaintext data length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4EncryptWithOFB", "Illegal Argument: SM4 secret key error, must be 16 bytes.")
    if iv == None or len(iv) != 16:
        CMBSMErrorLog(-10411, "CMBSM4EncryptWithOFB", "Illegal Argument: The size of IV error, must be 16 bytes.")
    if not isinstance(iv, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithOFB", "Illegal Argument:  iv must be bytes!")

    # 1. OFB
    CMBSMLog("iv is:" + iv.hex())
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_ENCRYPT)
    encData = sm4_d.ofb_Encrypt(iv, input)
    CMBSMLog("encData is:" + encData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4EncryptWithOFB" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4EncryptWithOFB end-------------")
    return encData


def CMBSM4DecryptWithOFB(key, iv, input):
    CMBSMLog("---------CMBSM4DecryptWithOFB start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4DecryptWithOFB", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithOFB", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithOFB", "Illegal Argument: input must be bytes!")
    if len(input) == 0 or len(input) % 16 != 0:
        CMBSMErrorLog(-10409, "CMBSM4DecryptWithOFB", "Illegal Argument: The cipher text length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4DecryptWithOFB", "Illegal Argument: SM4 secret key error, must be 16 bytes.")
    if iv == None or len(iv) != 16:
        CMBSMErrorLog(-10411, "CMBSM4DecryptWithOFB", "Illegal Argument: The size of IV error, must be 16 bytes.")
    # 1. CFB
    CMBSMLog("input is:" + input.hex())
    CMBSMLog("iv is:" + iv.hex())
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_ENCRYPT)
    plainData = sm4_d.ofb_Decrypt(iv, input)
    if plainData is None:
        CMBSMErrorLog(-10205, "CMBSM4DecryptWithOFB", "Illegal Argument: unPack plainData PKCS#7 error.")
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4DecryptWithOFB" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4DecryptWithOFB end-------------")
    return plainData


def CMBSM4EncryptWithCTR(key, input):
    CMBSMLog("---------CMBSM4EncryptWithCTR start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4EncryptWithCTR", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithCTR", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4EncryptWithCTR", "Illegal Argument: input must be bytes!")
    if len(input) == 0:
        CMBSMErrorLog(-10408, "CMBSM4EncryptWithCTR", "Illegal Argument: The plaintext data length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4EncryptWithCTR", "Illegal Argument: SM4 secret key error, must be 16 bytes.")
    # 1. CTR
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_ENCRYPT)
    encData = sm4_d.crypt_ctr(input)
    CMBSMLog("encData is:" + encData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4EncryptWithCTR" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4EncryptWithCTR end-------------")
    return encData


def CMBSM4DecryptWithCTR(key, input):
    CMBSMLog("---------CMBSM4DecryptWithCTR start-------------")
    access_start = datetime.datetime.now()
    if key is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4DecryptWithCTR", "Illegal Argument: key  or input cannot be None.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithCTR", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4DecryptWithCTR", "Illegal Argument: input must be bytes!")
    if len(input) == 0 or len(input) % 16 != 0:
        CMBSMErrorLog(-10409, "CMBSM4DecryptWithCTR", "Illegal Argument: The cipher text length error, The data length must be a multiple of 16.")
    if len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4DecryptWithCTR", "Illegal Argument: SM4 secret key error, must be 16 bytes.")
    # 1. CTR
    CMBSMLog("input is:" + input.hex().upper())
    sm4_d = sm4.CryptSM4()
    sm4_d.set_key(key, sm4.SM4_ENCRYPT)
    plainData = sm4_d.crypt_ctr(input, 1)
    if plainData is None:
        CMBSMErrorLog(-10205, "CMBSM4DecryptWithCTR", "Illegal Argument: unPack plainData PKCS#7 error.")
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4DecryptWithCTR" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4DecryptWithCTR end-------------")
    return plainData


def CMBSM4GenMac_1(key, input):
    CMBSMLog("---------CMBSM4GenMac_1 start-------------")
    access_start = datetime.datetime.now()
    if input is None:
        CMBSMErrorLog(-10400, "CMBSM4GenMac_1", "Illegal Argument: input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4GenMac_1", "SM4 secret key error, must be 16 bytes.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenMac_1", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenMac_1", "Illegal Argument: input must be bytes!")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenMac_1", "The size of data in the MAC is too small.")

    # 1. MAC_1
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genMac(input, 0)
    CMBSMLog("macData is:" + macData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4GenMac_1" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4GenMac_1 end-------------")
    return macData


def CMBSM4VerMac_1(key, input, mac):
    CMBSMLog("---------CMBSM4VerMac_1 start-------------")
    access_start = datetime.datetime.now()
    if input is None or mac is None:
        CMBSMErrorLog(-10400, "CMBSM4VerMac_1", "Illegal Argument: input or mac cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4VerMac_1", "SM4 secret key error, must be 16 bytes.")

    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_1", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_1", "Illegal Argument: input must be bytes!")
    if not isinstance(mac, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_1", "Illegal Argument: mac must be bytes!")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4VerMac_1", "The size of data in the MAC is too small.")

    if len(mac) != 16:
        CMBSMErrorLog(-10412, "CMBSM4VerMac_1", "Illegal Argument: The size of MAC error, must be 16 bytes.	")

    # 1. MAC_1
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genMac(input, 0)
    CMBSMLog("Gen macData is:" + macData.hex())
    CMBSMLog("mac is:" + mac.hex())
    if mac != macData:
        CMBSMErrorLog(-10207, "CMBSM4VerMac_1", "Failed to verify MAC using SM4 algorithm.")
    else:
        success = 0
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4VerMac_1" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4VerMac_1 end-------------")
    return success


def CMBSM4GenMac_2(key, input):
    CMBSMLog("---------CMBSM4GenMac_2 start-------------")
    access_start = datetime.datetime.now()
    if input is None:
        CMBSMErrorLog(-10400, "CMBSM4GenMac_2", "Illegal Argument: input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4GenMac_2", "SM4 secret key error, must be 16 bytes.")

    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenMac_2", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenMac_2", "Illegal Argument: input must be bytes!")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenMac_2", "The size of data in the MAC is too small.")

    # 1. MAC_2
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genMac(input, 2)
    CMBSMLog("macData is:" + macData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4GenMac_2" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4GenMac_2 end-------------")
    return macData


def CMBSM4VerMac_2(key, input, mac):
    CMBSMLog("---------CMBSM4VerMac_2 start-------------")
    access_start = datetime.datetime.now()
    if mac is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerMac_2", "Illegal Argument: mac or input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4VerMac_2", "SM4 secret key error, must be 16 bytes.")

    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_2", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_2", "Illegal Argument: input must be bytes!")
    if not isinstance(mac, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_2", "Illegal Argument: mac must be bytes!")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenMac_2", "The size of data in the MAC is too small.")

    if len(mac) != 16:
        CMBSMErrorLog(-10412, "CMBSM4GenMac_2", "Illegal Argument: The size of MAC error, must be 16 bytes.	")

    # 1. MAC_2
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genMac(input, 2)
    CMBSMLog("Gen macData is:" + macData.hex())
    CMBSMLog("mac is:" + mac.hex())
    if mac != macData:
        CMBSMErrorLog(-10207, "CMBSM4VerMac_2", "Failed to verify MAC using SM4 algorithm.")
    else:
        success = 0
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4VerMac_2" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4VerMac_2 end-------------")
    return success


def CMBSM4GenMac_3(key, input):
    CMBSMLog("---------CMBSM4GenMac_3 start-------------")
    access_start = datetime.datetime.now()
    if input is None:
        CMBSMErrorLog(-10400, "CMBSM4GenMac_3", "Illegal Argument: input cannot be None.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenMac_3", "The size of data in the MAC is too small.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4GenMac_3", "SM4 secret key error, must be 16 bytes.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenMac_3", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenMac_3", "Illegal Argument: input must be bytes!")

    # 1. MAC_3
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genMac(input, 1)
    CMBSMLog("macData is:" + macData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4GenMac_3" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4GenMac_3 end-------------")
    return macData


def CMBSM4VerMac_3(key, input, mac):
    CMBSMLog("---------CMBSM4VerMac_3 start-------------")
    access_start = datetime.datetime.now()
    if mac is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerMac_3", "Illegal Argument: mac or input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4VerMac_3", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4VerMac_3", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_3", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_3", "Illegal Argument: input must be bytes!")
    if not isinstance(mac, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_3", "Illegal Argument: mac must be bytes!")
    if len(mac) != 16:
        CMBSMErrorLog(-10412, "CMBSM4VerMac_3", "Illegal Argument: The size of MAC error, must be 16 bytes.	")

    # 1. MAC_3
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genMac(input, 1)
    CMBSMLog("Gen macData is:" + macData.hex())
    CMBSMLog("mac is:" + mac.hex())
    if mac != macData:
        CMBSMErrorLog(-10207, "CMBSM4VerMac_3", "Failed to verify MAC using SM4 algorithm.")
    else:
        success = 0
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4VerMac_3" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4VerMac_3 end-------------")
    return success


def CMBSM4GenMac_4(key, input):
    CMBSMLog("---------CMBSM4GenMac_4 start-------------")
    access_start = datetime.datetime.now()
    if input is None:
        CMBSMErrorLog(-10400, "CMBSM4GenMac_4", "Illegal Argument: input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4GenMac_4", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenMac_4", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenMac_4", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenMac_4", "Illegal Argument: input must be bytes!")

    # 1. MAC_4
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genMac(input, 3)
    CMBSMLog("macData is:" + macData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4GenMac_4" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4GenMac_4 end-------------")
    return macData


def CMBSM4VerMac_4(key, input, mac):
    CMBSMLog("---------CMBSM4VerMac_4 start-------------")
    access_start = datetime.datetime.now()
    if mac is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerMac_4", "Illegal Argument: mac or input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4VerMac_4", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4VerMac_4", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_4", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_4", "Illegal Argument: input must be bytes!")
    if not isinstance(mac, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerMac_4", "Illegal Argument: mac must be bytes!")
    if len(mac) != 16:
        CMBSMErrorLog(-10412, "CMBSM4VerMac_4", "Illegal Argument: The size of MAC error, must be 16 bytes.	")

    # 1. MAC_4
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genMac(input, 3)
    CMBSMLog("Gen macData is:" + macData.hex())
    CMBSMLog("mac is:" + mac.hex())
    if mac != macData:
        CMBSMErrorLog(-10207, "CMBSM4VerMac_4", "Failed to verify MAC using SM4 algorithm.")
    else:
        success = 0
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4VerMac_4" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4VerMac_4 end-------------")
    return success


def CMBSM4GenPosMac_1(key, input):
    CMBSMLog("---------CMBSM4GenPosMac_1 start-------------")
    access_start = datetime.datetime.now()
    if input is None:
        CMBSMErrorLog(-10400, "CMBSM4GenPosMac_1", "Illegal Argument:input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4GenPosMac_1", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenPosMac_1", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenPosMac_1", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenPosMac_1", "Illegal Argument: input must be bytes!")

    # 1. POS_MAC_1
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genPosmac(input, 0)
    CMBSMLog("macData is:" + macData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4GenPosMac_1" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4GenPosMac_1 end-------------")
    return macData


def CMBSM4VerPosMac_1(key, input, mac):
    CMBSMLog("---------CMBSM4VerPosMac_1 start-------------")
    access_start = datetime.datetime.now()
    if mac is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerPosMac_1", "Illegal Argument: mac or input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4VerPosMac_1", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4VerPosMac_1", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_1", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_1", "Illegal Argument: input must be bytes!")
    if not isinstance(mac, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_1", "Illegal Argument: mac must be bytes!")
    if len(mac) != 16:
        CMBSMErrorLog(-10412, "CMBSM4VerPosMac_1", "Illegal Argument: The size of MAC error, must be 16 bytes.	")

    # 1. POS_MAC_1
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genPosmac(input, 0)
    CMBSMLog("Gen macData is:" + macData.hex())
    CMBSMLog("mac is:" + mac.hex())
    if mac != macData:
        CMBSMErrorLog(-10207, "CMBSM4VerPosMac_1", "Failed to verify MAC using SM4 algorithm.")
    else:
        success = 0
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4VerPosMac_1" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4VerPosMac_1 end-------------")
    return success


def CMBSM4GenPosMac_2(key, input):
    CMBSMLog("---------CMBSM4GenPosMac_2 start-------------")
    access_start = datetime.datetime.now()
    if input is None:
        CMBSMErrorLog(-10400, "CMBSM4GenPosMac_2", "Illegal Argument:  input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4GenPosMac_2", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenPosMac_2", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenPosMac_2", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenPosMac_2", "Illegal Argument: input must be bytes!")
    # 1. POS_MAC_2
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genPosmac(input, 2)
    CMBSMLog("macData is:" + macData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4GenPosMac_2" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4GenPosMac_2 end-------------")
    return macData


def CMBSM4VerPosMac_2(key, input, mac):
    CMBSMLog("---------CMBSM4VerPosMac_2 start-------------")
    access_start = datetime.datetime.now()
    if mac is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerPosMac_2", "Illegal Argument: mac or input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4VerPosMac_2", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4VerPosMac_2", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_2", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_2", "Illegal Argument: input must be bytes!")
    if not isinstance(mac, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_2", "Illegal Argument: mac must be bytes!")
    if len(mac) != 16:
        CMBSMErrorLog(-10412, "CMBSM4VerPosMac_2", "Illegal Argument: The size of MAC error, must be 16 bytes.	")

    # 1. POS_MAC_2
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genPosmac(input, 2)
    CMBSMLog("Gen macData is:" + macData.hex())
    CMBSMLog("mac is:" + mac.hex())
    if mac != macData:
        CMBSMErrorLog(-10207, "CMBSM4VerPosMac_2", "Failed to verify MAC using SM4 algorithm.")
    else:
        success = 0
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4VerPosMac_2" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4VerPosMac_2 end-------------")
    return success


def CMBSM4GenPosMac_3(key, input):
    CMBSMLog("---------CMBSM4GenPosMac_3 start-------------")
    access_start = datetime.datetime.now()
    if input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerPosMac_2", "Illegal Argument: input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4GenPosMac_3", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenPosMac_3", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenPosMac_3", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenPosMac_3", "Illegal Argument: input must be bytes!")

    # 1. POS_MAC_3
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genPosmac(input, 1)
    CMBSMLog("macData is:" + macData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4GenPosMac_3" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4GenPosMac_3 end-------------")
    return macData


def CMBSM4VerPosMac_3(key, input, mac):
    CMBSMLog("---------CMBSM4VerPosMac_3 start-------------")
    access_start = datetime.datetime.now()
    if mac is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerPosMac_3", "Illegal Argument: input or mac cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4VerPosMac_3", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4VerPosMac_3", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_3", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_3", "Illegal Argument: input must be bytes!")
    if not isinstance(mac, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_3", "Illegal Argument: mac must be bytes!")
    if len(mac) != 16:
        CMBSMErrorLog(-10412, "CMBSM4VerPosMac_3", "Illegal Argument: The size of MAC error, must be 16 bytes.	")

    # 1. POS_MAC_3
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genPosmac(input, 1)
    CMBSMLog("Gen macData is:" + macData.hex())
    CMBSMLog("mac is:" + mac.hex())
    if mac != macData:
        CMBSMErrorLog(-10207, "CMBSM4VerPosMac_3", "Failed to verify MAC using SM4 algorithm.")
    else:
        success = 0
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4VerPosMac_3" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4VerPosMac_3 end-------------")
    return success


def CMBSM4GenPosMac_4(key, input):
    CMBSMLog("---------CMBSM4GenPosMac_4 start-------------")
    access_start = datetime.datetime.now()
    if input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerPosMac_3", "Illegal Argument: input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4GenPosMac_4", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4GenPosMac_4", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenPosMac_4", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4GenPosMac_4", "Illegal Argument: input must be bytes!")

    # 1. POS_MAC_4
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genPosmac(input, 3)
    CMBSMLog("macData is:" + macData.hex())
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4GenPosMac_4" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4GenPosMac_4 end-------------")
    return macData


def CMBSM4VerPosMac_4(key, input, mac):
    CMBSMLog("---------CMBSM4VerPosMac_4 start-------------")
    access_start = datetime.datetime.now()
    if mac is None or input is None:
        CMBSMErrorLog(-10400, "CMBSM4VerPosMac_4", "Illegal Argument: mac or input cannot be None.")
    if key is None or len(key) != 16:
        CMBSMErrorLog(-10410, "CMBSM4VerPosMac_4", "SM4 secret key error, must be 16 bytes.")
    if len(input) == 0:
        CMBSMErrorLog(-10413, "CMBSM4VerPosMac_4", "The size of data in the MAC is too small.")
    if not isinstance(key, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_4", "Illegal Argument: key must be bytes!")
    if not isinstance(input, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_4", "Illegal Argument: input must be bytes!")
    if not isinstance(mac, bytes):
        CMBSMErrorLog(-10001, "CMBSM4VerPosMac_4", "Illegal Argument: mac must be bytes!")
    if len(mac) != 16:
        CMBSMErrorLog(-10412, "CMBSM4VerPosMac_4", "Illegal Argument: The size of MAC error, must be 16 bytes.	")

    # 1. POS_MAC_4
    sm4_m = sm4mac.MacSM4(key, sm4.SM4_ENCRYPT)
    macData = sm4_m.genPosmac(input, 3)
    CMBSMLog("Gen macData is:" + macData.hex())
    CMBSMLog("mac is:" + mac.hex())
    if mac != macData:
        CMBSMErrorLog(-10207, "CMBSM4VerPosMac_4", "Failed to verify MAC using SM4 algorithm.")
    else:
        success = 0
    access_end = datetime.datetime.now()
    access_delta = (access_end - access_start).microseconds / 1000
    CMBSMLog("CMBSM4VerPosMac_4" + "\t" + str(access_delta) + "ms" + "\t" + "SUCC")
    CMBSMLog("---------CMBSM4VerPosMac_4 end-------------")
    return success
