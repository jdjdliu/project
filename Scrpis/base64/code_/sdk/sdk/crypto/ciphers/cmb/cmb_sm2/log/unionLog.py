# -*-coding:utf-8-*- # 作者     ：tianfw
# 创建时间 ：2020/3/11 17:24
# IDE      : PyCharm

import os
import sys

sys.path.append(os.path.abspath("../"))

import os
import sys
import threading
import time
from pathlib import Path

from ..SMCryptException import SMCryptException

log_dir = ""
log_file_name = ""
file_p = None
print_lock = None
print_data_lock = None
print_type = 0


def select_file(dir):
    ret = os.access(dir, os.R_OK)
    if ret:
        ret = os.access(dir, os.W_OK)
    return ret


def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize / float(1024 * 1024)

    return round(fsize, 2)


def makedirs(path):
    if not os.path.isdir(path):
        makedirs(os.path.split(path)[0])
    else:
        return
    os.mkdir(path)


def log_init(dir):
    global print_lock
    global print_type
    global print_data_lock
    global file_p
    global log_file_name
    global log_dir
    loop = 1
    if file_p:
        log_end()

    if dir is None or len(dir) == 0:
        print_type = 1
    else:
        print_type = 2
    if len(dir) > 260:
        raise SMCryptException(-10102, "in CMBSMSetLog:Bad log file path.")

    time_str = time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time()))
    date_str = time.strftime("%Y%m%d", time.localtime(time.time()))
    log_str = "--------log start at:" + time_str + "--------"
    if print_type == 2:
        while True:
            if not isinstance(dir, str):
                print_type = 0
                raise SMCryptException(-10001, "in CMBSMSetLog::parameter type error! dir must be str!")
            if not os.path.exists(dir):
                try:
                    makedirs(dir)
                except:
                    print_type = 0
                    raise SMCryptException(-10102, "in CMBSMSetLog:: Bad log file path.")
            ret = os.access(dir, os.F_OK)
            if not ret:
                print_type = 0
                raise SMCryptException(-10102, "in CMBSMSetLog:: Bad log file path.")
            if not select_file(dir):
                print_type = 0
                raise SMCryptException(-10104, "in CMBSMSetLog::Unauthorized access!")
            my_file = Path(dir)
            if my_file.is_dir():
                log_dir = dir
                log_file_name = "CMBSM" + date_str + "_" + "%02d" % loop + ".txt"
                path = os.path.join(dir, log_file_name)
                if os.path.exists(path) and get_FileSize(path) >= 1024:
                    loop += 1
                    continue
            else:
                path = dir
            print_lock = threading.Lock()
            print_data_lock = threading.Lock()
            try:
                file_p = open(path, "ab+")
            except:
                raise SMCryptException(-10106, "in CMBSMSetLog::No permission to create file.")
            info = "save log to file: " + path + "\r\n"
            file_p.write(str.encode(info))
            file_p.write(str.encode(log_str))
            file_p.write(b"\r\n")
            break
    elif print_type == 1:
        print(log_str)
        print("\r\n")


def print_start(data):
    global print_lock
    global file_p
    print_lock.acquire()
    if type(data) is str:
        data_str = data
    else:
        data_str = "".join(data)

    strs = "\r\n/***************" + data_str + " START ***************/"
    print(strs)
    file_p.write(strs)
    file_p.write("\r\n")
    print_lock.release()


def print_end(data):
    global print_lock
    global file_p
    print_lock.acquire()
    if type(data) is str:
        data_str = data
    else:
        data_str = "".join(data)
    strs = "/***************" + data_str + " END ***************/"
    print(strs)
    file_p.write(strs)
    file_p.write("\r\n")
    print_lock.release()


def CMBSMLog(data):
    global print_type
    global file_p
    global print_lock
    global log_file_name
    global log_dir
    if type(data) is str:
        data_str = data
    else:
        data_str = "".join(data)

    if print_type == 2:
        if not file_p:
            raise SMCryptException(-10100, "in CMBSMLog:: No initialization log file path.")
        time_str = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
        print_lock.acquire()
        size = get_FileSize(os.path.join(log_dir, log_file_name))
        if size >= 1024:
            log_end()
            log_init(log_dir)
        try:
            file_p.write(str.encode(time_str))
            file_p.write(b"\t")
            file_p.write(str.encode(data_str))
            file_p.write(b"\r\n")
        finally:
            print_lock.release()
    elif print_type == 1:
        print(data_str)


def print_data(data, info=""):
    global print_data_lock
    print_data_lock.acquire()
    if type(data) is list:
        data = data
    elif type(data) is str:
        data = [ord(x) for x in data]
    else:
        print("Type Error!")

    cnt = 0
    log_str = ""
    if len(info):
        CMBSMLog(info)
    for d in data:
        cnt += 1
        log_str += "0x%02x " % d
        if cnt % 16 == 0:
            CMBSMLog(log_str)
            log_str = ""
    if len(data) < 16:
        CMBSMLog("   [" + log_str + "]")
        print_data_lock.release()
        return
    if cnt % 16 != 0:
        CMBSMLog(log_str)
        # CMBSMLog('')
    print_data_lock.release()


def CMBSMHexLog(data, datalen, info=""):
    global print_data_lock
    print_data_lock.acquire()
    if type(data) is list:
        data = data
    elif type(data) is str:
        data = [ord(x) for x in data]

    if len(info):
        CMBSMLog(info)

    for i in range(datalen):
        if i and i % 16 == 0:
            CMBSMLog("")
        CMBSMLog("0x%02x ," % (data[i]))
    # print ''
    print_data_lock.release()


def CMBSMErrorLog(errCode, func, data):
    global print_type
    global print_lock
    global file_p
    global log_dir
    global log_file_name
    if type(data) is str:
        data_str = data
    else:
        data_str = "".join(data)
    time_str = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
    if print_type == 2:
        if not file_p:
            raise SMCryptException(-10100, "in CMBSMErrorLog:: No initialization log file path.")
        print_lock.acquire()
        size = get_FileSize(os.path.join(log_dir, log_file_name))
        if size >= 1024:
            log_end()
            log_init(log_dir)
        file_p.write(str.encode(time_str))
        file_p.write(b"\t")
        file_p.write(str.encode(func))
        file_p.write(b"\t")
        file_p.write(str.encode("ERROR"))
        file_p.write(b"\t")
        file_p.write(str.encode(str(errCode)))
        file_p.write(b"\t")
        file_p.write(str.encode(data_str))
        file_p.write(str.encode("\r\n"))
        print_lock.release()
    elif print_type == 1:
        print(data_str)
        # print("\r\n")
    raise SMCryptException(errCode, data_str)


def log_end(data=None):
    global print_type
    global print_lock
    global file_p
    global print_data_lock
    if data is None:
        info = "--------log end--------"
    else:
        if type(data) is str:
            info = data
        else:
            info = "".join(data)
    time_str = time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time()))
    log_str = "\r\n--------log end at:" + time_str + "--------"
    if print_type == 2:
        if not file_p:
            raise SMCryptException(-10100, "in CMBSMLogEnd:: No initialization log file path.")
        print_lock.acquire()
        if file_p is not None:
            file_p.write(str.encode(info))
            file_p.write(b"\r\n")
            file_p.write(str.encode(log_str))
            file_p.close()
            file_p = None
        print_lock.release()
        print_lock = None
        print_data_lock = None
        print_type = 0
    elif print_type == 1:
        print(info)
        # print("\r\n")
        print(log_str)
