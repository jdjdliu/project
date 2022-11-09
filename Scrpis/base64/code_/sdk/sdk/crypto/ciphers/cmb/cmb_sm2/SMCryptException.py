# _*_coding:utf-8
# 作者      : 80234805
# 创建时间  : 2020-1-7
# 文件      : SMCryptException.py
import traceback


class SMCryptException(Exception):
    def __init__(self, var1, var2):
        super().__init__(var1, var2)
        self.errorCode = var1
        self.errorMsg = var2

    def getErrorCode(self):
        return self.errorCode

    def getErrorMsg(self):
        return self.errorMsg
