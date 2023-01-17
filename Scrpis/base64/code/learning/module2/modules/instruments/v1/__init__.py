# -*- coding: utf-8 -*-
from sdk.utils import BigLogger

import pandas as pd
from sdk.datasource import DataReaderV2, DataSource, Market

from learning.module2.common.data import Outputs
from learning.module2.common.utils import read_hdf

D2 = DataReaderV2()
# log = logbook.Logger('instruments')
log = BigLogger("instruments")
bigquant_cacheable = True
bigquant_deprecated = "此模块不再维护"


class BigQuantModule:
    def __init__(self, start_date, end_date, market=Market.MG_CHINA_STOCK_A.symbol):
        """
        获取指定时间段，指定市场可交易产品的代码列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param market: 市场，目前支持 CN_STOCK_A ，表示A股市场
        @opensource
        """
        self.__start_date = start_date
        self.__end_date = end_date
        self.__market = market

    def run(self):
        instruments = D2.instruments(start_date=self.__start_date, end_date=self.__end_date, market=self.__market)
        ds = DataSource()
        pd.DataFrame({"instrument": instruments}).to_hdf(ds.path)
        return Outputs(data=ds)


def bigquant_postrun(outputs):
    outputs.extend_methods(read_data=lambda self, key=None: read_hdf(self.data.path, key))

    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
