import bigexpr
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list

import learning.api.tools as T

# 是否自动缓存结果
bigquant_cacheable = True

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = True
bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = "数据输入输出"
bigquant_friendly_name = "数据源"
bigquant_doc_url = "https://bigquant.com/docs/"


class BigQuantModule:
    def __init__(
        self,
        datasource_id: I.str("ID或别名，示例 bar1d"),
        start_date: I.str("开始日期") = "",
        end_date: I.str("结束日期") = "",
        instruments: I.port("代码列表", specific_type_name="列表|DataSource", optional=True) = None,
        features: I.port("特征列表", specific_type_name="列表|DataSource", optional=True) = None,
    ) -> [I.port("数据源", "data")]:
        """
        输入数据api名称和时间段，输出提取出来的数据，dataframe格式。用于替换DataSource语句。可用的数据api见帮助文档的【数据字典】部分。
        """
        self.__id = datasource_id
        self.__i = smart_list(instruments)
        self.__start_date = self.__i["start_date"] if self.__i else start_date
        self.__end_date = self.__i["end_date"] if self.__i else end_date
        self.__instruments = self.__i["instruments"] if self.__i else None
        features = smart_list(features)
        self.__features = bigexpr.extract_variables(features) if features else None
        # 更新绑定实盘运行参数
        is_live_run = T.live_run_param("trading_date", None) is not None
        if is_live_run:
            self.__end_date = T.live_run_param("trading_date", "trading_date")

    def run(self):
        if self.__start_date or self.__end_date or self.__instruments or self.__features:
            ds = DataSource(self.__id)
            df = ds.read(instruments=self.__instruments, start_date=self.__start_date, end_date=self.__end_date, fields=self.__features)
            ds = DataSource.write_df(df)
            return Outputs(data=ds)
        else:
            return Outputs(data=DataSource(self.__id))


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
