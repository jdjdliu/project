import learning.module2.common.interface as I
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

log = BigLogger("resample")
bigquant_cacheable = True

# 模块接口定义
bigquant_category = "数据处理"
bigquant_friendly_name = "数据重采样Resample"
bigquant_doc_url = ""


class BigQuantModule:
    def __init__(
        self, data1: I.port("数据输入", specific_type_name="DataSource"), group: I.str("分组列") = "instrument", sessions: I.str("频率类别") = "Q"
    ) -> [I.port("重采样后的输出数据", "data")]:
        """

        对输入数据进行resample处理

        """
        self.__data = data1
        self.__group = group
        self.__sessions = sessions

    def run(self):
        df = self.__data.read()
        df = df.groupby(self.__group).apply(lambda tmp: tmp.set_index("date").resample(self.__sessions).ffill().reset_index()).reset_index(drop=True)
        data_1 = DataSource.write_df(df)
        return Outputs(data=data_1)


def bigquant_postrun(outputs):
    return outputs
