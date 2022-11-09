from learning.module2.common.data import Outputs, DataSource
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger
import pandas as pd

# log = logbook.Logger('dropnan')
log = BigLogger("dropnan")
bigquant_cacheable = True

# 模块接口定义
bigquant_category = "数据处理"
bigquant_friendly_name = "缺失数据处理"
bigquant_doc_url = "https://bigquant.com/docs/"


class BigQuantModule:
    def __init__(
        self,
        input_data: I.port("输入数据源"),
        features: I.port("训练-特征，去掉指定特征列空值。默认为空，去掉有空值的行", optional=True, specific_type_name="列表|DataSource") = None,
    ) -> [I.port("数据", "data")]:
        """
        缺失数据处理：删除指定特征列有缺失数据的行
        """
        self.__data = input_data
        self.__features = smart_list(features) if smart_list(features) != [] else None

    def run(self):
        output_ds = DataSource()
        output_store = output_ds.open_df_store()

        before_drop_row_count = 0
        after_drop_row_count = 0
        for key, df in self.__data.iter_df():
            before_drop_row_count += len(df)
            final_df = df.dropna(subset=self.__features)
            if len(final_df) > 0:
                for column in final_df.columns:
                    if pd.api.types.is_bool(final_df[column].iloc[0]):
                        final_df[column] = final_df[column].astype("int")
                output_store[key] = final_df
                after_drop_row_count += len(final_df)
            log.info(f"{key}, {len(final_df)}/{len(df)}")

        output_ds.close_df_store()

        if after_drop_row_count == 0:
            raise Exception("no data left after dropnan")
        log.info(f"行数: {after_drop_row_count}/{before_drop_row_count}")

        return Outputs(data=output_ds)


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    from learning.api import M

    def m2_run_bigquant_run(input_1, input_2, input_3):
        # 示例代码如下。在这里编写您的代码
        df = pd.DataFrame({"data1": [4, 5, 6], "1": [0, 0, 0], "list": [1, 1, 1]})
        data_1 = DataSource.write_df(df)
        data_2 = DataSource.write_pickle(df)
        return Outputs(data_1=data_1, data_2=data_2, data_3=None)

    # 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
    def m2_post_run_bigquant_run(outputs):
        return outputs

    # Python 代码入口函数，input_1/2/3 对应三个输入端，data_1/2/3 对应三个输出端
    def m3_run_bigquant_run(input_1, input_2, input_3):
        # 示例代码如下。在这里编写您的代码
        df = pd.DataFrame({"list": [1, 2, 3, 1], "data1": [9, 9, 9, 4], "2": [7, 7, 9, 7]})
        data_1 = DataSource.write_df(df)
        data_2 = DataSource.write_pickle(df)
        return Outputs(data_1=data_1, data_2=data_2, data_3=None)

    # 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
    def m3_post_run_bigquant_run(outputs):
        return outputs

    m2 = M.cached.v3(run=m2_run_bigquant_run, post_run=m2_post_run_bigquant_run, input_ports="", params="{}", output_ports="", m_cached=False)

    m3 = M.cached.v3(run=m3_run_bigquant_run, post_run=m3_post_run_bigquant_run, input_ports="", params="{}", output_ports="", m_cached=False)

    m1 = M.join.v3(data1=m2.data_1, data2=m3.data_1, on="data1,list", how="outer", sort=False, m_cached=False)
    m5 = M.input_features.v1(
        features="""# #号开始的表示注释
    # 多个特征，每行一个，可以包含基础特征和衍生特征
    """
    )
    m4 = M.dropnan.v1(input_data=m1.data, features=m5.data)
    print(m4.data.read())
