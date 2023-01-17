from sdk.common import OSEnv

USERSERVICE_HOST = OSEnv.str("USERSERVICE_HOST", default="http://userservice")
MAIN_FACTION = """# Python 代码入口函数,input_1/2/3 对应三个输入端,data_1/2/3 对应三个输出端
        def bigquant_run(input_1, input_2, input_3):
            # 示例代码如下。在这里编写您的代码
            df = pd.DataFrame({'data': [1, 2, 3]})
            data_1 = DataSource.write_df(df)
            data_2 = DataSource.write_pickle(df)
            return Outputs(data_1=data_1, data_2=data_2, data_3=None)
        """
DEFAULT_POST_RUN = """# 后处理函数,可选。输入是主函数的输出,可以在这里对数据做处理,或者返回更友好的outputs数据格式。此函数输出不会被缓存。
        def bigquant_run(outputs):
            return outputs
        """
