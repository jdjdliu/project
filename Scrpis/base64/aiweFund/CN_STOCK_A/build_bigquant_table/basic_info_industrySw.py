import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import basic_info_IndustrySw as category_info


class BuildSw(Build):

    def __init__(self,write_mode='update'):
        super(BuildSw, self).__init__(None, None)

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': None,
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'primary_key': ['code', 'industry_sw_level', 'version'],
            'active': True,
            'fields': {'code': {'desc': '申万代码', 'type': 'str'},
                       'name': {'desc': '申万分类名', 'type': 'str'},
                       'industry_sw_level': {'desc': '申万行业级别', 'type': 'int32'},
                       'version': {'desc': '申万规则版本', 'type': 'int32'},
                       },
            'partition_date': None,
            # 'doc_show_all_data': True,
            'file_type': 'bdb'
        }
        self.write_mode = write_mode
        self.alias = 'basic_info_IndustrySw'

    def run(self):
        df = pd.read_hdf('../basic_info_IndustrySw.h5.csv')
        print(df)
        self._update_data(df=df)


if __name__ == '__main__':
    BuildSw().run()
