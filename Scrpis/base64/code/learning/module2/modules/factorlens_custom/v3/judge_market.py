def judge_market(factor_data):
    """
     # CN_MULFUND = {'HOF', 'HOF2', 'OFCN', 'OFCN2', 'OFCN3', 'OFHK', 'ZOF', 'ZOF2', 'ZOF3'}   
     # CN_STOCK_A = {'SHA', 'SZA'}    CN_STOCK_A
     # CN_FUTURE= {'CFX', 'CZC', 'DCE', 'INE', 'SHF'}  
     # CN_FUND  = {'HOF', 'HOF2', 'ZOF', 'ZOF2', 'ZOF3'}  
    """
    unique_ins = set([i.split('.')[-1] for i in factor_data.instrument.unique()])
    if unique_ins.issubset( {'CFX', 'CZC', 'DCE', 'INE', 'SHF'} ):
        return "CN_FUTURE"
    elif unique_ins.issubset({'SHA', 'SZA'} ):
        return "CN_STOCK_A"
    elif 'OFCN' in unique_ins and unique_ins.issubset( {'HOF', 'HOF2', 'OFCN', 'OFCN2', 'OFCN3', 'OFHK', 'ZOF', 'ZOF2', 'ZOF3'}):
        return "CN_MUTFUND"
    elif 'OFCN' not in unique_ins and unique_ins.issubset( {'HOF', 'HOF2', 'OFCN', 'OFCN2', 'OFCN3', 'OFHK', 'ZOF', 'ZOF2', 'ZOF3'}):
        return "CN_FUND"
    else:
        print('请检查输入的品种代码及其对应市场是否一致')