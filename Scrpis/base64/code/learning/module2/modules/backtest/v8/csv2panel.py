import pandas as pd
csv_file = '/var/app/data/bigquant/backtest/bitcoin.csv'


class bar_data:
    def __init__(self, fields):
        str_date = fields[0]
        year = str_date[6:10]
        day = str_date[:2]
        month = str_date[3:5]
        self.date = pd.Timestamp(pd.Timestamp(
            year + '-' + month + '-' + day + ' 00:00:00+00:00', tz='Asia/Shanghai'), tz='UTC')
        self.open = float(fields[1])
        self.close = float(fields[4])
        self.high = float(fields[2])
        self.low = float(fields[3])
        self.volume = float(fields[5])
        self.amount = round(self.close * self.volume, 2)
        self.adjust_factor = 1
        self.price_limit_status = 2

    def __str__(self):
        return 'bar[date:{0},open:{1},close:{2},high:{3},low:{4},vol:{5},amount:{6},adjust_factor:{7},price_limit_status:{8}]'\
            .format(self.date, self.open, self.close, self.high, self.low, self.volume, self.amount, self.adjust_factor, self.price_limit_status)


def get_panel_from_csv():
    file = open(csv_file, 'r')
    dates = []
    opens = []
    closes = []
    highs = []
    lows = []
    volumes = []
    amounts = []
    adjustfactors = []
    price_limit_statuses = []
    columns = ['date', 'open', 'high', 'low', 'close', 'volume',
               'amount', 'adjust_factor', 'price_limit_status']
    # last_year = None
    skipped_header = False
    while 1:
        line = file.readline()
        if not line:
            break
        if not skipped_header:
            skipped_header = True
            continue
        fields = line.split(',')
        # year = fields[1][:4]
        # last_year = year
        bar_item = bar_data(fields)

        # print(bar1m)
        dates.append(bar_item.date)
        opens.append(bar_item.open)
        closes.append(bar_item.close)
        highs.append(bar_item.high)
        lows.append(bar_item.low)
        volumes.append(bar_item.volume)
        amounts.append(bar_item.amount)
        adjustfactors.append(bar_item.adjust_factor)
        price_limit_statuses.append(bar_item.price_limit_status)

    dfdata = {'date': dates, 'open': opens, 'close': closes, 'high': highs, 'low': lows,
              'volume': volumes, 'amount': amounts, 'adjust_factor': adjustfactors, 'price_limit_status': price_limit_statuses}

    df = pd.DataFrame(data=dfdata, columns=columns)
    df = df.set_index('date')
    df = df.sort_index()
    pl_dict = {'BTC.DCC': df}
    pl = pd.Panel(data=pl_dict)

    return pl


if __name__ == '__main__':
    # panel = pd.read_pickle(data_file)
    # print("orig panel:", panel)
    # new_panel = get_new_panel(panel)
    # print("new_panel:", new_panel)
    pl = get_panel_from_csv()
    print(pl.items)
    print(pl['BTC.DCC'].tail())
    # file = open(new_file, 'wb')
    # pk.dump(new_panel, file)
