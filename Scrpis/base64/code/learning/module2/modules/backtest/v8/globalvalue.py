from sdk.datasource import constants


class GlobalValue:
    is_stock = True
    is_option = False
    # round number usually is 2, but testing DCC, we set as 6
    round_num = 3

    data_frequency = constants.frequency_daily

    market_info = "CN_Stock"  # CN/NYSE/HK/DCC

    @classmethod
    def set_is_stock(cls, value):
        cls.is_stock = value

    @classmethod
    def get_is_stock(cls):
        return cls.is_stock

    @classmethod
    def set_is_option(cls, value):
        if value:
            cls.is_stock = False
            cls.round_num = 4  # defaut precision for fund/option etc.
        cls.is_option = value

    @classmethod
    def get_is_option(cls):
        return cls.is_option

    @classmethod
    def set_data_frequency(cls, frequency):
        cls.data_frequency = frequency

    @classmethod
    def get_data_frequency(cls):
        return cls.data_frequency

    @classmethod
    def set_round_num(cls, num):
        cls.round_num = num

    @classmethod
    def get_round_num(cls):
        return cls.round_num

    @classmethod
    def set_market_info(cls, mi):
        cls.market_info = mi
