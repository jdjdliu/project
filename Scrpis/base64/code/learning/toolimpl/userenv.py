import os


class UserEnv:
    def __init__(self):
        self.__user_name = os.environ.get("JPY_USER", None)
        # run mode: backtest -> papertrading -> livetrading
        self.__run_mode = os.environ.get("RUN_MODE", None) or "backtest"
        self.__strategy_display_name = os.environ.get("STRATEGY_DISPLAY_NAME", None)
        self.__market_type = os.environ.get("MARKET_TYPE", "0")  # 默认为0，中国A股
        self.__trading_date = os.environ.get("TRADING_DATE", None)
        self.__test_start_date = os.environ.get("TEST_START_DATE", None)
        self.__test_end_date = os.environ.get("TEST_END_DATE", None)
        # PAPER_TRADING_ACCOUNT --> algo
        self.__paper_trading_account = os.environ.get("PAPER_TRADING_ACCOUNT", None)

        # TODO: live_trading_account: accountname and password (encrypt??)

    def is_back_test(self):
        return self.__run_mode == "backtest"

    def is_paper_trading(self):
        return self.__run_mode == "papertrading"

    def is_live_trading(self):
        return self.__run_mode == "livetrading"

    @property
    def user_name(self):
        return self.__user_name

    @property
    def run_mode(self):
        return self.__run_mode

    @property
    def strategy_display_name(self):
        return self.__strategy_display_name

    @property
    def market_type(self):
        return self.__market_type

    @property
    def trading_date(self):
        return self.__trading_date

    @property
    def test_start_date(self):
        return self.__test_start_date

    @property
    def test_end_date(self):
        return self.__test_end_date

    @property
    def paper_trading_account(self):
        return self.__paper_trading_account

    def __repr__(self):
        return "UserEnv(user_name=%s, run_mode=%s, market_type=%s, strategy_display_name=%s, trading_date=%s, paper_trading_account=%s)" % (
            self.__user_name,
            self.__run_mode,
            self.__market_type,
            self.__strategy_display_name,
            self.__trading_date,
            self.__paper_trading_account,
        )


_user_env = UserEnv()


def get_user_env():
    return _user_env


if __name__ == "__main__":
    # 测试代码
    print(get_user_env())
