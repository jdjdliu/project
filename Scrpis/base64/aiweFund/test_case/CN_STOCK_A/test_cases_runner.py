



if __name__ == "__main__":
    import os
    import unittest
    import datetime

    test_case_path = "/var/app/data/bigquant/datasource/data_build/aiweFund/test_case/CN_STOCK_A/"
    test_find_pattern = "test*.py"
    test_suite = unittest.defaultTestLoader.discover(test_case_path, test_find_pattern)
    from sdk.datasource import DataSource, D
    test_date_str = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    test_date_str = D.trading_days(end_date=test_date_str).date.dt.strftime('%Y-%m-%d').tolist()[-1]
    test_result_file = os.path.join(test_case_path, f"record_files/{test_date_str}.txt")
    test_total_file = os.path.join(test_case_path, f"record_files/total_record.txt")
    with open(test_result_file, mode='a', encoding='utf-8') as f:
        runner = unittest.TextTestRunner(stream=f)
        res = runner.run(test_suite)

    total_count = res.testsRun
    failure_count = len(res.failures)
    error_count = len(res.errors)
    with open(test_total_file, mode='a', encoding='utf-8') as f:
        f.write(f"[{test_date_str}]: total_test--> {total_count}  failure_count--> {failure_count}  error_count--> {error_count}\n")


