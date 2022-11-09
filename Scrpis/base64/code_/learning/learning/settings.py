import os
from sdk.common import OSEnv

# load env settings, e.g. bigjupyteruser__service__settings__current_version=abcd python3 server.py

site = os.environ.get("SITE", "cmb")

vip_users = []
run_mode = os.environ.get("RUN_MODE", "backtest")
is_paper_trading = run_mode == "papertrading"

gpu_node_selector = "gpu=true"
gpu_node_selector_by_user = {}
enabled_gpu_remoterun_users = []
# 使用gpu打包运行
enable_gpu_remoterun = False
enabled_gpu_remoterun_modules = ["dl_model_train", "dl_model_predict"]
always_remoterun_modules = ["gpu_acceleration", "gpu_hyper_run"]
has_postrun_params_modules = ["gpu_acceleration"]

# userbox中的用户名
jpy_user_name = os.getenv("JPY_USER", False)

# 默认namespace
default_namespace = "bigquant"

# 数据每个chunk数据行数
max_chunksize = int(os.getenv("MAX_CHUNKSIZE", 100 * 10000))

# 不使用缓存, 只在测试环境设置为True, 生产环境需要缓存
cache_disabled = OSEnv.bool("CACHE_DISABLED", False)

# TODO：稳定后删除，用于授权用户使用自动超参搜索模块
auto_hyper_module_user = []
