import os

from .bigsettings import settings_from_env

# 这里使用的任何密码和key都仅用于本地调试，必须在部署的环境里被重写，不得用于任何生产环境

# 简化常用数据库配置，可以使用环境变量 POSTGRES_CONF / bigshared__common__settings__postgres_conf 等 来配置相关参数
postgres_conf = os.getenv("POSTGRES_CONF", "postgresql+psycopg2://postgres:5432")
mysql_conf = os.getenv("MYSQL_CONF", "Server=mysql;Uid=root;Pwd=bigquant;")
mongodb_conf = os.getenv("MONGODB_CONF", "mongodb://mongodb-server:27017")
amqp_conf = os.getenv("AMQP_CONF", "amqp://rabbitmq-cluster-balancer")

service_log_level = "INFO"

# TODO: clean those START
service_port_bigenterpriseservice = 37171
deploy_mode = os.getenv("DEPLOY_MODE", "zx")

AI_CLOUD_USER_PW = os.getenv("AI_CLOUD_USER_PW", "")
SCHEDULE_USER_PW = os.getenv("SCHEDULE_USER_PW", "")

AI_CLOUD_DB_PASSWORD = "" if not AI_CLOUD_USER_PW else AI_CLOUD_USER_PW.split(":")[1]
SCHEDULE_DB_PASSWORD = "" if not SCHEDULE_USER_PW else SCHEDULE_USER_PW.split(":")[1]

AI_CLOUD_AUTH = "postgresql+psycopg2://{}@postgres".format(AI_CLOUD_USER_PW)
SCHEDULE_AUTH = "postgresql+psycopg2://{}@postgres".format(SCHEDULE_USER_PW)

SCHEDULE_URI = "{}/bigquant_schedule".format(SCHEDULE_AUTH)
LIVERUN_URI = "{}/bigquant_liverun".format(SCHEDULE_AUTH)
LIVETRADING_URI = "{}/livetrading".format(SCHEDULE_AUTH)
PERFORMANCE_URI = "{}/performance".format(SCHEDULE_AUTH)
PGCONTENT_DB_URI = "{}/pgcontents".format(SCHEDULE_AUTH)
BIGAUTH_DB_URI = "{}/bigauth".format(AI_CLOUD_AUTH)

BIGQUANT_SERVICE_DB_URI = "{}/bigquant_service".format(AI_CLOUD_AUTH)
JUPYTERHUB_DB_URI = "{}/bigquant_jupyterhub".format(AI_CLOUD_AUTH)
AIFLOW_DB_URI = "{}/aiflow".format(AI_CLOUD_AUTH)
# TODO: clean those END

RSA_KEY = """-----BEGIN RSA PRIVATE KEY-----
MIICYAIBAAKBgQCJBs2BO1fcmIuUIZ6+LxKyE6Y9DJY/3rv2vrIVBegmSHUpBoek
BKl4m1A8r2uVJolH8T2HV0q//tSywOYn26fzYhbDorVTJMFak2UuDviOAlQFUEJj
ZjE0Gf54G6JjBLBre/15YyMb9OcsDm7E9RPywVu2hXXsw8Ur4oq5ECGQoQIDAQAB
AoGATzLOXq5lBwxoSFLyRGxCuwheYhS/VuUu8NuhZENIva/Nf8vVZDiJlzr1sk2V
udxPJnTJoksS2ku4tiSFrqC6ieFB1zMTHEq3n/79i3RV1FKH6feK2H/pCM9kC7Qi
CIcuFyTvtdtuhkoHjBhoBJpGv9kwdaSBpUQXGFlFKq7Uls0CRQDIWlOwqn4+wd/b
P1DZeKTIsU/JGCFNlkS0nrA6+9ZVnv17bjeGsLXztgcfKT9Z8at+tKPO88RSsYgK
Ss11/tbM82nBWwI9AK8VywPMSYCS4KJntu8xB39CsckbZLHc3Gzp9WHrfBKGRR2m
5JvIhAsWth54kR9JycGGOQEIuWfPWT56swJELtjQENMMyefTTBS19uO6xCKRoVe/
EcE9N26TjncsWsDeTGotGZVgNigt6h9e7M0RaIMceluNYDiasq1i9WAtIacY51kC
PA33gxZueX6UOO87rtAt7RctztjoIoQ9xie6EiUZj/A6XdEbuyjiOgZYbXbIz2rb
jAiwGGQIuiGZ3NirGQJFAKXqDo2w/LmMGijL3RLGO48pQ/oVzZHNdVsC4z+Mbe7l
eARDCpZckC5p7yhRazv8Q0wy2vz2pgWRJvr72YTSwXPrWPxA
-----END RSA PRIVATE KEY-----
"""

RSA_PUBLIC_KEY = """-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAIkGzYE7V9yYi5Qhnr4vErITpj0Mlj/eu/a+shUF6CZIdSkGh6QEqXib
UDyva5UmiUfxPYdXSr/+1LLA5ifbp/NiFsOitVMkwVqTZS4O+I4CVAVQQmNmMTQZ
/ngbomMEsGt7/XljIxv05ywObsT1E/LBW7aFdezDxSviirkQIZChAgMBAAE=
-----END RSA PUBLIC KEY-----
"""

scheduler_repo_url_base = "http://bignbscheduler-repo:8888/api"

strategyfile_fetch_url = "https://bigquant.com/livetrading/strategyfile/fetch"

# 需为16位
AES_KEY = "cYa4T37bT8bPQe58"

AES_IV = "5E2P9zW54Ae0vW32"

aiflow_api_url = "http://aiflowweb:8080/aiflow/web/api/v1"

# nbconvert模板
nbconvert_template_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "nbconvert/templates")

# 内部用户配置模拟交易数
BIGQUANT_INNER_USER = {"rydeng": 50, "bigquant": 50, "jliang": 50}

# 资源包模拟交易位
SOURCE_FOR_PAPERTRADING = 31

# 资源包AI任务
SOURCE_FOR_AI = 32

# 资源包GPU
SOURCE_FOR_GPU = 39

# 新版本上线时间
BIGMEMBER_ONLINE_TIME = "2020-06-20 18:00:00"

# 邮箱相关配置
sendcloud_api_user = "bigquant"
sendcloud_api_key = "gnXEhcYyeynR4rYn"
mailgun_user = "no-reply@message.bigquant.com"
mailgun_passwd = "MOW204ofndmvi1f"
mailgun_api_key = "key-7adebae276a2a839b9508b19b9ba4e8b"

# 短信发送相关配置
SMS_USER = "bigquant"
SMS_KEY = "HeYgjx8CjIRZF7UssWZkrJvZJX9weSCo"
SEND_MSG_URL = "http://www.sendcloud.net/smsapi/send"

# 微信发送相关, 测试环境为wxfabf0113af6fb1a9 和 1fb5294f33c264e9b140434c87d43845
wechat_appid = "wx23397a234169d066"
wechat_secret = "8e35b02274ba84f413475e7e65929f36"


# load env settings, e.g. bigshared__common__settings__SCHEDULE_URI
settings_from_env("bigshared.common.settings", globals())


# TODO: remove this. DO import from singleton directly
def mongo_client():
    from .mongoclientinstance import MongoClientInstance

    return MongoClientInstance.instance()
