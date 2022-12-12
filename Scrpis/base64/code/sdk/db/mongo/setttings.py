from sdk.common import OSEnv

MONGO_URL = OSEnv.str("MONGO_URL", default="mongodb://root:root@mongodb-server:27017/?authSource=admin")
MONGO_DB = OSEnv.str("MONGO_DB", default="aiwequantdb", description="默认数据库")
