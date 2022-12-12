# TODO：简化实现，先利用dsutils分离简化DataSource的实现
import datetime
import hashlib
import os
import uuid
import shutil

from sdk.datasource.extensions.bigshared import utils as sharedutils
from sdk.datasource.extensions.bigshared.settings import mongo_client
from sdk.datasource.api.utils.dataplatform import data_source_create, get_metadata


WRITE_MODE_UPDATE = "update"
WRITE_MODE_FULL = "full"
FILE_TYPE_H5 = "h5"
FILE_TYPE_PICKLE = "pkl"
FILE_TYPE_CSV = "csv"
DATA_BASE_BIGQUANT = "bigquant"
DATA_BASE_USER = "user"
DATA_BASE_USER_PRIVATE = "userprivate"

ALIAS_BQ_PREFIX = "bigquant"
ALIAS_SUB_SEPARATOR = "."
ALIAS_SEPARATOR = "-"
BASE_PATH = "bigquant/datasource/{under}/{version}/{id_0_1}/{id_1_3}/{id}"
PATH_PATTERN = BASE_PATH + "/{date}.{file_type}"
STORAGE_KEY = "data"
VERSION = "v3"

USER_ALIAS_SUFFIX = "_U"  # 用户数据alias后缀
USER_HASH_SUFFIX = "U"  # 用户数据uuid后缀

# TODO: do we need to support this?
# remote_run: see bigjupyteruserservice/settings.py JobType.RemoteRun
# is_remote_run = os.environ.get('JOB_TYPE', None) == 'remote_run'
is_paper_trading = os.environ.get("RUN_MODE", "backtest") == "papertrading"
# 强制使用alias作为id，仅用于测试，e.g. bigalpha/tests/__init__.py
force_alia_as_id = False
# user_client = BigJupyterUserClient().instance()


def on_create(id, version=VERSION, file_type=None, data_base=DATA_BASE_USER, schema_json=None):
    data = data_source_create(id, version, file_type, data_base, schema_json)
    return data


def on_visit(id, get_meta=False, update_stats=True):
    metadata = get_metadata(id, get_meta=get_meta, update_stats=True)
    return metadata


def curr_user():
    return os.environ.get("JPY_USER", "bigquant")


def run_mode():
    return os.environ.get("RUN_MODE", "backtest")


def gen_id_for_temp_ds(flag=True):
    # TODO: 中间id，使用T结尾；其他场景？
    # 改成4，随机生成
    if flag is True:
        return uuid.uuid4().hex + "T"
    else:
        return uuid.uuid4().hex


def is_temp_ds(id):
    if len(id) == 33 and id[-1] == "T" and (id[-2].islower() or id[-2].isdigit()):
        return True

    # TODO 去掉这个判断，当都是用 T 结尾的时候
    # 兼容旧的uuid
    # 不严密的判断，只检查了长度
    if len(id) == 32 and "-" not in id and "_" not in id:
        return True

    return False


def is_system_ds(id):
    if is_temp_ds(id):
        return False
    if id.startswith("bigquant-"):
        return False
    return True


def load_meta(ds, force_load=False):
    if hasattr(ds, "__bq_protected_meta"):
        return
    if not force_load and is_temp_ds(ds.id):
        return
    if not force_load and ds.id.startswith("bigquant-"):
        return

    # TODO: read from database
    # setattr(ds, '__bq_protected_meta', value)
    return None


# TODO: 完善此函数，并最终都使用这个函数
#    中间数据 DataSource：uuid() + 'T'
#    平台公共数据 DataSource：alias （小写字母、数字、下划线）
#    bigquant手动添加数据：bigquant-xxxx （小写字母、数字、下划线）
#    仅在id传入的是uuid，且是系统表的时候，传入的data_base为True
def id_to_path(id, version, data_base=None):
    if not is_system_ds(id) and data_base is None:
        data_base = DATA_BASE_USER
    else:
        # TODO: read mongo to get real id from given alias(id)
        if data_base is None:
            # 此时id应为alias,需要查询数据库获取id
            visit_info = on_visit(id, get_meta=True)
            id = visit_info.get("id", id)
        data_base = DATA_BASE_BIGQUANT

    path = sharedutils.data_path("bigquant/datasource", data_base, version, id[0:1], id[1:3], id)
    return path


def id_to_intermediate_path(id):
    # 中间用于暂存的地址，先写入到这个地址，jupyteruserservice 移动到目标地址上
    return id_to_path(id, "intermediate")


def exists(id, version):
    # 判断datasource的数据文件是否存在,系统表只能判断目录是否存在
    path = id_to_path(id, version)
    return os.path.exists(path)


def pre_write(alias):
    # TODO: remove this??
    if not alias.split(ALIAS_SEPARATOR, 1)[1].isalnum():
        raise Exception("illegal alias")
    if mongo_client().bigquant.table_metadata.find_one({"alias": alias}):
        raise Exception("duplicated alias")


def local_temp_path(path):
    """
    容器内的临时目录：用于本地加速
    读写的时候先尝试容器本地临时目录。一般只在远程运行的时候启用
    """
    base = "/tmp/bigdatasource"
    if not os.path.exists(base):
        os.makedirs(base)
    path = str(path)
    path_id = hashlib.md5(path.encode("utf8")).hexdigest()
    path_part = path.replace("/", "_").replace("\\", "_")[-64:]
    return "%s/%s_%s" % (base, path_id, path_part)


# TODO: remove this
def insert_table_metadata(source_id, alias, under, owner, file_type, temp, schema, original_size, create_time, update_time):
    table_metadata = {
        "source_id": source_id,
        "alias": alias,
        "under": under,
        "owner": owner,
        "file_type": file_type,
        "temp": temp,
        "schema": schema,
        "original_size": original_size,
        "create_time": create_time,
        "update_time": update_time,
    }
    mongo_client().bigquant.table_metadata.update_one({"source_id": source_id}, {"$set": table_metadata}, upsert=True)
    # mongo_client().bigquant.table_metadata.insert_many([table_metadata])


def fetch_info(id):
    """
    :param id : 传递 source id 或者 alias
    :return: 返回所有的数据
    """
    table_metadata = mongo_client().bigquant.table_metadata.find_one({"source_id": id})
    table_metadata = table_metadata or mongo_client().bigquant.table_metadata.find_one({"alias": id})
    if not table_metadata:
        return {}
        # raise Exception('not found id:{0}'.format(id))
    return table_metadata


def update_table_stats(id):
    now = datetime.datetime.now()
    table_stats_data = {"last_hit_at": now}
    if run_mode() == "papertrading":
        table_stats_data.update({"paper_trading": True})

    mongo_client().bigquant.table_stats.update_one(
        {"source_id": id},
        {"$set": table_stats_data, "$inc": {"hits": 1}, "$push": {"access_list": {"$each": [{"user": curr_user(), "time": now}], "$slice": -50}}},
        upsert=True,
    )


def chown_path_bigquant(base_path):
    # HACK: do not chown
    return None
    # shutil.chown(base_path, 'bigquant', 'bigquant')
    # parent_path = os.path.dirname(base_path)
    # shutil.chown(parent_path, 'bigquant', 'bigquant')
    # parent_path = os.path.dirname(parent_path)
    # shutil.chown(parent_path, 'bigquant', 'bigquant')
