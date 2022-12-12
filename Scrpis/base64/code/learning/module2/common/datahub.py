import os
import datetime
import shutil
import hashlib
import re

from learning.settings import jpy_user_name, default_namespace
from bigjupyteruserservice.server.settings import node_selector_user_box, volumes, volume_mounts

aiflow_job_key = "AIFLOW_JOB"

jupyter_base_cmd = "jupyter nbconvert " "--execute " "--to notebook " "--inplace " "--ExecutePreprocessor.timeout=-1 " "--log-level=10"


class AiFlowDagUtil(object):

    base_work_path = "/home/bigquant/work"

    # 在定时任务运行的Pod中挂载/home下, /root下没有权限
    aiflow_job_pod_dag_path = "/home/airflow/dags"
    aiflow_job_pod_user_strategy_folder = "{}/jupyteruser_dags/{}/scripts".format(aiflow_job_pod_dag_path, jpy_user_name)

    default_args_name = "default_args"
    dag_name = "dag"

    # 定时任务环境变量，从当前用户的userbox中获取
    env_keys = ["JPY_USER", "DEPLOY_MODE", "SERVICE_AUTH_TOKEN", "ENVIRONMENT", "SITE"]

    def __init__(self, strategy_file):
        # self.strategy_file = strategy_file
        if self.base_work_path in strategy_file:
            strategy_file = strategy_file.replace(self.base_work_path, "")
        self.username = jpy_user_name
        # 根据当前notebook的文件路径生成一个对应工作目录的唯一文件名
        md5 = hashlib.md5(strategy_file.encode(encoding="utf-8")).hexdigest()
        file_name = os.path.split(strategy_file)[-1].rsplit(".", 1)[0]
        self.task_id = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", file_name)

        # 双下划线 '__' 后面的是会展示在 aifow 任务列表中展示的名称
        username = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", self.username)
        self.dag_id = "{}_{}__{}".format(username, md5[:8], self.task_id)

        # dag文件路径   默认和ipynb文件在一起 只是增加后缀 _dag.py
        self.dag_file_path = os.path.join(self.base_work_path, "{}.py".format(self.dag_id))

        strategy_file_bak = "{}.ipynb".format(self.dag_id)
        # 此文件通过 bigjupyteruserservice 移动到 aiflow dags 目录下
        self.strategy_file_bak = os.path.join(self.base_work_path, strategy_file_bak)
        shutil.copyfile(os.path.join(self.base_work_path, strategy_file), self.strategy_file_bak)

        self.execute_strategy_file = os.path.join(self.aiflow_job_pod_user_strategy_folder, strategy_file_bak)

    @property
    def _import_text(self):
        return """
import datetime

from airflow.models import DAG

"""

    def _default_args_text(self, default_args):
        default_args_text = self.default_args_name + " = {\n"
        for k, v in default_args.items():
            if isinstance(v, str):
                default_args_text += """    "{}": "{}",\n""".format(k, v)
            elif isinstance(v, datetime.datetime):
                v = "datetime.datetime({}, {}, {}, {}, {}, {})".format(v.year, v.month, v.day, v.hour, v.minute, v.second)
                default_args_text += """    "{}": {},\n""".format(k, str(v))
            else:
                default_args_text += """    "{}": {},\n""".format(k, str(v))
        return default_args_text + "}\n"

    def _dag_text(self, dag_id, schedule_interval, catchup=False):
        self._check_schedule_interval(schedule_interval)
        return """
{} = DAG('{}', schedule_interval='{}', catchup={}, default_args={}, description='用户定时任务')
""".format(
            self.dag_name, dag_id, schedule_interval, catchup, self.default_args_name
        )

    def gen_dag_file(self, default_args, schedule_interval):
        """
        生成dag文件

        Args:
            dag_id: str
            task_id: str
            default_args: dict
            schedule_interval: str
        """
        # jupyter nbconvert --execute --to notebook --inplace /home/bigquant/work/test.ipynb

        cmd = "{base_cmd} {strategy_file}".format(base_cmd=jupyter_base_cmd, strategy_file=self.execute_strategy_file)

        cmds = ["bash", "-cx", cmd]
        volume_mounts = self.gen_volume_mounts()

        env_var_map = self.gen_env_vars()

        dag_content_list = [
            self._import_text,
            self._default_args_text(default_args=default_args),
            self._dag_text(self.dag_id, schedule_interval),
            self._kube_aiflow_operator_text(self.task_id, cmds, volume_mounts, env_var_map, default_namespace, node_selector_user_box),
        ]
        self._write_dag_content(dag_content_list)
        return self.dag_file_path

    def gen_kube_dag_file(
        self, default_args, schedule_interval, env_vars=None, namespace=default_namespace, node_selectors=None, image="", volume_mounts=None
    ):

        cmd = "{base_cmd} {strategy_file}".format(base_cmd=jupyter_base_cmd, strategy_file=self.execute_strategy_file)

        cmds = ["bash", "-cx", cmd]
        volume_mounts_map = self.gen_volume_mounts(custom_volume_mounts=volume_mounts)

        env_var_map = self.gen_env_vars(custom_env_map=env_vars)

        node_selectors = node_selectors or node_selector_user_box

        dag_content_list = [
            self._import_text,
            self._default_args_text(default_args=default_args),
            self._dag_text(self.dag_id, schedule_interval),
            self._kube_aiflow_operator_text(self.task_id, cmds, volume_mounts_map, env_var_map, namespace, node_selectors, image),
        ]
        self._write_dag_content(dag_content_list)
        return self.dag_file_path

    def _kube_aiflow_operator_text(self, task_id, cmds, volume_mounts, env_vars, namespace, node_selectors, image="", is_delete_operator_pod=True):
        # if not image:
        #     image = os.getenv("JUPYTER_IMAGE_SPEC", "")
        return """
from airflow.contrib.operators.kube_aiflow_operator import KubeAiflowOperator

KubeAiflowOperator(
        task_id='{task_id}',
        cmds={cmds},
        volume_mounts={volume_mounts},
        env_vars={env_vars},
        dag={dag_name},
        namespace='{namespace}',
        node_selectors={node_selectors},
        image='{image}',  # image为空时，默认从环境变量中获取最新的userbox镜像, AIRFLOW__KUBE_OPERATOR__IMAGE_USER_BOX
        is_delete_operator_pod={is_delete_operator_pod}
    )
""".format(
            task_id=task_id,
            cmds=cmds,
            volume_mounts=volume_mounts,
            env_vars=env_vars,
            dag_name=self.dag_name,
            namespace=namespace,
            node_selectors=node_selectors,
            image=image,
            is_delete_operator_pod=is_delete_operator_pod,
        )

    def _write_dag_content(self, dag_content_list):
        if os.path.exists(self.dag_file_path):
            os.remove(self.dag_file_path)
        with open(self.dag_file_path, "a", encoding="utf-8") as file:
            for content in dag_content_list:
                file.write(content)

    @staticmethod
    def _check_schedule_interval(schedule_interval):
        schedule_values = ["@daily", "@once", "@hourly", "@weekly", "@monthly", "@yearly"]
        if schedule_interval in schedule_values or len(schedule_interval.split()) == 5:
            return True
        raise Exception("您的定时设置错误，您可以参考Linux的cron语法或者使用 @daily / @once / @hourly / @weekly / @monthly / @yearly !")

    def gen_volume_mounts(self, custom_volume_mounts=None):
        # 生成挂载目录  {key(pod目录) : value(host目录)}
        result = {
            self.aiflow_job_pod_dag_path: "pvc://aiflow-dags",
        }

        vm_map = {}
        for vm in volume_mounts:
            # {'name': 'bqranker', 'mountPath': '/var/app/data/bqranker'},
            name = vm.get("name")
            mount_path = vm.get("mountPath")
            vm_map[name] = mount_path

        for v in volumes:
            # {'name': 'bqranker', 'hostPath': {'path': '/var/app/nfsdata/bqranker'}},
            name = v.get("name")
            host_path = v.get("hostPath").get("path")
            if "{username}" in host_path:
                host_path = host_path.format(username=self.username)
            result[vm_map.get(name)] = host_path
        if custom_volume_mounts:
            result.update(custom_volume_mounts)
        return result

    def gen_env_vars(self, custom_env_map=None):
        result = {}
        for env_name in self.env_keys:
            result[env_name] = os.getenv(env_name)
        if custom_env_map:
            result.update(custom_env_map)
        result[aiflow_job_key] = "True"
        return result


def skip_current_operator_module(log, run_now=False):
    """检查是否运行定时任务模块"""
    # 在aiflow woker中没有jpy_user_name  kube aiflow pod 环境变量中会设置aiflow_job_key
    if not jpy_user_name or not run_now or os.getenv(aiflow_job_key):
        log.info("跳过此模块! (没有选择 [即时执行] 或者不在策略开发环境中运行)")
        return True

    return False
