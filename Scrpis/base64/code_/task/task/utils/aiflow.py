import json

import requests

from task.settings import AIFLOW_AUTH_PASSWORD, AIFLOW_AUTH_USER, AIFLOW_URL

__all__ = ["AIFlowAPI"]

headers = {"Content-Type": "application/json"}
auth = (AIFLOW_AUTH_USER, AIFLOW_AUTH_PASSWORD)


class AIFlowAPI:
    @staticmethod
    def last_dagrun_instance(dag_id: str, id: str):
        URL = f"{AIFLOW_URL}/dags/{dag_id}/dagRuns/{dag_id}_{id}"
        response = requests.get(URL, auth=auth, headers=headers)
        if response.status_code == 200:
            res = response.json()
            return {"task_id": res["task_id"], "state": res["state"]}
        else:
            raise Exception(response.status_code)

    @staticmethod
    def dagsrun(dag_id: str, payload: dict):
        data = json.dumps(payload) if payload else "{}"
        URL = f"{AIFLOW_URL}/dags/{dag_id}/dagRuns"
        response = requests.post(URL, auth=auth, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.status_code)
