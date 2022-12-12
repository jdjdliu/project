# 在alpha的pod中执行， 有些配置需要配置， 见settings
import json
import sys
from typing import List, Type
from uuid import UUID

import requests
from tortoise import Tortoise, run_async

from alpha.constants import AuditStatus
from alpha.models import Audit, Catalog, Repo
from settings import ALPHA_HOST, HEADERS, PUBLIC_REPO, TORTOISE_ORM


async def init_tortoise():
    await Tortoise.init(TORTOISE_ORM)


run_async(init_tortoise())


class AuditMethod:
    @classmethod
    def reset_audit_status(cls: Type["AuditMethod"], alpha_id: int, audit_id: UUID):
        url = f"{ALPHA_HOST}/api/alpha/audit/{audit_id}"
        s = requests.put(
            url,
            headers=HEADERS,
            json={
                "id": str(audit_id),
                "status": AuditStatus.PENDING,
            },
        )
        if not s.status_code == 200:
            print(s.text)
        else:
            print(f"{cls.__name__}.{sys._getframe().f_code.co_name}  | {alpha_id} 的因子库状态已更改")

    @classmethod
    async def get_audit_id(cls: Type["AuditMethod"], alpha_id: int):
        repo = await Repo.get(name=PUBLIC_REPO).only("id")
        audit = await Audit.get(alpha_id=alpha_id, repo_id=repo.id).only("id")
        return audit.id


class CatalogMethod:
    @classmethod
    async def clear_catalog(cls: Type["CatalogMethod"]):
        await Catalog.all().delete()
        print(f"{cls.__name__}.{sys._getframe().f_code.co_name}  | Catalog 已删除完毕.")

    @classmethod
    def get_catalog_alphas(cls: Type["CatalogMethod"]) -> List:
        url = f"{ALPHA_HOST}/api/alpha/catalog/alpha"
        s = requests.get(url, headers=HEADERS)
        alphas = []
        [alphas.extend(i["alphas"]) for i in json.loads(s.text)["data"]]
        return alphas


class AlphaMethod:
    @classmethod
    async def del_catalog_alphas(cls: Type["AlphaMethod"]):
        alphas = CatalogMethod.get_catalog_alphas()
        alpha_ids = [alpha["id"] for alpha in alphas]
        for alpha_id in alpha_ids:
            url = f"{ALPHA_HOST}/api/alpha/alpha/{alpha_id}"
            audit_id = await AuditMethod.get_audit_id(alpha_id=alpha_id)
            AuditMethod.reset_audit_status(alpha_id=alpha_id, audit_id=audit_id)
            s = requests.delete(url, headers=HEADERS)
            if s.status_code == 200:
                print(f"{cls.__name__}.{sys._getframe().f_code.co_name}  | Alpha[{alpha_id}] 已删除.\n")

        await CatalogMethod.clear_catalog()


if __name__ == "__main__":
    run_async(AlphaMethod.del_catalog_alphas())
