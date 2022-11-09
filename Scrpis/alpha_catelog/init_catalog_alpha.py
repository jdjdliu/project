# 在alpha的pod中执行， 有些配置需要配置， 见settings
import json
import sys
from typing import Type

import requests
from sdk.task import TaskStatus
from tortoise import Tortoise, run_async

from alpha.constants import AuditStatus
from alpha.models import Alpha, Audit, Repo
from alpha.schemas import BacktestSchema, CompositionAlpha, CreateAlphaFromBacktestRequest, CreateBacktestRequest
from settings import (
    ALPHA_HOST,
    ALPHA_PARAMETER,
    HEADERS,
    PUBLIC_REPO,
    TASK_HOST,
    TORTOISE_ORM,
    alpha_file,
    alpha_name,
    backtest_file,
    creator_id,
    final_mapping,
)


class BacktestMethod:
    @classmethod
    def create_backtest(cls: Type["BacktestMethod"]):
        create_url = f"{ALPHA_HOST}/api/alpha/backtest/composition"
        backtest_ids = {}
        for expression, name in alpha_name.items():
            data = CreateBacktestRequest(
                name=name,
                column="alpha",
                alphas=[CompositionAlpha(alpha=expression, weight=1)],
                parameter=ALPHA_PARAMETER,
                alpha_type="ALPHA",
                product_type="STOCK",
            )
            s = requests.post(create_url, data=data.json(), headers=HEADERS)
            backtest = json.loads(s.text)["data"]

            if not final_mapping.get(expression):
                continue

            cls._update_task_stats(backtest["task_id"])

            _bk = {
                "id": backtest["id"],
                "expression": expression,
                "name": name,
                "catalog_id": final_mapping.get(expression),
                "task_id": json.loads(s.text)["data"]["task_id"],
            }
            backtest_ids[backtest["id"]] = _bk
            print(f"{cls.__name__}.{sys._getframe().f_code.co_name}  |  {backtest['id']}, {name} 已写入 ‘构建中的因子’.")

        with open(backtest_file, "w") as f:
            f.write(json.dumps(backtest_ids))

    @classmethod
    def _update_task_stats(cls, task_id):
        update_task_url = f"{TASK_HOST}/api/task/task/{task_id}/status"
        data = {
            "id": task_id,
            "status": TaskStatus.SUCCESS,
        }
        s = requests.put(update_task_url, headers=HEADERS, json=data)
        if not s.status_code == 200:
            print(s.text)

    @classmethod
    def delete_backtest(cls: Type["BacktestMethod"]):
        with open(backtest_file, "r") as f:
            backtests = json.loads(f.read())

        backtests_id = [int(key) for key in backtests]
        print(f"{cls.__name__}.{sys._getframe().f_code.co_name}  | Backtest: {backtests_id}, 已删除 .")

        url = f"{ALPHA_HOST}/api/alpha/backtest"

        requests.delete(url, headers=HEADERS, json={"backtest_ids": backtests_id})


class CatalogMethod:
    INSERT_SQL = """
        BEGIN;
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('07b7f227-18f9-49e4-b924-484aad76033e', '2022-06-06 08:23:14.349339', '2022-06-06 08:23:14.349369', '{creator_id}', '质量因子', NULL);
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('399fcfce-094b-44ac-9891-e594acd68c33', '2022-06-06 08:23:14.203692', '2022-06-06 08:23:14.203721', '{creator_id}', '盈利因子', NULL);
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('6c8a48b8-1a08-4107-81be-68e3d190e008', '2022-06-06 08:23:14.508172', '2022-06-06 08:23:14.508200', '{creator_id}', '营运因子', NULL);
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('778a5e2b-f83a-4a4b-b330-3b4268763366', '2022-06-06 08:23:14.653122', '2022-06-06 08:23:14.653153', '{creator_id}', '每股因子', NULL);
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('7bc854a9-1aea-4488-843e-e8e106e6c4b3', '2022-06-06 08:23:14.278449', '2022-06-06 08:23:14.278478', '{creator_id}', '成长因子', NULL);
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('94cdc334-d51e-487b-8995-00e65957bd07', '2022-06-06 08:23:14.013960', '2022-06-06 08:23:14.013990', '{creator_id}', '技术分析因子', NULL);
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('9b86158f-27e8-4ade-b5fa-4ff71432a27f', '2022-06-06 08:23:13.883365', '2022-06-06 08:23:13.883396', '{creator_id}', '估值因子', NULL);
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('a731a4c5-9007-470a-a87a-42ea4be14d15', '2022-06-06 08:23:13.652428', '2022-06-06 08:23:13.652478', '{creator_id}', '股东因子', NULL);
        INSERT INTO `alpha__catalog` (`id`, `created_at`, `updated_at`, `creator`, `name`, `description`) VALUES ('f03bb289-a54d-4eaf-bb69-7bb9e87f3055', '2022-06-06 08:23:14.155149', '2022-06-06 08:23:14.155175', '{creator_id}', '波动率因子', NULL);
        COMMIT;
    """.format(
        creator_id=creator_id
    )

    @classmethod
    def main(cls: Type["CatalogMethod"]):
        run_async(cls.init_catalog())

    @classmethod
    async def init_catalog(cls: Type["CatalogMethod"]):
        """insert catalog"""
        print(f"{cls.__name__}.{sys._getframe().f_code.co_name}  | Catalog 已写入完毕.")
        await Tortoise.init(TORTOISE_ORM)
        await Tortoise.generate_schemas()
        conn = Tortoise.get_connection("default")
        await conn.execute_query(cls.INSERT_SQL)


class AlphaMethod:
    @classmethod
    def put_alpha(cls: Type["AlphaMethod"]):
        url = f"{ALPHA_HOST}/api/alpha/alpha/backtest"
        with open(backtest_file, "r") as f:
            backtest_mapping = json.loads(f.read())

        latest_backtest_mapping = {}
        alpha_ids = {}
        for id, item in backtest_mapping.items():
            cataloga_id = item.pop("catalog_id")
            # create alpha
            data = CreateAlphaFromBacktestRequest(name=item["name"].split("__")[0], backtest_id=id)
            s = requests.post(url, data=data.json(), headers=HEADERS)
            if not s.status_code == 200:
                print(s.text)
                latest_backtest_mapping[id] = item
                continue

            _alpha = json.loads(s.text)["data"]
            alpha_ids[_alpha["id"]] = cataloga_id
            print(f'{cls.__name__}.{sys._getframe().f_code.co_name}  | Alpha: {_alpha["name"]}: {_alpha["id"]} 已写入 ‘我的因子’')

        to_del_backtest = set(backtest_mapping.keys() - latest_backtest_mapping.keys())
        bulk_del_url = f"{ALPHA_HOST}/api/alpha/backtest"
        s = requests.delete(bulk_del_url, json=list(to_del_backtest), headers=HEADERS)

        with open(alpha_file, "w") as f:
            f.write(json.dumps(alpha_ids))

        run_async(cls.update_catalog(alpha_ids))

    @classmethod
    async def update_catalog(cls: Type["AlphaMethod"], alpha_mapping):
        await Tortoise.init(TORTOISE_ORM)
        for alpha_id, catalog_id in alpha_mapping.items():
            alpha = await Alpha.get(id=alpha_id)
            alpha.update_from_dict({"catalog_id": catalog_id})
            await alpha.save()

        repo = await Repo.get(name=PUBLIC_REPO)
        for alpha_id in alpha_mapping.keys():
            await Audit.create(
                alpha_id=alpha_id,
                repo_id=repo.id,
                status=AuditStatus.APPROVED,
                creator=creator_id,
                auditor=creator_id,
            )


if __name__ == "__main__":
    CatalogMethod.main()  # 初始化catalog
    BacktestMethod.create_backtest()  # 创建backtest
    AlphaMethod.put_alpha()  # 从backtest提交并更新catalog & 添加alpha到repo
    BacktestMethod.delete_backtest()  # 删除backtest
