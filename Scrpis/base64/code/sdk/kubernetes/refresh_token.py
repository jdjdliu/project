import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import requests
import rsa
from kubernetes import client as kube_client
from sdk.common import base64

from .settings import K8S_AUTH_HOST, K8S_PASSWORD, K8S_USERNAME, K8S_API_KEY_REFRESH_INTERVAL


def refresh_api_key_hook(configuration: kube_client.Configuration) -> None:
    current_token: str = configuration.api_key.get("authorization", None)

    if current_token is not None:
        if current_token.startswith("Bearer "):
            current_token = current_token[7:]

        _, payload_str, _ = current_token.split(".")

        payload = json.loads(base64.urlsafe_b64decode(payload_str))
        expired_at = datetime.fromtimestamp(payload["exp"])

        now = datetime.now()

        if not now > (expired_at - timedelta(hours=K8S_API_KEY_REFRESH_INTERVAL)):
            return

    token = refresh_function(
        host=K8S_AUTH_HOST.split("://")[1],
        username=K8S_USERNAME,
        password=K8S_PASSWORD,
        proto=K8S_AUTH_HOST.split("://")[0],
        auth="local",
    )

    configuration.api_key = {"authorization": f"Bearer {token}"}


def refresh_function(
    host: str,
    username: str,
    password: str,
    proto: str = "https",
    auth: str = "local",
    http_proxy: Optional[str] = None,
) -> Any:
    if http_proxy:
        proxy: Optional[Dict[str, str]] = {"http": http_proxy, "https": http_proxy}
    else:
        proxy = None
    headers = {"Referer": "{proto}://{host}/console-acp".format(proto=proto, host=host)}
    r1 = requests.get(
        "{proto}://{host}/console-acp/api/v1/token/login".format(proto=proto, host=host), headers=headers, proxies=proxy, timeout=10, verify=False
    )
    auth_url = r1.json().get("auth_url")
    state = r1.json().get("state")
    r2 = requests.get(auth_url, headers=headers, proxies=proxy, timeout=10, verify=False)
    req = re.findall(r"/dex/auth/{auth}\?req=(\w{1,})".replace("{auth}", auth), r2.text)[0]
    ret = requests.get("{proto}://{host}/dex/pubkey".format(proto=proto, host=host), proxies=proxy, verify=False)
    content = ret.json()
    ts_num = content.get("ts")
    pub_key = rsa.PublicKey.load_pkcs1_openssl_pem(content.get("pubkey"))
    data = {"ts": ts_num, "password": password}
    crypto = rsa.encrypt(json.dumps(data).encode("utf8"), pub_key)
    pwd = str(base64.b64encode(crypto), "utf8")
    r3 = requests.post(
        "{proto}://{host}/dex/auth/{auth}?req={req}".format(auth=auth, proto=proto, host=host, req=req),
        params={"req": req, "login": username, "encrypt": pwd},
        headers=headers,
        proxies=proxy,
        timeout=10,
        verify=False,
    )
    print(r3.history)
    code = re.findall(r"code=(\w{1,})", r3.history[1].text)[0]
    r4 = requests.get(
        "{proto}://{host}/console-acp/api/v1/token/callback".format(proto=proto, host=host),
        params={"code": code, "state": state},
        headers=headers,
        proxies=proxy,
        timeout=10,
        verify=False,
    )
    return r4.json().get("id_token")
