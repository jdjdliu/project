## 支持不同环境的配置


### 设计原理

配置来源：

- 通用配置文件：我们一般通过 settings.py 为服务或者模块配置参数
- 基于环境的配置：通过环境变量传入。环境变量通过 k8s configmap传入 (TODO)

### 使用示例

#### foo/bar/settings.py

```
hello = 'world'

# load env settings, e.g. bigjupyteruser__service__settings__current_version=abcd python3 server.py
from bigshared.bigsettings import settings_from_env
settings_from_env(__name__, globals())
```

#### configmap 示例

TODO
