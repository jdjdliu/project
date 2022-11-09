from aiohttp import request
from proto import hello_pb2

request = hello_pb2.HelloRequest()
request.name = 'baby'
# 序列化和反序列化  可变长编码
res_str = request.SerializeToString()
print(len(res_str))
res_json = {
    'name' : 'baby'
}
import json
print(len(json.dumps(res_json)))