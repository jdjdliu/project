from aiohttp import request
from proto import hello_pb2

request = hello_pb2.HelloRequest()
request.name = 'baby'
# 序列化和反序列化
res_str = request.SerializeToString()
print(res_str)
# 通过字符串反向生成对象
request2 = hello_pb2.HelloRequest()
request2.ParseFromString(res_str)
print(request2.name)




 #比json好一倍左右的压缩比
print(len(res_str))
res_json = {
    'name' : 'baby'
}
import json

print(len(json.dumps(res_json)))
