import grpc
from grpc_hello import helloworld_pb2,helloworld_pb2_grpc

if __name__ == '__main__':
    with grpc.insecure_channel("10.24.20.25:50051") as channel:
        # 创建本地代理
        stub = helloworld_pb2_grpc.GreeterStub(channel)
        rsp: helloworld_pb2.HelloReply = stub.SayHello(helloworld_pb2.HelloRequest(name = 'babsdfby'))
        print(rsp.message)
        