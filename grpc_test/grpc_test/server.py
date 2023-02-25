from concurrent import futures

import grpc

from grpc_hello import helloworld_pb2, helloworld_pb2_grpc
 
 
class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def SayHello(self, request, context):
        return helloworld_pb2.HelloReply(message=f'你好,{request.name}')
    
if __name__ == '__main__':
    #1. 实例化server 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    #2. 注册逻辑到server中
    helloworld_pb2_grpc.add_GreeterServicer_to_server(Greeter(),server)
    #3. 启动server
    server.add_insecure_port('10.24.20.25:50051')
    server.start()
    server.wait_for_termination()
    