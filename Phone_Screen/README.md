该项目由fastapi编写
运行先安装fastapi
pip install fastapi
pip install "uvicorn[standard]"

启动命令：
uvicorn alpha:app --host 0.0.0.0 --port 8000  --reload


数据接口访问缓慢    # x = requests.get('https://brain-cdn.s3-us-west-1.amazonaws.com/develop/static/brus/review.json')
故导出为review.json测试


api文档地址为   http://localhost:8000/Phone_Screen/docs#/