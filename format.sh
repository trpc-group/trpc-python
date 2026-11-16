#!/bin/bash

python3 -m pip install --upgrade pip

pip3 install yapf


# 查找并格式化 Python 文件
find trpc_sdk/ -name "*.py" -not -path "./.venv/*" \
                    -not -path "trpc_sdk/_inner/pb/*"  \
                    -exec yapf -i {} \;



# find ./examples/helloworld/ -type f -name "*.py" -not -path "*/__pycache__/*" -not -name "helloworld_pb2.py" -exec yapf -i {} \;
# find ./examples/hellostream/ -type f -name "*.py" -not -path "*/__pycache__/*" -not -name "helloworld_pb2.py" -exec yapf -i {} \;

find trpc/  -type f -name "*.py" -not -path "*/__pycache__/*" -not -name "*_pb2.py" -exec yapf -i {} \;
