# hellotrt 示例说明

一元 tRPC。`SayHello` 在处理这条请求的 serving 循环上做一次 CPU ResNet50 前向。`SayHi` 把同一张图交给 task `normal_task`，task 进程把一批图叠成一次前向。服务名 `trpc.trpc_python_group.hellotrt.Greeter`，默认监听 `127.0.0.1:8000`，`tcp` / `trpc`，服务端 timeout `100000` 毫秒。

输入是 key `input` 的 `DT_FLOAT` 张量，形状 `[3, 224, 224]`，数值在 0 到 1，NCHW。会话里做 ImageNet 的 mean/std。输出是 key `logits` 的 `[1000]`。不再回显输入。

权重用 torchvision 的 `ResNet50_Weights.IMAGENET1K_V1`。第一次 `Session.load` 下载约 98MB 到 `~/.cache/torch/hub/checkpoints/`，不进仓库。每个 serving 循环和 task 进程各自再加载一份模块。

## 关键特性

- `SayHello` 用 `current_handler_thread().state["session"]` 做单张前向
- `SayHi` 调用 `send_to_task`。task handler 最多收 5 张图，`infer_batch` 做一次 `forward`，按原顺序返回
- `eval` 下 BatchNorm 用固定统计量，所以一批和逐张的 logits 一致（绝对误差 `1e-3`）
- peer 不调用 `register_task`。`SayHi` 在这个布局上直接报错。拒绝反例仍是 helloworld `features/peer` 的 `TASK=1`

## 五种布局

| 检查名 | 配置 | 进程模型 | 端口 | `SESSION_LOAD` | task |
| --- | --- | --- | --- | --- | --- |
| `merge` | [trpc_python.yaml](trpc_python.yaml) | 单循环 merge，`task_num: 1` | `8000` | 1 | 有 |
| `single_process_merge` | [conf/single_process_merge.yaml](conf/single_process_merge.yaml) | 多线程 merge，`io_num` / `handle_num` 为 3 | `8110` | 3 | 有 |
| `single_process_separate` | [conf/single_process_separate.yaml](conf/single_process_separate.yaml) | separate-thread，`io_num: 2`，`handle_num: 3` | `8120` | 3（只在 handle 线程） | 有 |
| `multi_process_separate` | [conf/multi_process_separate.yaml](conf/multi_process_separate.yaml) | 多进程 separate，`handle_num: 2` | `8130` | 2（每个 rpc task） | 有 |
| `multi_process_merge` | [conf/multi_process_merge.yaml](conf/multi_process_merge.yaml) | peer，`handle_num: 2` | `8140` | 2（每个 worker） | 无 |

有 task 的四份配置写了 `server.io_wait_count: 1`。task 的 initializer 在权重进内存并打印 `TASK_SESSION <pid>` 之后调用 `inc_io_wait_count`，端口在这之前不监听。

`SESSION_LOAD <index> <count> <pid>` 打在 hook 里，打在权重已经进内存之后。peer 的日志里没有 `TASK_SESSION`。

## 关键文件

- [trpc_main.py](trpc_main.py)：`--conf` / `-c`，默认旁边的 [trpc_python.yaml](trpc_python.yaml)。`serve()` 前按 YAML 判断 peer，再注册 hook；非 peer 才 `register_task`
- [session.py](session.py)：`Session.load` / `infer` / `infer_batch`
- [greeter.py](greeter.py)：`SayHello`、`SayHi`
- [test_service_rpc.py](test_service_rpc.py)：`--port`，默认 `8000`。不启动服务，也不 `TrpcConfig.load`
- [check_modes.py](check_modes.py)：五种布局各跑一遍
- [requirements.txt](requirements.txt)：本示例额外需要 `torch` 和 `torchvision`，不进框架依赖

## 调用关系

```text
test_service_rpc.py / check_modes.py
└── :<port>  trpc.trpc_python_group.hellotrt.Greeter
    ├── SayHello
    │   └── 本 serving 循环的 ResNet50
    └── SayHi                    （peer 不调用）
        └── normal_task
            └── task 进程里的 ResNet50，一批一次 forward
```

## 关键代码解释

直接跑 `python trpc_main.py` 和在仓库根目录跑 `python -m examples.hellotrt.trpc_main` 都会把本目录和 `stub/` 放进 `sys.path`。

`register_task` 不在 import 时执行。peer 配置如果在 fork 前注册了 task，会被框架拒绝。本示例的 peer 要能启动，所以读到 `multi_process` + `io_handle_type: merge` 时不注册。

hook 在 `serve()` 前注册，在 fork 之后、每个 serving 循环自己的线程上执行。父进程在 `serve()` 前不调用 `Session.load`。`trpc_main.py` 在导入 torch 之前把 `OMP_NUM_THREADS` 和 `MKL_NUM_THREADS` 设为 `1`。

客户端用一份确定性的 `[3, 224, 224]` 图。有 task 的布局同时打 `SayHello` 和 `SayHi`，两边都要是 1000 维，并且 `allclose`（绝对误差 `1e-3`）。检查脚本只发一张图，task 里就是长度为 1 的批。

`register_static("/static", <仓库根>/docs)` 仍然注册。客户端不访问 `/static`。

## 环境要求

- Python `>=3.8`（[pyproject.toml](../../pyproject.toml) 的 `requires-python`）
- macOS 或 Linux
- C 编译工具链（`clang` 或 `gcc`）。可编辑安装会编译 Cython 扩展
- 本示例额外需要 `torch` 和 `torchvision`（CPU 即可）。首次运行需要能访问 PyTorch 的权重地址

在仓库根目录：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e .
pip install torch torchvision
```

安装说明见仓库 [README.zh_CN.md](../../README.zh_CN.md)。`uv sync` 和 `./build.sh` 也在那里。

[requirements.txt](requirements.txt) 里其余旧依赖和当前代码的 `trpc_sdk` 导入不一致。跑这个示例以上面的可编辑安装加上 `torch` / `torchvision` 为准。

## 运行步骤

每个终端先 `source .venv/bin/activate`。`trpc_main` 会一直占用当前终端，客户端另开一个；`check_modes` 自己拉起服务。都在仓库根目录：

```bash
python -m examples.hellotrt.trpc_main
python -m examples.hellotrt.test_service_rpc
python -m examples.hellotrt.check_modes
```

换布局：

```bash
python -m examples.hellotrt.trpc_main --conf examples/hellotrt/conf/single_process_merge.yaml
python -m examples.hellotrt.test_service_rpc --port 8110
```

`check_modes` 一次只起一份布局，就绪等待 120 秒。

## 运行结果

有 task 的布局，客户端打印：

```text
  test_async_say_hello logits 1000

  test_async_say_hi logits 1000

allclose ok
```

peer 只打印 `SayHello` 那一行。`check_modes` 每种布局打印 `ok <name>`，五份都通过时打印 `all layouts passed`。

服务日志里有 `SESSION_LOAD`。有 task 的布局还有一条 `TASK_SESSION`。

## 适用场景

- 看 `register_handler_thread_hook` 怎么让每个 serving 循环各持有一份模型
- 看 `send_to_task` 把多张图合成一次前向
- 看 peer 为什么不能注册 task，同时服务本身仍能启动

## 需要注意什么

1. 端口 `8000` 和 `8110`–`8140` 要空闲。多线程那一档大约是 3 份循环会话加 1 份 task 会话
2. 第一次运行会下载权重。缓存缺失且没有网络时，`on_start` 失败，进程退出
