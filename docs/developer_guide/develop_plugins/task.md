English | [中文](task.zh_CN.md)

# How to Write a Task Plugin

In tRPC-Python, a task plugin is a set of child processes for background batch computation. A worker that handles network requests can send work that blocks, or that should be batched, such as model inference or CPU-heavy computation, to a task child process. The task process calls the user function on a batch, then returns each result to the worker asynchronously.

A task plugin differs from an ordinary plugin in a few ways:

- **It does not subclass `BasePlugin`.** It has no `init()` / `start()` lifecycle. Register it with [`register_task`](../../../trpc_sdk/plugin/_registry.py). That function always uses `PluginType.TASK`, but `PluginSpec.cls` is `None`, so `get_plugin(PluginType.TASK, name)` does not return a callable object.
- **There is no `plugins.task` configuration.** A task exists only if `register_task` ran when the module was imported. The service entry must import the module that defines the task before it calls `serve()`.
- **It is not a timer or an RPC child process.** A scheduled job uses `TimerTask`. An RPC child process under `multi_process` + `separate` is a network worker. `register_task` is for a batch-computation child process, or for other compute-heavy work.

The data path is:

```mermaid
flowchart LR
  worker[Worker handler coroutine] --> send["send_to_task"]
  send --> queue[Task request queue]
  queue --> consumer[TaskConsumer thread]
  consumer --> handler["handler(batch)"]
  handler --> rspQueue[Response queue]
  rspQueue --> future[Worker Future]
```

## 1. Register a Batch Task

The public type is [`trpc_sdk.abc.TaskInfo`](../../../trpc_sdk/abc/_task_info.py). The function is `trpc_sdk.plugin.register_task`:

```python
from trpc_sdk.abc import TaskInfo
from trpc_sdk.plugin import register_task


def handler(reqs):
    return [req.upper() for req in reqs]


def initializer():
    return None


register_task(
    "normal_task",
    TaskInfo(
        name="normal_task",
        handler=handler,
        initializer=initializer,
        batch_size=5,
    ),
)
```

`register_task` overwrites `task_info.name` with the name passed as the first argument. Keep the two strings the same.

### The `handler` Contract

[`TaskConsumer`](../../../trpc_sdk/task/_consumer.py) requires the following of `handler`:

1. **The output list matches the input, index by index.** `reqs` is a `list`, and the return value must be a `list` of the same length, with each index corresponding to the input. If the return value is not a `list`, the lengths differ, or `handler` raises, every caller in that batch receives `TaskHandleError`.
2. **It may be a synchronous function or a coroutine function.** The consumer thread uses `inspect.iscoroutinefunction` to choose the synchronous or asynchronous entry, and the thread has its own event loop. Do not use the worker process's event loop inside `handler`.
3. **It receives business objects directly.** Each element of the batch is the `req` passed to `send_to_task`. The framework serializes and deserializes across processes with the task codec. The business function does not handle bytes itself.

### `initializer` and `TaskInfo` Fields

- **`handler` (required):** the batch function.
- **`initializer` (optional):** called once in each task child process, after `fork` and before it starts receiving requests from the pipe. See [`TaskProcess._prepare_to_serve`](../../../trpc_sdk/runtime/_process/_task_process.py). This is the place to load a model or initialize other process-local resources. It runs in the task child process, not in the worker. If it raises, that task child process does not start.
- **`batch_size`:** the maximum number of requests in one batch for this task. When `<= 0`, the server setting `server.task_batch_size` is used instead.
- **`task_num`:** how many task child processes to start. The default is `1`. With more than one, the process names look like `normal_task:0` and `normal_task:1`. `send_to_task` still uses the registered name `"normal_task"`. The framework chooses the connection.
- **`pause_heartbeat`:** default `False`. When `True`, [`build_task_infos`](../../../trpc_sdk/runtime/_process/_task_process.py) pauses naming-service heartbeats before `fork` and resumes them after the child process's `initializer` returns. Turn this on only when the service should become discoverable after the task child has finished initializing. Ordinary cases keep the default `False`.

Only worker-side processes can send to a task. Task processes cannot call each other.

## 2. How the Task Child Process Starts

When the service starts, the framework brings up and connects the task child processes in this order:

1. **Register the task.** Importing the service entry runs `register_task`, which writes the task into the registry. `get_registered_tasks()` can list them.
2. **Build the process topology.** After `serve()`, and before `fork`, `_spawn_children()` runs. `build_task_infos()` makes one process record for each `task_num`, and calls `pause_registry_heartbeat()` when needed.
3. **Create the inter-process pipes.** `TaskIpcMesh` creates the worker-to-task `socketpair` pipes before `fork`.
4. **Start the child and initialize plugins.** After `fork`, the `TaskProcess` child calls `task_init(queue_size)` to set up the underlying queue, then `init_task_plugins()`. That call initializes only plugins marked `PluginInitPos.TASK` that are not `USER_DEFINED`.
5. **Run the consumers.** The child closes unrelated file descriptors, calls `initializer()`, then starts `TaskConsumer` threads according to `server.task_consumer_num`, and runs `consume_response` on the main event loop to send results back. At the same time, the worker calls `register_task_conn` on its own service event loop to register the task connections.

## 3. Submit a Task from Business Code

Inside a worker RPC handler, `await` [`send_to_task(ctx, task_name, req)`](../../../trpc_sdk/task/_task.py) to send the request to a task child process:

```python
from trpc_sdk.task import send_to_task


async def SayHello(self, ctx, request):
    rsp = await send_to_task(ctx, "normal_task", request.message)
    return rsp
```

- **How a batch forms.** `TaskConsumer` takes at most `batch_size` requests from the queue and passes that list to `handler`. `batch_size` is an upper bound: a single waiting request is still handled as its own batch. When the queue is empty, the consumer thread sleeps `task_wait_interval` milliseconds and checks again.
- **Common errors:**
  - `RuntimeError`: the current thread has no task connection table. `send_to_task` must run on the worker's service event loop. When `handle_thread_num > 0`, a separate I/O thread has no task pipe and cannot call it.
  - `TaskHandleError`: `task_name` is not registered (the resolved connection is `None`), or `handler` in the task child process raises or returns a list of the wrong length.

## 4. Server Configuration (`trpc_python.yaml`)

Queue and consumer settings live under `server`, not under `plugins`. The defaults are in [`trpc_sdk/config/_types.py`](../../../trpc_sdk/config/_types.py) and [`_constants.py`](../../../trpc_sdk/config/_constants.py):

```yaml
server:
  task_batch_size: 1
  task_wait_interval: 5
  task_queue_size: 1000
  task_consumer_num: 1
```

- `task_batch_size`: the global default batch limit, default `1`. **It applies only when that task's `TaskInfo.batch_size <= 0`.** A positive value set at registration, such as `batch_size=5` in the example above, wins over this setting.
- `task_wait_interval`: how long the consumer thread sleeps when the task queue is empty, default `5`, in milliseconds.
- `task_queue_size`: the request-queue capacity of each task child process, default `1000`.
- `task_consumer_num`: how many `TaskConsumer` threads each task child process starts, default `1`.
