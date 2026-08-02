# -*- coding: utf-8 -*-
"""
Unit tests for BaseProfilerHandler, CpuProfilerHandler, HeapProfilerHandler
and Configer.debug() worker selection.
"""
import asyncio
import os
import tempfile
import tracemalloc
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import mock_open
from unittest.mock import patch

from trpc_sdk.admin.debug_handler.base_profiler_handler import BaseProfilerHandler
from trpc_sdk.admin.debug_handler.cpu_profiler_handler import CpuProfilerHandler
from trpc_sdk.admin.debug_handler.heap_profiler_handler import HeapProfilerHandler

# ---------------------------------------------------------------------------
# Helper: 最小可实例化子类
# ---------------------------------------------------------------------------

class _DummyHandler(BaseProfilerHandler):
    """仅记录调用，不做任何实际采集。"""

    def command_handle(self, params: dict, res: dict) -> None:
        res['errorcode'] = 0
        res['message'] = 'OK'
        res['params'] = dict(params)


class _RaisingHandler(BaseProfilerHandler):
    """command_handle 固定抛出异常，用于覆盖异常处理分支。"""

    def command_handle(self, params: dict, res: dict) -> None:
        raise RuntimeError('boom')


# ===========================================================================
# BaseProfilerHandler.get_duration
# ===========================================================================

class TestGetDuration:
    def setup_method(self):
        self.h = _DummyHandler()

    def test_missing_key_returns_minus1(self):
        assert self.h.get_duration({}) == -1

    def test_empty_string_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': ''}) == -1

    def test_non_digit_string_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': 'abc'}) == -1

    def test_float_string_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': '1.5'}) == -1

    def test_negative_string_returns_minus1(self):
        # '-100' 无法通过 isdigit() 检查
        assert self.h.get_duration({'duration_ms': '-100'}) == -1

    def test_zero_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': '0'}) == -1

    def test_valid_positive_integer(self):
        assert self.h.get_duration({'duration_ms': '500'}) == 500

    def test_minimum_valid_value(self):
        assert self.h.get_duration({'duration_ms': '1'}) == 1

    def test_whitespace_stripped(self):
        assert self.h.get_duration({'duration_ms': ' 200 '}) == 200


# ===========================================================================
# BaseProfilerHandler.send_file
# ===========================================================================

class TestSendFile:
    def setup_method(self):
        self.h = _DummyHandler()

    def test_reads_existing_file_successfully(self):
        content = '<svg>flame graph data</svg>'
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.svg', delete=False, encoding='utf-8'
        ) as f:
            f.write(content)
            path = f.name
        try:
            res = {}
            self.h.send_file(path, res)
            assert res['errorcode'] == 0
            assert res['message'] == 'OK'
            assert res['data'] == content
        finally:
            os.unlink(path)

    def test_nonexistent_file_sets_error(self):
        res = {}
        self.h.send_file('/nonexistent/__no_such_file__.svg', res)
        assert res['errorcode'] == -1
        assert 'open file failed' in res['message']


# ===========================================================================
# BaseProfilerHandler.handle (async)
# ===========================================================================

class TestHandleAsync:
    """通过 asyncio.run() 直接驱动协程，无需 HTTP 服务器。"""

    def test_rejects_concurrent_request(self):
        """锁被占用时应立即返回 in-progress 错误，不执行 command_handle。"""
        h = _DummyHandler()
        h._lock.acquire()
        try:
            mock_req = MagicMock()
            mock_req.query = {}
            res = asyncio.run(h.handle(mock_req))
            assert res['errorcode'] == -1
            assert 'in progress' in res['message']
        finally:
            h._lock.release()

    def test_normal_path_releases_lock_after_completion(self):
        """正常执行后锁应被释放，能够再次获取。"""
        h = _DummyHandler()
        mock_req = MagicMock()
        mock_req.query = {'duration_ms': '50'}
        res = asyncio.run(h.handle(mock_req))
        assert res['errorcode'] == 0
        # 确认锁已释放
        acquired = h._lock.acquire(blocking=False)
        assert acquired
        h._lock.release()

    def test_lock_released_even_when_command_handle_raises(self):
        """command_handle 抛出异常后，finally 块仍需释放锁。"""
        h = _RaisingHandler()
        mock_req = MagicMock()
        mock_req.query = {}
        res = asyncio.run(h.handle(mock_req))
        assert res['errorcode'] == -1
        # 确认锁已释放
        acquired = h._lock.acquire(blocking=False)
        assert acquired
        h._lock.release()


# ===========================================================================
# BaseProfilerHandler._run_command
# ===========================================================================

class TestRunCommand:
    def test_normal_execution_forwards_params(self):
        h = _DummyHandler()
        res = {}
        h._run_command({'duration_ms': '100'}, res)
        assert res['errorcode'] == 0
        assert res['params'] == {'duration_ms': '100'}

    def test_exception_in_command_handle_sets_errorcode(self):
        h = _RaisingHandler()
        res = {}
        h._run_command({}, res)
        assert res['errorcode'] == -1
        assert 'boom' in res['message']


# ===========================================================================
# CpuProfilerHandler.command_handle
# ===========================================================================

class TestCpuCommandHandle:
    def setup_method(self):
        self.h = CpuProfilerHandler()

    def test_bad_duration_missing_returns_error(self):
        res = {}
        self.h.command_handle({}, res)
        assert res['errorcode'] == -1
        assert 'duration_ms' in res['message']

    def test_bad_duration_zero_returns_error(self):
        res = {}
        self.h.command_handle({'duration_ms': '0'}, res)
        assert res['errorcode'] == -1
        assert 'duration_ms' in res['message']

    def test_no_child_pid_returns_error(self):
        res = {}
        with patch.object(self.h, '_pick_child_pid', return_value=None):
            self.h.command_handle({'duration_ms': '100'}, res)
        assert res['errorcode'] == -1
        assert 'no child' in res['message']

    def test_child_index_absent_passes_none_to_pick(self):
        """未提供 child_index 时，应以 child_index=None 调用 _pick_child_pid。"""
        with patch.object(self.h, '_pick_child_pid', return_value=None) as mock_pick:
            self.h.command_handle({'duration_ms': '100'}, {})
        mock_pick.assert_called_once_with(child_index=None)

    def test_child_index_non_digit_passes_none_to_pick(self):
        """child_index 为非数字时，应退避为 None 传入 _pick_child_pid。"""
        with patch.object(self.h, '_pick_child_pid', return_value=None) as mock_pick:
            self.h.command_handle({'duration_ms': '100', 'child_index': 'abc'}, {})
        mock_pick.assert_called_once_with(child_index=None)

    def test_child_index_negative_string_passes_none_to_pick(self):
        """child_index 为负数字符串时（isdigit 失败），应退避为 None。"""
        with patch.object(self.h, '_pick_child_pid', return_value=None) as mock_pick:
            self.h.command_handle({'duration_ms': '100', 'child_index': '-1'}, {})
        mock_pick.assert_called_once_with(child_index=None)

    def test_child_index_valid_digit_passes_int_to_pick(self):
        """child_index 为合法数字字符串时，应转为 int 传入 _pick_child_pid。"""
        with patch.object(self.h, '_pick_child_pid', return_value=None) as mock_pick:
            self.h.command_handle({'duration_ms': '100', 'child_index': '2'}, {})
        mock_pick.assert_called_once_with(child_index=2)

    def test_child_index_zero_passes_zero_to_pick(self):
        """child_index=0 应作为合法整数传入（选取第一个子进程）。"""
        with patch.object(self.h, '_pick_child_pid', return_value=None) as mock_pick:
            self.h.command_handle({'duration_ms': '100', 'child_index': '0'}, {})
        mock_pick.assert_called_once_with(child_index=0)

    def test_pyspy_failure_captures_stderr(self):
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stderr = 'permission denied'
        mock_proc.stdout = ''
        res = {}
        with patch.object(self.h, '_pick_child_pid', return_value=1234), \
             patch('subprocess.run', return_value=mock_proc):
            self.h.command_handle({'duration_ms': '100'}, res)
        assert res['errorcode'] == -1
        assert 'py-spy failed' in res['message']
        assert 'permission denied' in res['message']

    def test_pyspy_failure_falls_back_to_stdout_when_stderr_empty(self):
        """stderr 为空时应回退使用 stdout 作为错误信息。"""
        mock_proc = MagicMock()
        mock_proc.returncode = 2
        mock_proc.stderr = ''
        mock_proc.stdout = 'stdout fallback msg'
        res = {}
        with patch.object(self.h, '_pick_child_pid', return_value=1234), \
             patch('subprocess.run', return_value=mock_proc):
            self.h.command_handle({'duration_ms': '100'}, res)
        assert res['errorcode'] == -1
        assert 'stdout fallback msg' in res['message']

    def test_pyspy_success_returns_svg_data(self):
        """py-spy 成功时应将 SVG 内容写入 res['data']，data_type 为 'svg'。"""
        svg_content = '<svg>flame</svg>'

        def fake_run(cmd, **kwargs):
            output_path = cmd[cmd.index('--output') + 1]
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(svg_content)
            m = MagicMock()
            m.returncode = 0
            return m

        res = {}
        with patch.object(self.h, '_pick_child_pid', return_value=1234), \
             patch('subprocess.run', side_effect=fake_run):
            self.h.command_handle({'duration_ms': '100'}, res)
        assert res['errorcode'] == 0
        assert res['data_type'] == 'svg'
        assert res['data'] == svg_content

    def test_temp_file_cleaned_up_after_pyspy_failure(self):
        """无论 py-spy 成功与否，finally 块都应删除临时文件。"""
        created_paths = []
        original_mkstemp = tempfile.mkstemp

        def spy_mkstemp(*args, **kwargs):
            fd, path = original_mkstemp(*args, **kwargs)
            created_paths.append(path)
            return fd, path

        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stderr = 'fail'
        mock_proc.stdout = ''
        res = {}
        with patch.object(self.h, '_pick_child_pid', return_value=1234), \
             patch('subprocess.run', return_value=mock_proc), \
             patch('tempfile.mkstemp', side_effect=spy_mkstemp):
            self.h.command_handle({'duration_ms': '100'}, res)

        assert created_paths, '未捕获到 mkstemp 调用'
        for path in created_paths:
            assert not os.path.exists(path), f'临时文件未被清理: {path}'

    def test_temp_file_cleaned_up_after_pyspy_success(self):
        """py-spy 成功后临时文件同样应被删除。"""
        created_paths = []
        original_mkstemp = tempfile.mkstemp

        def spy_mkstemp(*args, **kwargs):
            fd, path = original_mkstemp(*args, **kwargs)
            created_paths.append(path)
            return fd, path

        def fake_run(cmd, **kwargs):
            output_path = cmd[cmd.index('--output') + 1]
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('<svg/>')
            m = MagicMock()
            m.returncode = 0
            return m

        res = {}
        with patch.object(self.h, '_pick_child_pid', return_value=1234), \
             patch('subprocess.run', side_effect=fake_run), \
             patch('tempfile.mkstemp', side_effect=spy_mkstemp):
            self.h.command_handle({'duration_ms': '100'}, res)

        assert created_paths, '未捕获到 mkstemp 调用'
        for path in created_paths:
            assert not os.path.exists(path), f'临时文件未被清理: {path}'


# ===========================================================================
# CpuProfilerHandler._pick_child_pid
# ===========================================================================

class TestPickChildPid:
    def _make_psutil(self, pids: list):
        """构造一个 mock psutil 模块，parent.children() 返回指定 pid 列表对应的 Process 对象。"""
        mock_children = []
        for p in pids:
            c = MagicMock()
            c.pid = p
            mock_children.append(c)
        mock_parent = MagicMock()
        mock_parent.children.return_value = mock_children
        mock_psutil = MagicMock()
        mock_psutil.Process.return_value = mock_parent
        return mock_psutil

    # ---- 无 child_index（随机选取）----

    def test_returns_pid_via_psutil_when_children_exist(self):
        """psutil 可用且有子进程时，应返回其中一个子进程的 PID。"""
        with patch.dict('sys.modules', {'psutil': self._make_psutil([9999])}):
            pid = CpuProfilerHandler._pick_child_pid()
        assert pid == 9999

    def test_returns_none_via_psutil_when_no_children(self):
        """psutil 可用但无子进程时，应返回 None。"""
        with patch.dict('sys.modules', {'psutil': self._make_psutil([])}):
            pid = CpuProfilerHandler._pick_child_pid()
        assert pid is None

    def test_proc_fallback_returns_child_pid(self):
        """/proc 回退路径：能正确读取 PPid 并返回子进程 PID。"""
        own_pid = os.getpid()
        child_pid = own_pid + 1000
        fake_status = f'Name:\tpython\nPPid:\t{own_pid}\nPid:\t{child_pid}\n'

        with patch.dict('sys.modules', {'psutil': None}), \
             patch('os.listdir', return_value=[str(child_pid), 'self']), \
             patch('builtins.open', mock_open(read_data=fake_status)):
            pid = CpuProfilerHandler._pick_child_pid()
        assert pid == child_pid

    def test_proc_fallback_returns_none_when_no_child_entries(self):
        """psutil 不可用，/proc 下无数字目录条目时返回 None。"""
        with patch.dict('sys.modules', {'psutil': None}), \
             patch('os.listdir', return_value=['self', 'net', 'abc']):
            pid = CpuProfilerHandler._pick_child_pid()
        assert pid is None

    def test_proc_fallback_returns_none_when_ppid_does_not_match(self):
        """status 文件中 PPid 与当前进程不同，不应被选中。"""
        other_pid = os.getpid() + 5000
        fake_status = f'Name:\tpython\nPPid:\t{other_pid}\n'

        with patch.dict('sys.modules', {'psutil': None}), \
             patch('os.listdir', return_value=['12345', 'self']), \
             patch('builtins.open', mock_open(read_data=fake_status)):
            pid = CpuProfilerHandler._pick_child_pid()
        assert pid is None

    def test_proc_fallback_returns_none_on_listdir_exception(self):
        """os.listdir 抛出意外异常时应安全返回 None，不向外传播。"""
        with patch.dict('sys.modules', {'psutil': None}), \
             patch('os.listdir', side_effect=Exception('unexpected')):
            pid = CpuProfilerHandler._pick_child_pid()
        assert pid is None

    # ---- child_index 按序号选取（psutil 路径）----

    def test_child_index_selects_first_child_via_psutil(self):
        """child_index=0 应返回第一个子进程的 PID。"""
        with patch.dict('sys.modules', {'psutil': self._make_psutil([1001, 1002, 1003])}):
            pid = CpuProfilerHandler._pick_child_pid(child_index=0)
        assert pid == 1001

    def test_child_index_selects_second_child_via_psutil(self):
        """child_index=1 应返回第二个子进程的 PID。"""
        with patch.dict('sys.modules', {'psutil': self._make_psutil([1001, 1002, 1003])}):
            pid = CpuProfilerHandler._pick_child_pid(child_index=1)
        assert pid == 1002

    def test_child_index_selects_last_child_via_psutil(self):
        """child_index=N-1 应返回最后一个子进程的 PID。"""
        with patch.dict('sys.modules', {'psutil': self._make_psutil([1001, 1002, 1003])}):
            pid = CpuProfilerHandler._pick_child_pid(child_index=2)
        assert pid == 1003

    def test_child_index_out_of_range_falls_back_to_random_via_psutil(self):
        """child_index >= 子进程数量时，应退避到随机选取（结果在 pids 列表内）。"""
        pids = [1001, 1002]
        with patch.dict('sys.modules', {'psutil': self._make_psutil(pids)}):
            pid = CpuProfilerHandler._pick_child_pid(child_index=99)
        assert pid in pids

    # ---- child_index 按序号选取（/proc 回退路径）----

    def test_child_index_selects_by_order_via_proc_fallback(self):
        """child_index=0 在 /proc 回退路径下应返回第一个枚举到的子进程 PID。"""
        own_pid = os.getpid()
        child_a, child_b = own_pid + 100, own_pid + 200
        fake_status = f'Name:\tpython\nPPid:\t{own_pid}\n'

        with patch.dict('sys.modules', {'psutil': None}), \
             patch('os.listdir', return_value=[str(child_a), str(child_b), 'self']), \
             patch('builtins.open', mock_open(read_data=fake_status)):
            pid = CpuProfilerHandler._pick_child_pid(child_index=0)
        assert pid == child_a

    def test_child_index_out_of_range_falls_back_to_random_via_proc(self):
        """child_index 越界时，/proc 路径同样退避到随机选取。"""
        own_pid = os.getpid()
        child_a, child_b = own_pid + 100, own_pid + 200
        fake_status = f'Name:\tpython\nPPid:\t{own_pid}\n'

        with patch.dict('sys.modules', {'psutil': None}), \
             patch('os.listdir', return_value=[str(child_a), str(child_b), 'self']), \
             patch('builtins.open', mock_open(read_data=fake_status)):
            pid = CpuProfilerHandler._pick_child_pid(child_index=99)
        assert pid in (child_a, child_b)


# ===========================================================================
# HeapProfilerHandler.get_duration
# ===========================================================================

class TestHeapGetDuration:
    """
    HeapProfilerHandler.get_duration 与 BaseProfilerHandler 的同名方法存在差异：
    它先将 raw 值通过 str() 转换，因此可以直接接受整数类型的 duration_ms。
    """

    def setup_method(self):
        self.h = HeapProfilerHandler()

    def test_missing_key_returns_minus1(self):
        assert self.h.get_duration({}) == -1

    def test_empty_string_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': ''}) == -1

    def test_non_digit_string_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': 'abc'}) == -1

    def test_float_string_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': '1.5'}) == -1

    def test_negative_string_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': '-100'}) == -1

    def test_zero_string_returns_minus1(self):
        assert self.h.get_duration({'duration_ms': '0'}) == -1

    def test_zero_int_returns_minus1(self):
        # 直接传入整数 0，str(0)='0'，val>0 不满足
        assert self.h.get_duration({'duration_ms': 0}) == -1

    def test_valid_string_returns_value(self):
        assert self.h.get_duration({'duration_ms': '500'}) == 500

    def test_valid_int_returns_value(self):
        # HeapProfilerHandler 通过 str(raw) 兼容整数类型
        assert self.h.get_duration({'duration_ms': 200}) == 200

    def test_minimum_valid_value(self):
        assert self.h.get_duration({'duration_ms': '1'}) == 1

    def test_whitespace_stripped(self):
        assert self.h.get_duration({'duration_ms': ' 300 '}) == 300


# ===========================================================================
# HeapProfilerHandler.handle (async)
# ===========================================================================

class TestHeapHandle:
    def test_rejects_concurrent_request(self):
        """锁被占用时应立即写入 in-progress 错误并返回，不进入 run_in_executor。"""
        h = HeapProfilerHandler()
        h._lock.acquire()
        try:
            res = {}
            asyncio.run(h.handle({}, res))
            assert res['errorcode'] == -1
            assert 'in progress' in res['message']
        finally:
            h._lock.release()

    def test_normal_path_releases_lock_after_completion(self):
        """_run 正常返回后锁应被释放。"""
        h = HeapProfilerHandler()
        with patch.object(h, '_run'):
            res = {}
            asyncio.run(h.handle({'duration_ms': '10'}, res))
        acquired = h._lock.acquire(blocking=False)
        assert acquired
        h._lock.release()

    def test_lock_released_when_run_raises(self):
        """run_in_executor 内部抛出异常时，finally 块仍需释放锁。"""
        h = HeapProfilerHandler()
        with patch.object(h, '_run', side_effect=RuntimeError('run error')):
            res = {}
            asyncio.run(h.handle({}, res))
        assert res.get('errorcode') == -1
        assert 'run error' in res.get('message', '')
        acquired = h._lock.acquire(blocking=False)
        assert acquired
        h._lock.release()


# ===========================================================================
# HeapProfilerHandler._run
# ===========================================================================

class TestHeapRun:
    def test_delegates_to_do_collect(self):
        """_run 应将 params 和 res 原样转发给 _do_collect。"""
        h = HeapProfilerHandler()
        with patch.object(h, '_do_collect') as mock_collect:
            res = {}
            h._run({'duration_ms': '100'}, res)
        mock_collect.assert_called_once_with({'duration_ms': '100'}, res)

    def test_do_collect_exception_sets_errorcode(self):
        """_do_collect 抛出异常时，_run 应捕获并将 errorcode=-1 写入 res。"""
        h = HeapProfilerHandler()
        with patch.object(h, '_do_collect', side_effect=RuntimeError('collect fail')):
            res = {}
            h._run({}, res)
        assert res['errorcode'] == -1
        assert 'collect fail' in res['message']


# ===========================================================================
# HeapProfilerHandler._do_collect
# ===========================================================================

class TestHeapDoCollect:
    def setup_method(self):
        self.h = HeapProfilerHandler()

    def test_bad_duration_sets_error(self):
        """duration_ms 非法时应写入 errorcode=-1，不调用 tracemalloc。"""
        res = {}
        with patch('tracemalloc.start') as mock_start:
            self.h._do_collect({}, res)
        assert res['errorcode'] == -1
        assert 'duration_ms' in res['message']
        mock_start.assert_not_called()

    def test_success_starts_and_stops_tracemalloc(self):
        """tracemalloc 未运行时，_do_collect 应 start，完成后 stop，并填充结果。"""
        mock_snap1 = MagicMock()
        mock_snap2 = MagicMock()
        mock_snap2.compare_to.return_value = ['stat_a', 'stat_b']

        with patch('tracemalloc.is_tracing', return_value=False), \
             patch('tracemalloc.start') as mock_start, \
             patch('tracemalloc.stop') as mock_stop, \
             patch('tracemalloc.take_snapshot', side_effect=[mock_snap1, mock_snap2]), \
             patch('time.sleep') as mock_sleep:
            res = {}
            self.h._do_collect({'duration_ms': '100'}, res)

        mock_start.assert_called_once()
        mock_stop.assert_called_once()
        mock_sleep.assert_called_once_with(0.1)
        mock_snap2.compare_to.assert_called_once_with(mock_snap1, 'lineno')
        assert res['errorcode'] == 0
        assert res['data_type'] == 'tracemalloc'
        assert res['data'] == ['stat_a', 'stat_b']
        assert res['message'] == 'OK'

    def test_success_does_not_restart_when_already_tracing(self):
        """tracemalloc 已在运行时，_do_collect 不应调用 start/stop。"""
        mock_snap1 = MagicMock()
        mock_snap2 = MagicMock()
        mock_snap2.compare_to.return_value = []

        with patch('tracemalloc.is_tracing', return_value=True), \
             patch('tracemalloc.start') as mock_start, \
             patch('tracemalloc.stop') as mock_stop, \
             patch('tracemalloc.take_snapshot', side_effect=[mock_snap1, mock_snap2]), \
             patch('time.sleep'):
            res = {}
            self.h._do_collect({'duration_ms': '50'}, res)

        mock_start.assert_not_called()
        mock_stop.assert_not_called()
        assert res['errorcode'] == 0

    def test_take_snapshot_exception_stops_tracemalloc_and_reraises(self):
        """take_snapshot 抛异常时，若本次调用启动了 tracemalloc，finally 应 stop 后再 reraise。"""
        with patch('tracemalloc.is_tracing', return_value=False), \
             patch('tracemalloc.start'), \
             patch('tracemalloc.stop') as mock_stop, \
             patch('tracemalloc.take_snapshot', side_effect=RuntimeError('snap error')):
            res = {}
            try:
                self.h._do_collect({'duration_ms': '100'}, res)
            except RuntimeError as e:
                assert 'snap error' in str(e)
            else:
                raise AssertionError('RuntimeError 应被向上传播')

        mock_stop.assert_called_once()

    def test_take_snapshot_exception_does_not_stop_when_already_tracing(self):
        """take_snapshot 抛异常但 tracemalloc 已在运行时，finally 不应调用 stop。"""
        with patch('tracemalloc.is_tracing', return_value=True), \
             patch('tracemalloc.stop') as mock_stop, \
             patch('tracemalloc.take_snapshot', side_effect=RuntimeError('snap error')):
            res = {}
            try:
                self.h._do_collect({'duration_ms': '100'}, res)
            except RuntimeError:
                pass

        mock_stop.assert_not_called()

    def test_sleep_duration_matches_params(self):
        """sleep 时长应与 duration_ms 精确对应（单位换算为秒）。"""
        mock_snap = MagicMock()
        mock_snap.compare_to.return_value = []

        with patch('tracemalloc.is_tracing', return_value=True), \
             patch('tracemalloc.take_snapshot', return_value=mock_snap), \
             patch('time.sleep') as mock_sleep:
            self.h._do_collect({'duration_ms': '250'}, {})

        mock_sleep.assert_called_once_with(0.25)


# ===========================================================================
# Configer.debug: child_index worker selection
# ===========================================================================

class TestConfigerDebugChildIndex:
    """
    验证 Configer.debug() 中 child_index 对 worker 的选取逻辑。

    策略：使用极短的 timeout（0.01s）让 asyncio.wait_for 快速超时，
    debug() 返回 None，但 send_str 已在超时前被调用，
    通过检查各 ws.send_str 的调用次数确认选中了正确的 worker。
    """

    def _make_configer(self, n: int):
        """
        构造一个含 n 个 mock worker 的 Configer 实例。
        返回 (configer, [ws0, ws1, ...])，调用方需持有 ws 列表防止被 GC。
        """
        from unittest.mock import AsyncMock

        from trpc_sdk.admin.server import Configer
        c = Configer()
        workers = []
        for _ in range(n):
            ws = MagicMock()
            ws.closed = False
            ws.send_str = AsyncMock()
            c.add_client(ws)
            workers.append(ws)
        return c, workers

    def _run(self, coro):
        return asyncio.run(coro)

    def test_child_index_selects_first_worker(self):
        """child_index=0 应将消息发给第一个 worker。"""
        c, (ws0, ws1, ws2) = self._make_configer(3)
        result = self._run(c.debug({'type': 'heap_profile'}, timeout=0.01, child_index=0))
        assert result is None  # wait_for 超时，但 send_str 已完成
        ws0.send_str.assert_called_once()
        ws1.send_str.assert_not_called()
        ws2.send_str.assert_not_called()

    def test_child_index_selects_second_worker(self):
        """child_index=1 应将消息发给第二个 worker。"""
        c, (ws0, ws1, ws2) = self._make_configer(3)
        result = self._run(c.debug({'type': 'heap_profile'}, timeout=0.01, child_index=1))
        assert result is None
        ws0.send_str.assert_not_called()
        ws1.send_str.assert_called_once()
        ws2.send_str.assert_not_called()

    def test_child_index_selects_last_worker(self):
        """child_index=N-1 应将消息发给最后一个 worker。"""
        c, (ws0, ws1, ws2) = self._make_configer(3)
        result = self._run(c.debug({'type': 'heap_profile'}, timeout=0.01, child_index=2))
        assert result is None
        ws0.send_str.assert_not_called()
        ws1.send_str.assert_not_called()
        ws2.send_str.assert_called_once()

    def test_child_index_out_of_range_falls_back_to_round_robin(self):
        """child_index >= worker 数量时，退避到 round-robin，消息应发给某一个 worker。"""
        c, workers = self._make_configer(2)
        self._run(c.debug({'type': 'heap_profile'}, timeout=0.01, child_index=99))
        total_calls = sum(ws.send_str.call_count for ws in workers)
        assert total_calls == 1  # round-robin 仍选出了一个 worker

    def test_child_index_none_uses_round_robin(self):
        """child_index=None 时走 round-robin，每次轮到不同的 worker。"""
        c, (ws0, ws1) = self._make_configer(2)
        self._run(c.debug({'type': 'heap_profile'}, timeout=0.01, child_index=None))
        self._run(c.debug({'type': 'heap_profile'}, timeout=0.01, child_index=None))
        # 两次各发一次，轮到 ws0 再轮到 ws1（或相反）
        assert ws0.send_str.call_count + ws1.send_str.call_count == 2

    def test_child_index_absent_uses_round_robin(self):
        """不传 child_index 时（默认 None）走 round-robin，消息应发给某一个 worker。"""
        c, workers = self._make_configer(2)
        self._run(c.debug({'type': 'heap_profile'}, timeout=0.01))
        total_calls = sum(ws.send_str.call_count for ws in workers)
        assert total_calls == 1

    def test_child_index_negative_string_treated_as_none(self):
        """
        负数字符串经 handler 层 isdigit() 过滤后变为 None，
        此处直接传 None 验证退避到 round-robin 的行为。
        """
        c, workers = self._make_configer(2)
        self._run(c.debug({'type': 'heap_profile'}, timeout=0.01, child_index=None))
        total_calls = sum(ws.send_str.call_count for ws in workers)
        assert total_calls == 1

    def test_no_workers_returns_none_immediately(self):
        """无可用 worker 时 debug() 应立即返回 None，不发送任何消息。"""
        from trpc_sdk.admin.server import Configer
        c = Configer()
        result = self._run(c.debug({'type': 'heap_profile'}, timeout=0.01, child_index=0))
        assert result is None
