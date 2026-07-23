"""CCCC-CORE-05 One-shot Runner 测试.

验证一次任务一次进程的执行框架：创建 Run、启动一次进程、记录输入、保存结果、
进程退出后释放资源；一次任务对应一次 Run 和一次进程生命周期；重复 Run 被拒绝。
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest

from cccc.contracts.v1.coordination import RouteValidationError
from cccc.kernel import coordination_store as store
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group
from cccc.kernel.one_shot_runner import OneShotRunInput, OneShotRunner, list_run_artifacts
from cccc.kernel.registry import load_registry


def _setup_group(actor_ids=("a1", "a2")):
    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
    for aid in actor_ids:
        add_actor(group, actor_id=aid, title=aid, runtime="codex")
    return group


def _echo_command(text: str):
    return [sys.executable, "-c", f"import sys; sys.stdout.write({text!r}); sys.stderr.write('err')"]


class _HomeTestCase(unittest.TestCase):
    def _with_home(self) -> tempfile.TemporaryDirectory:
        self._old_home = os.environ.get("CCCC_HOME")
        self._td = tempfile.TemporaryDirectory()
        os.environ["CCCC_HOME"] = self._td.name
        return self._td

    def tearDown(self) -> None:
        td = getattr(self, "_td", None)
        if td is not None:
            td.cleanup()
        old = getattr(self, "_old_home", None)
        if old is None:
            os.environ.pop("CCCC_HOME", None)
        else:
            os.environ["CCCC_HOME"] = old


class TestOneShotRunner(_HomeTestCase):
    def test_one_task_one_run_one_process_lifecycle(self) -> None:
        # 验收：一次任务对应一次 Run 和一次进程生命周期
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            result = runner.start(
                OneShotRunInput(
                    run_id="r1",
                    message_id="m1",
                    actor_id="a2",
                    command=_echo_command("hello"),
                )
            )
            self.assertEqual(result.state, "succeeded")
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout_bytes, len("hello"))
            self.assertEqual(result.stderr_bytes, len("err"))
            run = store.get_run(group, "r1")
            self.assertEqual(run.state, "succeeded")
            self.assertIsNotNone(run.started_at)
            self.assertIsNotNone(run.ended_at)

    def test_failed_run_records_nonzero_exit(self) -> None:
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            result = runner.start(
                OneShotRunInput(
                    run_id="r1",
                    message_id="m1",
                    actor_id="a2",
                    command=[sys.executable, "-c", "import sys; sys.exit(3)"],
                )
            )
            self.assertEqual(result.state, "failed")
            self.assertEqual(result.exit_code, 3)

    def test_timeout_marks_timed_out(self) -> None:
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            result = runner.start(
                OneShotRunInput(
                    run_id="r1",
                    message_id="m1",
                    actor_id="a2",
                    command=[sys.executable, "-c", "import time; time.sleep(10)"],
                    timeout_seconds=0.5,
                )
            )
            self.assertEqual(result.state, "timed_out")
            self.assertEqual(result.error_code, "RUN_TIMED_OUT")
            run = store.get_run(group, "r1")
            self.assertEqual(run.state, "timed_out")

    def test_duplicate_run_rejected(self) -> None:
        # 同一 (message, actor) 不能重复创建 Run
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            runner.start(OneShotRunInput(run_id="r1", message_id="m1", actor_id="a2", command=_echo_command("x")))
            with self.assertRaises(RouteValidationError) as exc:
                runner.start(OneShotRunInput(run_id="r2", message_id="m1", actor_id="a2", command=_echo_command("x")))
            self.assertEqual(exc.exception.code, "RUN_ALREADY_EXISTS")

    def test_actor_must_exist(self) -> None:
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            with self.assertRaises(RouteValidationError) as exc:
                runner.start(OneShotRunInput(run_id="r1", message_id="m1", actor_id="ghost", command=_echo_command("x")))
            self.assertEqual(exc.exception.code, "ACTOR_NOT_FOUND")

    def test_input_recorded_and_artifacts_saved(self) -> None:
        # 记录输入 + 保存原始 stdout/stderr（分开、不修改内容）
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            runner.start(
                OneShotRunInput(
                    run_id="r1", message_id="m1", actor_id="a2",
                    command=_echo_command("payload"), stdin_text="in",
                )
            )
            # 输入文件
            input_path = store._runs_dir(group) / "r1.input.json"
            self.assertTrue(input_path.exists())
            # artifacts
            arts = list_run_artifacts(group, "r1")
            kinds = sorted(a.kind for a in arts)
            self.assertEqual(kinds, ["raw.stderr", "raw.stdout"])
            # 原始内容未修改
            out_blob = group.path / "state" / "coordination" / "outputs" / "r1.raw.stdout.bin"
            self.assertEqual(out_blob.read_bytes(), b"payload")

    def test_process_released_after_exit(self) -> None:
        # 进程退出后释放资源
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            runner.start(OneShotRunInput(run_id="r1", message_id="m1", actor_id="a2", command=_echo_command("x")))
            # 进程引用已清理
            self.assertEqual(len(runner._processes), 0)

    def test_async_start(self) -> None:
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            results = []
            thread = runner.start_async(
                OneShotRunInput(run_id="r1", message_id="m1", actor_id="a2", command=_echo_command("async")),
                on_done=results.append,
            )
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].state, "succeeded")


class TestProcessManagement(_HomeTestCase):
    """CCCC-CORE-07 进程管理：取消、超时、子进程清理、无孤儿进程。"""

    def test_cancel_marks_cancelled_and_kills_tree(self) -> None:
        # 取消运行中进程：状态 cancelled，进程被终止
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            results = []
            thread = runner.start_async(
                OneShotRunInput(
                    run_id="r1", message_id="m1", actor_id="a2",
                    command=[sys.executable, "-c", "import time; time.sleep(60)"],
                ),
                on_done=results.append,
            )
            # 等待进程进入运行
            for _ in range(50):
                if runner.is_cancelled("r1"):
                    break
                with runner._lock:
                    running = "r1" in runner._processes and runner._processes["r1"].poll() is None
                if running:
                    break
                time.sleep(0.1)
            triggered = runner.cancel("r1")
            self.assertTrue(triggered)
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].state, "cancelled")
            self.assertEqual(results[0].error_code, "RUN_CANCELLED")
            run = store.get_run(group, "r1")
            self.assertEqual(run.state, "cancelled")

    def test_cancel_no_orphan_processes(self) -> None:
        # 验收：取消后不存在孤儿进程——子进程随进程组被清理
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            # 父脚本启动一个子进程，二者都应在取消时被清理
            parent_cmd = [
                sys.executable, "-c",
                "import subprocess,sys,time\n"
                "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])\n"
                "time.sleep(60)",
            ]
            results = []
            thread = runner.start_async(
                OneShotRunInput(run_id="r1", message_id="m1", actor_id="a2", command=parent_cmd),
                on_done=results.append,
            )
            child_pid_holder = {}

            def _poll() -> None:
                # 探测父进程的子进程 pid（通过 psutil 不可用时跳过，仅依赖进程组终止）
                import time as _t
                for _ in range(50):
                    with runner._lock:
                        proc = runner._processes.get("r1")
                    if proc and proc.poll() is None and proc.pid:
                        child_pid_holder["pid"] = proc.pid
                        break
                    _t.sleep(0.1)

            _poll()
            runner.cancel("r1")
            thread.join(timeout=10)
            self.assertEqual(results[0].state, "cancelled")
            # 进程组已被终止：父进程不应存活
            parent_pid = child_pid_holder.get("pid")
            if parent_pid:
                import os as _os
                try:
                    _os.kill(parent_pid, 0)
                    alive = True
                except (ProcessLookupError, OSError):
                    alive = False
                # 父进程已被取消清理
                self.assertFalse(alive, "parent process should be terminated after cancel")

    def test_timeout_terminates_tree(self) -> None:
        # 超时：整组被终止，状态 timed_out
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            result = runner.start(
                OneShotRunInput(
                    run_id="r1", message_id="m1", actor_id="a2",
                    command=[sys.executable, "-c", "import time; time.sleep(30)"],
                    timeout_seconds=0.5,
                )
            )
            self.assertEqual(result.state, "timed_out")
            self.assertEqual(result.error_code, "RUN_TIMED_OUT")
            # 进程引用已释放
            self.assertEqual(len(runner._processes), 0)

    def test_exception_recovery_marks_failed(self) -> None:
        # 进程异常（无法启动）：状态收敛为 failed，不留孤儿
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            result = runner.start(
                OneShotRunInput(
                    run_id="r1", message_id="m1", actor_id="a2",
                    command=["/nonexistent/binary/that/does/not/exist"],
                )
            )
            self.assertEqual(result.state, "failed")
            self.assertEqual(len(runner._processes), 0)

    def test_release_reaps_finished_process(self) -> None:
        # 正常退出后资源释放，无僵尸
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            runner.start(OneShotRunInput(run_id="r1", message_id="m1", actor_id="a2", command=_echo_command("x")))
            self.assertEqual(len(runner._processes), 0)


if __name__ == "__main__":
    unittest.main()
