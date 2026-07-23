"""CCCC One-shot Runner (CCCC-CORE-05).

以新增执行模式（execution_mode=one_shot）实现一次任务一次进程的执行框架，
与现有 PTY / headless 模式并存，不替换长期 PTY 执行模式。

生命周期：
    创建 Run（coordination_store）→ 启动一次智能体进程 → 记录输入 →
    保存执行结果（stdout/stderr/exit_code）→ 进程退出后释放资源 → Run 终态。

Actor 仍是逻辑角色，不绑定长期进程。本模块只实现执行框架与进程生命周期管理，
不绑定具体 Agent CLI（通过 command 参数注入，便于测试与多 runtime 复用）。
实际 Runtime Adapter（codex/claude 等）属后续任务。
"""

from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from ..contracts.v1.coordination import (
    ERR_OUTPUT_CAPTURE_FAILED,
    ERR_RUN_ALREADY_EXISTS,
    ERR_RUN_CANCELLED,
    ERR_RUN_TIMED_OUT,
    Run,
    RunArtifact,
    RunState,
    RouteValidationError,
    StandardEvent,
    StandardEventStream,
)
from ..util.fs import atomic_write_bytes
from ..util.time import utc_now_iso
from . import coordination_store as store
from .actors import find_actor
from .group import Group


@dataclass
class OneShotRunInput:
    """One-shot Run 的输入。"""

    run_id: str
    message_id: str
    actor_id: str
    command: List[str]
    cwd: str = ""
    env: Dict[str, str] = field(default_factory=dict)
    stdin_text: str = ""
    timeout_seconds: Optional[float] = None
    session_id: str = ""


@dataclass
class OneShotRunResult:
    """One-shot Run 的执行结果。"""

    run_id: str
    state: RunState
    exit_code: Optional[int]
    stdout_bytes: int
    stderr_bytes: int
    started_at: str
    ended_at: str
    error_code: Optional[str] = None


class OneShotRunner:
    """一次任务一次进程的执行框架。

    线程安全：每个 Run 在独立线程中执行；进程退出后释放资源。
    """

    def __init__(self, group: Group, *, runtime: str = "") -> None:
        self.group = group
        self.runtime = runtime
        self._lock = threading.Lock()
        self._processes: Dict[str, subprocess.Popen] = {}
        self._cancelled: Set[str] = set()
        self._cancel_events: Dict[str, threading.Event] = {}

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------

    def start(self, run_input: OneShotRunInput) -> OneShotRunResult:
        """同步执行一个 One-shot Run：创建 Run → 启动进程 → 保存结果 → 释放资源。"""
        self._validate_actor(run_input.actor_id)
        run = self._create_run(run_input)
        started_at = utc_now_iso()
        cancel_event = threading.Event()
        with self._lock:
            self._cancel_events[run.run_id] = cancel_event
        try:
            store.update_run_state(self.group, run.run_id, state="running", started_at=started_at)
            result = self._execute(run_input, run, started_at, cancel_event)
            return result
        finally:
            self._release(run.run_id)
            with self._lock:
                self._cancel_events.pop(run.run_id, None)
                self._cancelled.discard(run.run_id)

    def cancel(self, run_id: str) -> bool:
        """取消运行中的 Run：发信号终止进程树，标记 cancelled。返回是否触发了取消。

        验收：取消后不存在孤儿进程——对整个进程组发 SIGTERM/SIGKILL 并回收。
        """
        with self._lock:
            cancel_event = self._cancel_events.get(run_id)
            self._cancelled.add(run_id)
            proc = self._processes.get(run_id)
        if cancel_event is not None:
            cancel_event.set()
        if proc is not None and proc.poll() is None:
            self._terminate_tree(proc)
            return True
        return False

    def is_cancelled(self, run_id: str) -> bool:
        with self._lock:
            return run_id in self._cancelled

    def start_async(self, run_input: OneShotRunInput, on_done: Optional[Callable[[OneShotRunResult], None]] = None) -> threading.Thread:
        """异步执行；返回执行线程。"""
        thread = threading.Thread(
            target=self._async_wrapper,
            args=(run_input, on_done),
            name=f"oneshot-{run_input.run_id}",
            daemon=True,
        )
        thread.start()
        return thread

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _async_wrapper(self, run_input: OneShotRunInput, on_done: Optional[Callable[[OneShotRunResult], None]]) -> None:
        try:
            result = self.start(run_input)
        except Exception as exc:  # noqa: BLE001
            result = OneShotRunResult(
                run_id=run_input.run_id,
                state="failed",
                exit_code=None,
                stdout_bytes=0,
                stderr_bytes=0,
                started_at=utc_now_iso(),
                ended_at=utc_now_iso(),
                error_code=ERR_OUTPUT_CAPTURE_FAILED,
            )
            _ = exc
        if on_done is not None:
            try:
                on_done(result)
            except Exception:
                pass

    def _validate_actor(self, actor_id: str) -> None:
        if not actor_id:
            raise RouteValidationError("ACTOR_NOT_FOUND", "missing actor_id")
        if find_actor(self.group, actor_id) is None:
            raise RouteValidationError("ACTOR_NOT_FOUND", f"actor not found: {actor_id}")

    def _create_run(self, run_input: OneShotRunInput) -> Run:
        run = Run(
            run_id=run_input.run_id,
            group_id=self.group.group_id,
            actor_id=run_input.actor_id,
            message_id=run_input.message_id,
            session_id=run_input.session_id,
            runtime=self.runtime,
            execution_mode="one_shot",
            state="pending",
        )
        try:
            return store.create_run(self.group, run)
        except RouteValidationError as exc:
            if exc.code == ERR_RUN_ALREADY_EXISTS:
                raise
            raise

    def _execute(self, run_input: OneShotRunInput, run: Run, started_at: str, cancel_event: threading.Event) -> OneShotRunResult:
        # 记录输入
        self._record_input(run_input)
        try:
            stdout, stderr, exit_code, timed_out, cancelled = self._run_process(run_input, cancel_event)
        except Exception as exc:  # noqa: BLE001
            self._save_failed(run, started_at, ERR_OUTPUT_CAPTURE_FAILED, str(exc))
            return OneShotRunResult(
                run_id=run.run_id, state="failed", exit_code=None,
                stdout_bytes=0, stderr_bytes=0, started_at=started_at,
                ended_at=utc_now_iso(), error_code=ERR_OUTPUT_CAPTURE_FAILED,
            )

        # 保存原始 stdout / stderr（分开、不修改内容）
        stdout_bytes = self._save_artifact(run, "raw.stdout", stdout)
        stderr_bytes = self._save_artifact(run, "raw.stderr", stderr)

        ended_at = utc_now_iso()
        if cancelled:
            state: RunState = "cancelled"
            error_code = ERR_RUN_CANCELLED
        elif timed_out:
            state = "timed_out"
            error_code = ERR_RUN_TIMED_OUT
        elif exit_code == 0:
            state = "succeeded"
            error_code = None
        else:
            state = "failed"
            error_code = None

        store.update_run_state(
            self.group, run.run_id,
            state=state, ended_at=ended_at, exit_code=exit_code, error_code=error_code,
        )
        return OneShotRunResult(
            run_id=run.run_id, state=state, exit_code=exit_code,
            stdout_bytes=stdout_bytes, stderr_bytes=stderr_bytes,
            started_at=started_at, ended_at=ended_at, error_code=error_code,
        )

    def _run_process(
        self, run_input: OneShotRunInput, cancel_event: threading.Event
    ) -> tuple[bytes, bytes, Optional[int], bool, bool]:
        """启动一次智能体进程并等待退出。

        返回 (stdout, stderr, exit_code, timed_out, cancelled)。
        进程以独立进程组启动（start_new_session=True），便于按组终止整个进程树，
        取消/超时后对进程组发 SIGTERM→SIGKILL 并回收，避免孤儿进程。
        """
        cmd = list(run_input.command)
        if not cmd:
            raise ValueError("empty command")
        env = dict(os.environ)
        env.update(run_input.env)
        cwd = run_input.cwd or None
        stdin_data = run_input.stdin_text.encode("utf-8") if run_input.stdin_text else None
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE if stdin_data is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,  # 子进程独立进程组，便于整组终止
        )
        with self._lock:
            self._processes[run_input.run_id] = proc
        timed_out = False
        cancelled = False
        try:
            # 用线程等待，主循环同时监听取消事件，实现可取消的等待
            stdout_holder: Dict[str, bytes] = {"stdout": b""}
            stderr_holder: Dict[str, bytes] = {"stderr": b""}
            comm_exc: List[BaseException] = []

            def _comm() -> None:
                try:
                    out, err = proc.communicate(input=stdin_data, timeout=run_input.timeout_seconds)
                    stdout_holder["stdout"] = out or b""
                    stderr_holder["stderr"] = err or b""
                except subprocess.TimeoutExpired:
                    comm_exc.append(subprocess.TimeoutExpired(cmd, run_input.timeout_seconds or 0))
                except BaseException as exc:  # noqa: BLE001
                    comm_exc.append(exc)

            comm_thread = threading.Thread(target=_comm, name=f"oneshot-comm-{run_input.run_id}", daemon=True)
            comm_thread.start()

            # 轮询取消事件，超时由 communicate 内部触发
            poll_interval = 0.1
            while comm_thread.is_alive():
                comm_thread.join(timeout=poll_interval)
                if cancel_event.is_set():
                    # 取消优先：标记 cancelled 并终止进程树，无论进程当前是否已退出
                    cancelled = True
                    self._terminate_tree(proc)
                    comm_thread.join(timeout=5)
                    break

            # 处理竞态：cancel 可能在 communicate 即将返回时触发，导致 comm_thread 先于
            # 取消检测退出。此处补判一次 cancel_event，确保取消语义优先于退出码判定。
            if cancel_event.is_set():
                cancelled = True
                self._terminate_tree(proc)
                comm_thread.join(timeout=5)

            if comm_exc and isinstance(comm_exc[0], subprocess.TimeoutExpired) and not cancelled:
                timed_out = True
                self._terminate_tree(proc)
                comm_thread.join(timeout=5)
            elif comm_exc and not cancelled:
                # communicate 抛出其他异常：确保进程树被清理后向上冒泡为失败
                self._terminate_tree(proc)
                raise comm_exc[0]
            # cancelled 优先：取消导致的管道异常忽略，状态由 cancelled 决定

            stdout = stdout_holder.get("stdout", b"")
            stderr = stderr_holder.get("stderr", b"")
            exit_code = proc.returncode
            return stdout, stderr, exit_code, timed_out, cancelled
        finally:
            with self._lock:
                self._processes.pop(run_input.run_id, None)
            # 最终保险：确保进程及其子进程被回收，无孤儿
            self._reap_tree(proc)

    def _record_input(self, run_input: OneShotRunInput) -> None:
        """记录 Run 输入（命令 + stdin）。"""
        input_dir = store._runs_dir(self.group)
        input_path = input_dir / f"{store._safe_id(run_input.run_id)}.input.json"
        atomic_write_bytes(
            input_path,
            __import__("json").dumps(
                {
                    "run_id": run_input.run_id,
                    "message_id": run_input.message_id,
                    "actor_id": run_input.actor_id,
                    "command": run_input.command,
                    "cwd": run_input.cwd,
                    "stdin": run_input.stdin_text,
                    "session_id": run_input.session_id,
                },
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8"),
        )

    def _save_artifact(self, run: Run, kind: str, data: bytes) -> int:
        """保存原始 stdout/stderr 产物（sha256 身份，不修改内容）。"""
        import hashlib

        artifact_id = f"{run.run_id}.{kind}"
        sha256 = hashlib.sha256(data).hexdigest()
        store.create_artifact(
            self.group,
            RunArtifact(
                artifact_id=artifact_id,
                run_id=run.run_id,
                kind=kind,  # type: ignore[arg-type]
                sha256=sha256,
                bytes=len(data),
            ),
        )
        # 写入原始内容到 blobs（不修改、不摘要）
        blob_dir = self.group.path / "state" / "coordination" / "outputs"
        blob_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_bytes(blob_dir / f"{artifact_id}.bin", data)
        return len(data)

    def _save_failed(self, run: Run, started_at: str, error_code: str, detail: str) -> None:
        store.update_run_state(
            self.group, run.run_id,
            state="failed", ended_at=utc_now_iso(), error_code=error_code,
        )

    def _release(self, run_id: str) -> None:
        """进程退出后释放资源（清理进程引用，确保无孤儿进程）。"""
        with self._lock:
            proc = self._processes.pop(run_id, None)
        if proc is not None and proc.poll() is None:
            self._terminate_tree(proc)
        if proc is not None:
            self._reap_tree(proc)

    def _kill(self, proc: subprocess.Popen) -> None:
        self._terminate_tree(proc)

    def _terminate_tree(self, proc: subprocess.Popen) -> None:
        """终止整个进程树：先对进程组发 SIGTERM，超时未退出再 SIGKILL。

        子进程以 start_new_session=True 启动，pgid = pid，因此 os.killpg 可整组终止，
        避免子进程成为孤儿。
        """
        if proc.poll() is not None:
            return
        pgid: Optional[int]
        try:
            pgid = os.getpgid(proc.pid) if proc.pid else None
        except (ProcessLookupError, OSError):
            pgid = None
        # SIGTERM 整组
        self._signal_group(pgid, proc, signal.SIGTERM)
        # 等待最多 3s 优雅退出
        for _ in range(30):
            if proc.poll() is not None:
                return
            time.sleep(0.1)
        # 仍未退出：SIGKILL 整组
        self._signal_group(pgid, proc, signal.SIGKILL)
        try:
            proc.wait(timeout=5)
        except Exception:
            pass

    def _signal_group(self, pgid: Optional[int], proc: subprocess.Popen, sig: int) -> None:
        if pgid is not None:
            try:
                os.killpg(pgid, sig)
                return
            except (ProcessLookupError, OSError):
                pass
        # 回退：仅对主进程发信号
        try:
            if proc.pid:
                os.kill(proc.pid, sig)
        except (ProcessLookupError, OSError):
            pass

    def _reap_tree(self, proc: subprocess.Popen) -> None:
        """回收进程，避免僵尸进程。已退出则 wait；未退出则强制终止后 wait。"""
        if proc.poll() is None:
            self._terminate_tree(proc)
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
        # 关闭管道，释放文件描述符
        for pipe in (proc.stdout, proc.stderr, proc.stdin):
            if pipe is not None:
                try:
                    pipe.close()
                except Exception:
                    pass


def append_run_event(group: Group, event: StandardEvent) -> None:
    """便捷：向 Run 追加标准事件（复用 coordination_store）。"""
    store.append_standard_event(group, event)


def list_run_artifacts(group: Group, run_id: str) -> List[RunArtifact]:
    return store.list_artifacts(group, run_id=run_id)
