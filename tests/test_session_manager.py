"""CCCC-CORE-06 Session 管理测试.

验证无 session_id 创建新会话、有 session_id 继续执行、保存归属、Actor 所有权校验、
防止 Session 串用（a1 不能使用 a2 Session）、并发 Run 拒绝、恢复失败不静默创建新会话。
"""

from __future__ import annotations

import os
import tempfile
import unittest

from cccc.contracts.v1.coordination import RouteValidationError
from cccc.kernel import coordination_store as store
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group
from cccc.kernel.registry import load_registry
from cccc.kernel.session_manager import (
    acquire_session_for_run,
    close_session,
    create_session,
    list_sessions_for_actor,
    release_session,
    resume_session,
)


def _setup_group(actor_ids=("a1", "a2", "a3")):
    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
    for aid in actor_ids:
        add_actor(group, actor_id=aid, title=aid, runtime="codex")
    return group


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


class TestSessionCreate(_HomeTestCase):
    def test_create_new_session_records_owner(self) -> None:
        with self._with_home():
            group = _setup_group()
            s = create_session(group, session_id="s1", owner_actor_id="a2", runtime="codex", work_dir="/repo")
            self.assertEqual(s.owner_actor_id, "a2")
            self.assertEqual(s.state, "idle")
            self.assertEqual(s.runtime, "codex")

    def test_list_sessions_for_actor_isolated(self) -> None:
        # a2 只能看到自己的 Session，防止跨 Actor 查询
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2")
            create_session(group, session_id="s2", owner_actor_id="a3")
            self.assertEqual(len(list_sessions_for_actor(group, "a2")), 1)
            self.assertEqual(len(list_sessions_for_actor(group, "a3")), 1)


class TestSessionResume(_HomeTestCase):
    def test_resume_same_owner_ok(self) -> None:
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2", runtime="codex", work_dir="/repo")
            s = resume_session(group, session_id="s1", actor_id="a2", runtime="codex", work_dir="/repo")
            self.assertEqual(s.owner_actor_id, "a2")

    def test_a1_cannot_use_a2_session(self) -> None:
        # 验收：a1 不能使用 a2 Session
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2")
            with self.assertRaises(RouteValidationError) as exc:
                resume_session(group, session_id="s1", actor_id="a1")
            self.assertEqual(exc.exception.code, "SESSION_ACTOR_MISMATCH")

    def test_resume_missing_session_no_silent_create(self) -> None:
        # 恢复失败不得静默创建新会话
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                resume_session(group, session_id="ghost", actor_id="a2")
            self.assertEqual(exc.exception.code, "SESSION_NOT_FOUND")
            # 确认没有静默创建
            self.assertIsNone(store.get_session(group, "ghost"))

    def test_resume_runtime_mismatch_blocked(self) -> None:
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2", runtime="codex")
            with self.assertRaises(RouteValidationError) as exc:
                resume_session(group, session_id="s1", actor_id="a2", runtime="claude")
            self.assertEqual(exc.exception.code, "SESSION_RUNTIME_MISMATCH")

    def test_resume_workdir_mismatch_blocked(self) -> None:
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2", work_dir="/repo-a")
            with self.assertRaises(RouteValidationError) as exc:
                resume_session(group, session_id="s1", actor_id="a2", work_dir="/repo-b")
            self.assertEqual(exc.exception.code, "SESSION_WORKSPACE_PATH_MISMATCH")


class TestSessionConcurrency(_HomeTestCase):
    def test_busy_session_rejects_concurrent_run(self) -> None:
        # 同一 Session 并发执行多个 Run 被拒绝
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2")
            acquire_session_for_run(group, session_id="s1", run_id="r1", actor_id="a2")
            with self.assertRaises(RouteValidationError) as exc:
                acquire_session_for_run(group, session_id="s1", run_id="r2", actor_id="a2")
            self.assertEqual(exc.exception.code, "SESSION_BUSY")

    def test_release_allows_reuse(self) -> None:
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2")
            acquire_session_for_run(group, session_id="s1", run_id="r1", actor_id="a2")
            s = release_session(group, session_id="s1")
            self.assertEqual(s.state, "idle")
            self.assertIsNone(s.current_run_id)
            # 释放后可再次占用
            s2 = acquire_session_for_run(group, session_id="s1", run_id="r2", actor_id="a2")
            self.assertEqual(s2.state, "busy")
            self.assertEqual(s2.current_run_id, "r2")

    def test_acquire_wrong_owner_blocked(self) -> None:
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2")
            with self.assertRaises(RouteValidationError) as exc:
                acquire_session_for_run(group, session_id="s1", run_id="r1", actor_id="a1")
            self.assertEqual(exc.exception.code, "SESSION_ACTOR_MISMATCH")

    def test_release_failed_marks_session_failed(self) -> None:
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2")
            acquire_session_for_run(group, session_id="s1", run_id="r1", actor_id="a2")
            s = release_session(group, session_id="s1", failed=True)
            self.assertEqual(s.state, "failed")
            # failed session 不能再被占用
            with self.assertRaises(RouteValidationError) as exc:
                acquire_session_for_run(group, session_id="s1", run_id="r2", actor_id="a2")
            self.assertEqual(exc.exception.code, "SESSION_RESUME_FAILED")


class TestSessionClose(_HomeTestCase):
    def test_close_session(self) -> None:
        with self._with_home():
            group = _setup_group()
            create_session(group, session_id="s1", owner_actor_id="a2")
            s = close_session(group, session_id="s1")
            self.assertEqual(s.state, "closed")


if __name__ == "__main__":
    unittest.main()
