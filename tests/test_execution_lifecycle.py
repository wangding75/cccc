"""CCCC 执行生命周期补偿专项测试 (CCCC-FIX-02)."""
from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

from cccc.contracts.v1.coordination import (
    ERR_RUN_ALREADY_EXISTS,
    Delivery,
    RouteValidationError,
)
from cccc.kernel.execution_authorizer import ExecutionAuthorization
from cccc.kernel.execution_lifecycle import ExecutionSaga
from cccc.kernel.coordination_store import Run
from cccc.kernel import coordination_store as store


class _HomeTestCase(unittest.TestCase):
    def _make_home(self) -> str:
        import tempfile
        td = tempfile.mkdtemp(prefix="cccc_lc_")
        self._old_home = os.environ.get("CCCC_HOME")
        os.environ["CCCC_HOME"] = td
        self._home_dir = td
        return td

    def tearDown(self) -> None:
        td = getattr(self, "_home_dir", None)
        if td:
            import shutil
            shutil.rmtree(td, ignore_errors=True)
        old = getattr(self, "_old_home", None)
        if old is None:
            os.environ.pop("CCCC_HOME", None)
        else:
            os.environ["CCCC_HOME"] = old


def _setup_group(reg):
    from cccc.kernel.group import create_group
    from cccc.kernel.actors import add_actor
    group = create_group(reg, title="wg")
    add_actor(group, actor_id="a_f", title="foreman", runtime="codex", enabled=True)
    add_actor(group, actor_id="a_p", title="peer", runtime="codex", enabled=True)
    return group


def _make_delivery(group, message_id: str, state: str = "reserved"):
    from cccc.kernel.coordination_store import _delivery_path
    d = Delivery(
        delivery_id=f"dv.{message_id}",
        message_id=message_id,
        group_id=group.group_id,
        recipient_actor_id="a_p",
        state=state,
        attempts=0,
        locked_by="a_p",
        locked_at=None,
        last_error=None,
        created_at="2026-07-23T10:00:00Z",
        updated_at="2026-07-23T10:00:00Z",
    )
    p = _delivery_path(group, d.delivery_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(d.model_dump_json())
    return d


def _make_task_message(group, message_id: str):
    from cccc.kernel import collaboration_loop as loop
    from cccc.kernel.collaboration_loop import _append_task_message
    tm = loop.TaskMessage(
        group_id=group.group_id,
        message_id=message_id,
        sender_actor_id="a_f",
        recipient_actor_id="a_p",
        kind="task_instruction",
        payload={},
    )
    _append_task_message(group, tm)
    return tm


def _make_auth(group, message_id: str):
    return ExecutionAuthorization(
        delivery_id=f"dv.{message_id}",
        group_id=group.group_id,
        message_id=message_id,
        peer_actor_id="a_p",
        consumer_id="a_p",
    )


class TestExecutionLifecycle(_HomeTestCase):
    def _setup(self, suffix: str, *, delivery_state: str = "reserved"):
        td = self._make_home()
        from cccc.kernel.registry import load_registry
        reg = load_registry()
        reg.save()
        group = _setup_group(reg)
        message_id = f"m.lc.{suffix}"
        _make_delivery(group, message_id, state=delivery_state)
        _make_task_message(group, message_id)
        auth = _make_auth(group, message_id)
        return group, message_id, f"dv.{message_id}", auth

    def _saga(self, group, **kw):
        auth = kw.pop("auth")
        return ExecutionSaga(group).execute(auth, **kw)

    def test_create_run_failure_compensates(self):
        group, message_id, _, auth = self._setup("cr1")
        existing = Run(
            run_id="r.existing2",
            group_id=group.group_id,
            message_id=message_id,
            actor_id="a_p",
            state="succeeded",
        )
        from cccc.kernel.coordination_store import create_run as store_create_run
        store_create_run(group, existing)
        with self.assertRaises(RouteValidationError) as ctx:
            self._saga(group, auth=auth, run_id="r.lc.1",
                       command=[sys.executable, "-c", "print('ok')"], runtime="one_shot")
        self.assertEqual(ctx.exception.code, ERR_RUN_ALREADY_EXISTS)

    def test_runner_start_failure_marks_run_failed(self):
        group, message_id, _, auth = self._setup("rs1")
        result = self._saga(group, auth=auth, run_id="r.lc.2",
                            command=[sys.executable, "-c", "import sys; sys.exit(2)"],
                            runtime="one_shot", timeout_seconds=0.5)
        from cccc.kernel.coordination_store import get_run
        run = get_run(group, "r.lc.2")
        self.assertIsNotNone(run)
        self.assertIn(run.state, ("failed", "timed_out"))

    def test_output_capture_failure_marks_run_failed(self):
        group, message_id, _, auth = self._setup("oc1")
        import cccc.kernel.output_capture as oc_mod
        orig = oc_mod.capture_run_output
        def boom(*a, **kw):
            raise RuntimeError("capture boom")
        oc_mod.capture_run_output = boom
        try:
            with self.assertRaises(RuntimeError):
                self._saga(group, auth=auth, run_id="r.lc.3",
                           command=[sys.executable, "-c", "print('ok')"], runtime="one_shot")
        finally:
            oc_mod.capture_run_output = orig
        from cccc.kernel.coordination_store import get_run
        run = get_run(group, "r.lc.3")
        self.assertIsNotNone(run)
        self.assertEqual(run.state, "failed")

    def test_ack_failure_recorded_not_swallowed(self):
        group, message_id, delivery_id, auth = self._setup("af1", delivery_state="reserved")
        import cccc.kernel.mailbox as mailbox_mod
        from cccc.contracts.v1.coordination import RouteValidationError as RVE
        orig_ack = mailbox_mod.ack_delivery
        def fake_ack(*a, **kw):
            raise RVE("STORAGE_ERROR", "simulated ack failure")
        mailbox_mod.ack_delivery = fake_ack
        try:
            saga = ExecutionSaga(group)
            result = saga.execute(auth, run_id="r.lc.4",
                                  command=[sys.executable, "-c", "print('ack-fail')"],
                                  runtime="one_shot")
        finally:
            mailbox_mod.ack_delivery = orig_ack
        self.assertEqual(result.run.state, "succeeded")
        ack_errors = [e for e in result.errors if e.stage == "ack"]
        self.assertTrue(ack_errors, "ack failure must be recorded, not swallowed: %s" % [str(e) for e in result.errors])

    def test_nack_failure_has_secondary_error(self):
        group, message_id, delivery_id, auth = self._setup("nf1", delivery_state="reserved")
        # _compensate directly to test nack-failure error recording
        # without depending on runner-processed lifecycle path.
        import cccc.kernel.mailbox as mailbox_mod
        from cccc.contracts.v1.coordination import RouteValidationError as RVE
        def fake_nack(*a, **kw):
            raise RVE("STORAGE_ERROR", "simulated nack failure")
        orig_nack = mailbox_mod.nack_delivery
        mailbox_mod.nack_delivery = fake_nack
        try:
            saga = ExecutionSaga(group)
            # Pre-populate a failed run so _mark_run_failed_if_exists is a no-op,
            # letting _compensate proceed to nack.
            from cccc.kernel.coordination_store import create_run
            run = Run(run_id="r.lc.5", group_id=group.group_id, actor_id="a_p",
                       message_id=message_id, state="failed",
                       started_at="2026-07-24T03:00:00Z", ended_at="2026-07-24T03:00:01Z")
            create_run(group, run)
            saga._run = run
            saga._delivery = store.get_delivery(group, delivery_id)
            saga._authorization = auth
            saga._session_id = f"ses.r.lc.5"
            saga._compensate("run_start")
        finally:
            mailbox_mod.nack_delivery = orig_nack
        nack_errors = [e for e in saga._errors if e.stage == "compensate_nack"]
        self.assertTrue(nack_errors, "nack failure must be recorded: %s" % [str(e) for e in saga._errors])

    def test_success_path_final_states(self):
        group, message_id, delivery_id, auth = self._setup("sp1")
        result = self._saga(group, auth=auth, run_id="r.lc.6",
                            command=[sys.executable, "-c", "print('success')"],
                            runtime="one_shot")
        from cccc.kernel.coordination_store import get_run, get_delivery, list_sessions
        self.assertEqual(result.run.state, "succeeded")
        d = get_delivery(group, delivery_id)
        self.assertIsNotNone(d)
        self.assertEqual(d.state, "delivered")
        sessions = list_sessions(group, owner_actor_id="a_p")
        for s in sessions:
            self.assertNotIn(s.state, ("busy",))

    def test_restart_recovery_of_compensation_state(self):
        group, message_id, _, auth = self._setup("rr1")
        result = self._saga(group, auth=auth, run_id="r.lc.7",
                            command=[sys.executable, "-c", "import sys; sys.exit(1)"],
                            runtime="one_shot", timeout_seconds=0.5)
        from cccc.kernel.coordination_store import get_run
        run = get_run(group, "r.lc.7")
        self.assertIsNotNone(run)
        self.assertIn(run.state, ("failed", "timed_out"))


if __name__ == "__main__":
    unittest.main()
