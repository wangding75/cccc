"""CCCC-CORE-11 协调控制 API 测试.

验收：UI 无需读取内部文件即可完成操作。
覆盖 Workspace/Actor/Message/Run/Session/Event 查询 API。
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

from fastapi.testclient import TestClient


class _HomeTestCase(unittest.TestCase):
    def _with_home(self):
        self._old_home = os.environ.get("CCCC_HOME")
        self._td = tempfile.TemporaryDirectory()
        os.environ["CCCC_HOME"] = self._td.name
        self.addCleanup(self._cleanup_home)
        return self  # act as a no-op context manager target

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def _cleanup_home(self) -> None:
        td = getattr(self, "_td", None)
        if td is not None:
            td.cleanup()
        old = getattr(self, "_old_home", None)
        if old is None:
            os.environ.pop("CCCC_HOME", None)
        else:
            os.environ["CCCC_HOME"] = old


def _setup_group(actor_ids=("a1", "a2", "a3")):
    from cccc.kernel.actors import add_actor
    from cccc.kernel.group import create_group
    from cccc.kernel.registry import load_registry

    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
    for aid in actor_ids:
        add_actor(group, actor_id=aid, title=aid, runtime="codex")
    return group


def _echo_cmd(text: str):
    return [sys.executable, "-c", f"import sys; sys.stdout.write({text!r}); sys.stderr.write('e')"]


class TestCoordinationApi(_HomeTestCase):
    def _client(self) -> TestClient:
        from cccc.ports.web.app import create_app

        return TestClient(create_app())

    def _run_full_loop(self, group):
        from cccc.kernel import collaboration_loop as loop

        loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="m.instr.1", payload={"task": "X"})
        tm = loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2")
        loop.execute_instruction(group, peer_actor_id="a2", task_message=tm, run_id="r1", command=_echo_cmd("done"), runtime="codex", consumer_id="a2")
        loop.send_result(group, peer_actor_id="a2", foreman_actor_id="a1", instruction_message_id="m.instr.1", result_message_id="m.result.1", run_id="r1")

    def test_workspace_settings_api(self) -> None:
        with self._with_home():
            group = _setup_group()
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/settings")
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["group_id"], group.group_id)
            self.assertIn("settings", body)

    def test_actor_api_lists_roles(self) -> None:
        with self._with_home():
            group = _setup_group()
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/actors")
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            ids = [a["actor_id"] for a in body["actors"]]
            self.assertEqual(sorted(ids), ["a1", "a2", "a3"])
            foreman = body["foreman_actor_id"]
            self.assertTrue(foreman in ids)
            foreman_entry = next(a for a in body["actors"] if a["actor_id"] == foreman)
            self.assertEqual(foreman_entry["role"], "foreman")
            self.assertTrue(foreman_entry["is_foreman"])

    def test_actor_detail_api(self) -> None:
        with self._with_home():
            group = _setup_group()
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/actors/a2")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["actor_id"], "a2")
            resp404 = c.get(f"/api/v1/groups/{group.group_id}/coordination/actors/ghost")
            self.assertEqual(resp404.status_code, 404)

    def test_message_api_lists_and_filters(self) -> None:
        with self._with_home():
            group = _setup_group()
            self._run_full_loop(group)
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/messages")
            self.assertEqual(resp.status_code, 200)
            msgs = resp.json()["messages"]
            self.assertEqual(len(msgs), 2)
            # filter by kind
            resp_kind = c.get(f"/api/v1/groups/{group.group_id}/coordination/messages?kind=task_result")
            self.assertEqual(len(resp_kind.json()["messages"]), 1)
            # single message
            resp_one = c.get(f"/api/v1/groups/{group.group_id}/coordination/messages/m.instr.1")
            self.assertEqual(resp_one.status_code, 200)
            self.assertEqual(resp_one.json()["kind"], "task_instruction")

    def test_message_chain_api(self) -> None:
        # 完整链路可追踪：通过 API 获取 instruction -> result -> run -> session -> events
        with self._with_home():
            group = _setup_group()
            self._run_full_loop(group)
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/messages/m.instr.1/chain")
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertIsNotNone(body["instruction"])
            self.assertIsNotNone(body["result"])
            self.assertIsNotNone(body["run"])
            self.assertIsNotNone(body["session"])
            self.assertTrue(body["events"])

    def test_deliveries_api(self) -> None:
        with self._with_home():
            group = _setup_group()
            self._run_full_loop(group)
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/deliveries?recipient=a2")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(len(resp.json()["deliveries"]), 1)

    def test_run_api(self) -> None:
        with self._with_home():
            group = _setup_group()
            self._run_full_loop(group)
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/runs")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(len(resp.json()["runs"]), 1)
            resp_one = c.get(f"/api/v1/groups/{group.group_id}/coordination/runs/r1")
            self.assertEqual(resp_one.status_code, 200)
            self.assertEqual(resp_one.json()["state"], "succeeded")
            resp404 = c.get(f"/api/v1/groups/{group.group_id}/coordination/runs/ghost")
            self.assertEqual(resp404.status_code, 404)

    def test_run_artifacts_api(self) -> None:
        with self._with_home():
            group = _setup_group()
            self._run_full_loop(group)
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/runs/r1/artifacts")
            self.assertEqual(resp.status_code, 200)
            kinds = sorted(a["kind"] for a in resp.json()["artifacts"])
            self.assertEqual(kinds, ["raw.stderr", "raw.stdout"])

    def test_session_api(self) -> None:
        with self._with_home():
            group = _setup_group()
            self._run_full_loop(group)
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/sessions?owner_actor_id=a2")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(len(resp.json()["sessions"]), 1)
            sid = resp.json()["sessions"][0]["session_id"]
            resp_one = c.get(f"/api/v1/groups/{group.group_id}/coordination/sessions/{sid}")
            self.assertEqual(resp_one.status_code, 200)
            self.assertEqual(resp_one.json()["owner_actor_id"], "a2")

    def test_event_query_api(self) -> None:
        with self._with_home():
            group = _setup_group()
            self._run_full_loop(group)
            c = self._client()
            resp = c.get(f"/api/v1/groups/{group.group_id}/coordination/runs/r1/events")
            self.assertEqual(resp.status_code, 200)
            events = resp.json()["events"]
            self.assertTrue(events)
            # 顺序保留
            seqs = [e["sequence"] for e in events]
            self.assertEqual(seqs, sorted(seqs))

    def test_ui_does_not_read_internal_files(self) -> None:
        # 验收：所有协调视图均可通过 API 获取，不需要直读内部文件
        with self._with_home():
            group = _setup_group()
            self._run_full_loop(group)
            c = self._client()
            base = f"/api/v1/groups/{group.group_id}/coordination"
            endpoints = [
                f"{base}/settings",
                f"{base}/actors",
                f"{base}/messages",
                f"{base}/messages/m.instr.1/chain",
                f"{base}/deliveries",
                f"{base}/runs",
                f"{base}/runs/r1/artifacts",
                f"{base}/runs/r1/events",
                f"{base}/sessions",
            ]
            for url in endpoints:
                self.assertEqual(c.get(url).status_code, 200, url)


if __name__ == "__main__":
    unittest.main()
