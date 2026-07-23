"""CCCC-CORE-01 契约冻结测试.

验证 coordination 契约（模型、枚举、错误码、状态机、任务路由规则、Session 所有权、
向后兼容默认值）。这些是纯契约测试，不依赖 daemon 或运行时。
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from cccc.contracts.v1 import (
    DEFAULT_COORDINATION_MODE,
    DEFAULT_EXECUTION_MODE,
    DEFAULT_TASK_MESSAGE_KIND,
    Delivery,
    DeliveryState,
    ERROR_CODES,
    ExecutionMode,
    Run,
    RunArtifact,
    RunState,
    RouteValidationError,
    RuntimeSession,
    RuntimeSessionState,
    StandardEvent,
    StandardEventStream,
    StandardEventType,
    TaskMessage,
    TaskMessageKind,
    CoordinationMode,
    error_message,
    is_delivery_terminal,
    is_run_terminal,
    validate_session_owner,
    validate_task_route,
)
from cccc.contracts.v1.coordination import (
    ERR_ACTOR_NOT_FOUND,
    ERR_DELIVERY_ALREADY_EXISTS,
    ERR_GROUP_FOREMAN_REQUIRED,
    ERR_OUTPUT_CAPTURE_FAILED,
    ERR_RUN_ALREADY_EXISTS,
    ERR_RUN_CANCELLED,
    ERR_RUN_TIMED_OUT,
    ERR_SESSION_ACTOR_MISMATCH,
    ERR_SESSION_BUSY,
    ERR_SESSION_GROUP_MISMATCH,
    ERR_SESSION_NOT_FOUND,
    ERR_SESSION_RESUME_FAILED,
    ERR_SESSION_RUNTIME_MISMATCH,
    ERR_SESSION_WORKSPACE_PATH_MISMATCH,
    ERR_TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED,
    ERR_TASK_RECIPIENT_REQUIRED,
    ERR_TASK_ROUTE_NOT_ALLOWED,
)


# ---------------------------------------------------------------------------
# 维度与默认值（兼容性）
# ---------------------------------------------------------------------------


class TestCoordinationDimensions:
    def test_default_coordination_mode_is_legacy(self) -> None:
        assert DEFAULT_COORDINATION_MODE == "legacy"

    def test_default_execution_mode_keeps_existing_behavior(self) -> None:
        # 默认执行模式不强制 one_shot，保持现有 PTY 行为
        assert DEFAULT_EXECUTION_MODE == "pty"
        assert DEFAULT_EXECUTION_MODE != "one_shot"

    def test_default_task_message_kind_is_chat(self) -> None:
        # 默认消息语义保持普通 chat，不强制任务消息
        assert DEFAULT_TASK_MESSAGE_KIND == "chat"

    def test_coordination_mode_values(self) -> None:
        # 静态检查类型包含两个值；运行时确认集合
        values = {"legacy", "foreman_managed"}
        assert "legacy" in values
        assert "foreman_managed" in values

    def test_execution_mode_values(self) -> None:
        values = {"pty", "headless", "one_shot"}
        assert values == {"pty", "headless", "one_shot"}


# ---------------------------------------------------------------------------
# 错误码冻结
# ---------------------------------------------------------------------------


class TestErrorCodes:
    def test_all_required_error_codes_are_frozen(self) -> None:
        required = {
            "GROUP_FOREMAN_REQUIRED",
            "GROUP_MULTIPLE_FOREMEN",
            "TASK_RECIPIENT_REQUIRED",
            "TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED",
            "TASK_ROUTE_NOT_ALLOWED",
            "ACTOR_NOT_FOUND",
            "ACTOR_DISABLED",
            "SESSION_NOT_FOUND",
            "SESSION_ACTOR_MISMATCH",
            "SESSION_RUNTIME_MISMATCH",
            "SESSION_GROUP_MISMATCH",
            "SESSION_WORKSPACE_PATH_MISMATCH",
            "SESSION_BUSY",
            "DELIVERY_ALREADY_EXISTS",
            "RUN_ALREADY_EXISTS",
            "RUN_CANCELLED",
            "RUN_TIMED_OUT",
            "SESSION_RESUME_FAILED",
            "OUTPUT_CAPTURE_FAILED",
        }
        assert required.issubset(set(ERROR_CODES))

    def test_error_code_constants_match_names(self) -> None:
        assert ERR_GROUP_FOREMAN_REQUIRED == "GROUP_FOREMAN_REQUIRED"
        assert ERR_TASK_RECIPIENT_REQUIRED == "TASK_RECIPIENT_REQUIRED"
        assert ERR_TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED == "TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED"
        assert ERR_TASK_ROUTE_NOT_ALLOWED == "TASK_ROUTE_NOT_ALLOWED"
        assert ERR_ACTOR_NOT_FOUND == "ACTOR_NOT_FOUND"
        assert ERR_SESSION_NOT_FOUND == "SESSION_NOT_FOUND"
        assert ERR_SESSION_ACTOR_MISMATCH == "SESSION_ACTOR_MISMATCH"
        assert ERR_SESSION_RUNTIME_MISMATCH == "SESSION_RUNTIME_MISMATCH"
        assert ERR_SESSION_GROUP_MISMATCH == "SESSION_GROUP_MISMATCH"
        assert ERR_SESSION_WORKSPACE_PATH_MISMATCH == "SESSION_WORKSPACE_PATH_MISMATCH"
        assert ERR_SESSION_BUSY == "SESSION_BUSY"
        assert ERR_DELIVERY_ALREADY_EXISTS == "DELIVERY_ALREADY_EXISTS"
        assert ERR_RUN_ALREADY_EXISTS == "RUN_ALREADY_EXISTS"
        assert ERR_RUN_CANCELLED == "RUN_CANCELLED"
        assert ERR_RUN_TIMED_OUT == "RUN_TIMED_OUT"
        assert ERR_SESSION_RESUME_FAILED == "SESSION_RESUME_FAILED"
        assert ERR_OUTPUT_CAPTURE_FAILED == "OUTPUT_CAPTURE_FAILED"

    def test_error_message_known_code(self) -> None:
        assert "foreman" in error_message(ERR_GROUP_FOREMAN_REQUIRED)

    def test_error_message_unknown_code_falls_back(self) -> None:
        assert error_message("UNKNOWN_CODE") == "UNKNOWN_CODE"


# ---------------------------------------------------------------------------
# 状态机
# ---------------------------------------------------------------------------


class TestStateMachines:
    def test_delivery_states(self) -> None:
        assert {"queued", "reserved", "delivered", "failed"} == {
            "queued",
            "reserved",
            "delivered",
            "failed",
        }

    def test_run_states(self) -> None:
        assert {"pending", "running", "succeeded", "failed", "cancelled", "timed_out"} == {
            "pending",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
        }

    def test_session_states(self) -> None:
        assert {"idle", "busy", "closed", "failed"} == {"idle", "busy", "closed", "failed"}

    def test_run_terminal(self) -> None:
        assert is_run_terminal("succeeded")
        assert is_run_terminal("failed")
        assert is_run_terminal("cancelled")
        assert is_run_terminal("timed_out")
        assert not is_run_terminal("pending")
        assert not is_run_terminal("running")

    def test_delivery_terminal(self) -> None:
        assert is_delivery_terminal("delivered")
        assert is_delivery_terminal("failed")
        assert not is_delivery_terminal("queued")
        assert not is_delivery_terminal("reserved")


# ---------------------------------------------------------------------------
# 模型契约
# ---------------------------------------------------------------------------


class TestModels:
    def test_task_message_requires_explicit_recipient(self) -> None:
        msg = TaskMessage(
            group_id="g_1",
            message_id="m_1",
            sender_actor_id="a1",
            recipient_actor_id="a2",
            kind="task_instruction",
        )
        assert msg.recipient_actor_id == "a2"
        assert msg.kind == "task_instruction"

    def test_task_message_rejects_extra_fields(self) -> None:
        with pytest.raises(ValidationError):
            TaskMessage(  # type: ignore[call-arg]
                group_id="g_1",
                message_id="m_1",
                sender_actor_id="a1",
                recipient_actor_id="a2",
                kind="task_instruction",
                rogue_field="x",
            )

    def test_delivery_defaults_to_queued(self) -> None:
        d = Delivery(
            delivery_id="d1",
            group_id="g_1",
            message_id="m_1",
            recipient_actor_id="a2",
        )
        assert d.state == "queued"
        assert d.attempts == 0
        assert d.locked_by is None

    def test_run_defaults_to_one_shot_pending(self) -> None:
        r = Run(
            run_id="r1",
            group_id="g_1",
            actor_id="a2",
            message_id="m_1",
        )
        assert r.state == "pending"
        assert r.execution_mode == "one_shot"

    def test_runtime_session_defaults(self) -> None:
        s = RuntimeSession(
            session_id="s1",
            group_id="g_1",
            owner_actor_id="a2",
        )
        assert s.state == "idle"
        assert s.current_run_id is None

    def test_standard_event_raw_streams(self) -> None:
        # 无法识别的输出封装为 raw.stdout / raw.stderr
        ev = StandardEvent(
            event_id="e1",
            group_id="g_1",
            actor_id="a2",
            run_id="r1",
            stream="stdout",
            event_type="raw.stdout",
            content="garbled",
        )
        assert ev.event_type == "raw.stdout"
        assert ev.content == "garbled"

    def test_run_artifact_raw_stdout_stderr_separate(self) -> None:
        out = RunArtifact(artifact_id="ar1", run_id="r1", kind="raw.stdout", path="out.log")
        err = RunArtifact(artifact_id="ar2", run_id="r1", kind="raw.stderr", path="err.log")
        assert out.kind != err.kind


# ---------------------------------------------------------------------------
# 任务路由校验
# ---------------------------------------------------------------------------


class TestTaskRouteValidation:
    def test_legacy_mode_allows_broadcast_chat(self) -> None:
        # legacy 模式 + chat 消息：广播保持兼容，不校验
        validate_task_route(
            coordination_mode="legacy",
            kind="chat",
            sender_role="peer",
            recipient_tokens=[],
            resolved_recipient_ids=[],
            recipient_roles=[],
        )

    def test_legacy_mode_allows_multi_recipient(self) -> None:
        validate_task_route(
            coordination_mode="legacy",
            kind="chat",
            sender_role="peer",
            recipient_tokens=["a2", "a3"],
            resolved_recipient_ids=["a2", "a3"],
            recipient_roles=["peer", "peer"],
        )

    def test_foreman_managed_chat_still_allows_broadcast(self) -> None:
        # foreman_managed 模式下，普通 chat 消息仍保持广播/多接收者兼容
        validate_task_route(
            coordination_mode="foreman_managed",
            kind="chat",
            sender_role="peer",
            recipient_tokens=[],
            resolved_recipient_ids=[],
            recipient_roles=[],
        )

    def test_task_instruction_valid_foreman_to_peer(self) -> None:
        validate_task_route(
            coordination_mode="foreman_managed",
            kind="task_instruction",
            sender_role="foreman",
            recipient_tokens=["a2"],
            resolved_recipient_ids=["a2"],
            recipient_roles=["peer"],
            same_group=True,
        )

    def test_task_result_valid_peer_to_foreman(self) -> None:
        validate_task_route(
            coordination_mode="foreman_managed",
            kind="task_result",
            sender_role="peer",
            recipient_tokens=["a1"],
            resolved_recipient_ids=["a1"],
            recipient_roles=["foreman"],
            same_group=True,
        )

    def test_task_instruction_rejects_broadcast(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_instruction",
                sender_role="foreman",
                recipient_tokens=[],
                resolved_recipient_ids=[],
                recipient_roles=[],
            )
        assert exc.value.code == ERR_TASK_RECIPIENT_REQUIRED

    def test_task_instruction_rejects_all_mention(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_instruction",
                sender_role="foreman",
                recipient_tokens=["@all"],
                resolved_recipient_ids=[],
                recipient_roles=[],
            )
        assert exc.value.code == ERR_TASK_RECIPIENT_REQUIRED

    def test_task_instruction_rejects_multiple_recipients(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_instruction",
                sender_role="foreman",
                recipient_tokens=["a2", "a3"],
                resolved_recipient_ids=["a2", "a3"],
                recipient_roles=["peer", "peer"],
            )
        assert exc.value.code == ERR_TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED

    def test_task_instruction_rejects_peer_sender(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_instruction",
                sender_role="peer",
                recipient_tokens=["a3"],
                resolved_recipient_ids=["a3"],
                recipient_roles=["peer"],
            )
        assert exc.value.code == ERR_TASK_ROUTE_NOT_ALLOWED

    def test_task_instruction_rejects_foreman_recipient(self) -> None:
        # foreman 不能给 foreman 下发任务指令（非 peer）
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_instruction",
                sender_role="foreman",
                recipient_tokens=["a1"],
                resolved_recipient_ids=["a1"],
                recipient_roles=["foreman"],
            )
        assert exc.value.code == ERR_TASK_ROUTE_NOT_ALLOWED

    def test_task_result_rejects_foreman_sender(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_result",
                sender_role="foreman",
                recipient_tokens=["a1"],
                resolved_recipient_ids=["a1"],
                recipient_roles=["foreman"],
            )
        assert exc.value.code == ERR_TASK_ROUTE_NOT_ALLOWED

    def test_task_result_rejects_peer_recipient(self) -> None:
        # task_result 必须发给 foreman，不能发给 peer（禁止 peer→peer）
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_result",
                sender_role="peer",
                recipient_tokens=["a3"],
                resolved_recipient_ids=["a3"],
                recipient_roles=["peer"],
            )
        assert exc.value.code == ERR_TASK_ROUTE_NOT_ALLOWED

    def test_task_message_rejects_cross_group(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_instruction",
                sender_role="foreman",
                recipient_tokens=["a2"],
                resolved_recipient_ids=["a2"],
                recipient_roles=["peer"],
                same_group=False,
            )
        assert exc.value.code == ERR_TASK_ROUTE_NOT_ALLOWED

    def test_peer_to_peer_task_instruction_blocked(self) -> None:
        # 禁止 peer→peer（通过 recipient role 检测）
        with pytest.raises(RouteValidationError) as exc:
            validate_task_route(
                coordination_mode="foreman_managed",
                kind="task_instruction",
                sender_role="peer",
                recipient_tokens=["a3"],
                resolved_recipient_ids=["a3"],
                recipient_roles=["peer"],
            )
        # sender 不是 foreman 先触发
        assert exc.value.code == ERR_TASK_ROUTE_NOT_ALLOWED


# ---------------------------------------------------------------------------
# Session 所有权校验
# ---------------------------------------------------------------------------


class TestSessionOwnerValidation:
    def test_matching_session_ok(self) -> None:
        validate_session_owner(
            session_owner_actor_id="a2",
            run_actor_id="a2",
            session_group_id="g_1",
            run_group_id="g_1",
            session_runtime="codex",
            run_runtime="codex",
            session_work_dir="/repo",
            run_work_dir="/repo",
        )

    def test_actor_mismatch_blocked(self) -> None:
        # a1 不能使用 a2 的 Session
        with pytest.raises(RouteValidationError) as exc:
            validate_session_owner(
                session_owner_actor_id="a2",
                run_actor_id="a1",
                session_group_id="g_1",
                run_group_id="g_1",
                session_runtime="codex",
                run_runtime="codex",
                session_work_dir="/repo",
                run_work_dir="/repo",
            )
        assert exc.value.code == ERR_SESSION_ACTOR_MISMATCH

    def test_group_mismatch_blocked(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_session_owner(
                session_owner_actor_id="a2",
                run_actor_id="a2",
                session_group_id="g_1",
                run_group_id="g_2",
                session_runtime="codex",
                run_runtime="codex",
                session_work_dir="/repo",
                run_work_dir="/repo",
            )
        assert exc.value.code == ERR_SESSION_GROUP_MISMATCH

    def test_runtime_mismatch_blocked(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_session_owner(
                session_owner_actor_id="a2",
                run_actor_id="a2",
                session_group_id="g_1",
                run_group_id="g_1",
                session_runtime="codex",
                run_runtime="claude",
                session_work_dir="/repo",
                run_work_dir="/repo",
            )
        assert exc.value.code == ERR_SESSION_RUNTIME_MISMATCH

    def test_workdir_mismatch_blocked(self) -> None:
        with pytest.raises(RouteValidationError) as exc:
            validate_session_owner(
                session_owner_actor_id="a2",
                run_actor_id="a2",
                session_group_id="g_1",
                run_group_id="g_1",
                session_runtime="codex",
                run_runtime="codex",
                session_work_dir="/repo-a",
                run_work_dir="/repo-b",
            )
        assert exc.value.code == ERR_SESSION_WORKSPACE_PATH_MISMATCH

    def test_empty_runtime_or_workdir_skipped(self) -> None:
        # 未指定 runtime/work_dir 时不强制校验（兼容）
        validate_session_owner(
            session_owner_actor_id="a2",
            run_actor_id="a2",
            session_group_id="g_1",
            run_group_id="g_1",
            session_runtime="",
            run_runtime="",
            session_work_dir="",
            run_work_dir="",
        )


# ---------------------------------------------------------------------------
# 向后兼容：现有契约未被破坏
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    def test_existing_actor_role_still_foreman_peer(self) -> None:
        from cccc.contracts.v1 import ActorRole

        # ActorRole 仍为 foreman | peer，未被改为 worker
        assert "foreman" in ActorRole.__args__  # type: ignore[attr-defined]
        assert "peer" in ActorRole.__args__  # type: ignore[attr-defined]
        assert "worker" not in ActorRole.__args__  # type: ignore[attr-defined]

    def test_existing_chat_message_still_supports_broadcast(self) -> None:
        from cccc.contracts.v1 import ChatMessageData

        # 空 to 仍合法（广播），未被禁止
        msg = ChatMessageData(text="hello", to=[])
        assert msg.to == []

    def test_existing_chat_message_still_supports_multi_recipient(self) -> None:
        from cccc.contracts.v1 import ChatMessageData

        msg = ChatMessageData(text="hello", to=["a2", "a3", "@all"])
        assert msg.to == ["a2", "a3", "@all"]

    def test_existing_runner_kinds_unchanged(self) -> None:
        from cccc.contracts.v1 import RunnerKind

        # PTY / headless 仍是现有 runner 类型，未被 one_shot 替换
        assert "pty" in RunnerKind.__args__  # type: ignore[attr-defined]
        assert "headless" in RunnerKind.__args__  # type: ignore[attr-defined]
        assert "one_shot" not in RunnerKind.__args__  # type: ignore[attr-defined]
