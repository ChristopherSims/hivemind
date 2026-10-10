"""Vendor prompt consumption of canonical checkpoint/progress/handoff context."""
import json
import queue
from types import SimpleNamespace

import pytest

from hivemind.recovery import UnknownOutcome
from hivemind.vendor_runtimes import (FactoryRuntime, OhMyPiRuntime, RESUME_CONTEXT_BUDGET_BYTES,
                                      resume_context)


def options(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    config = tmp_path / "private" / "agent.json"
    config.parent.mkdir()
    config.write_text(json.dumps({"project_id": "project", "agent_id": "agent",
                                  "base_url": "http://testserver", "token": "tok-test-secret-1234"}))
    return {"workspace": str(workspace), "config": str(config),
            "state_file": str(config.parent / "runtime.json"), "timeout_seconds": 2}


def task_and_context(handoff=None, checkpoint=None, progress=40, resumed=True):
    execution = {"id": "execution", "run_id": "run", "progress": progress}
    if checkpoint is not None:
        execution["checkpoint"] = checkpoint
    task = {"id": "task", "title": "Resume task", "description": "Continue the work",
            "scope_ids": ["scope"], "execution": execution}
    context = {"project_id": "project", "agent_id": "agent", "run_id": "run",
               "execution_id": "execution", "progress": progress, "resumed": resumed,
               "handoff": handoff}
    return task, context


def accepted_handoff(summary="Phase one done", progress=40):
    return {"id": "handoff-1", "state": "accepted", "from_agent_id": "agent-prior",
            "from_run_id": "run-prior", "from_execution_id": "execution-prior",
            "accepted_execution_id": "execution", "progress": progress, "reason": "rebalance",
            "checkpoint": {"id": "checkpoint-prior", "summary": summary,
                           "evidence_refs": ["artifact-1"], "recorded_at": 1.0}}


class RecordingSession:
    prompts = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.cwd, self.id = kwargs.get("cwd"), "vendor-session"

    @classmethod
    def resume(cls, session_id, **kwargs):
        return cls(**kwargs)

    async def open(self):
        pass

    async def close(self):
        pass

    async def interrupt(self):
        pass

    def stream(self, prompt, timeout):
        RecordingSession.prompts.append(prompt)

        class Stream:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                self.result = SimpleNamespace(interrupted=False, success=True, text="done")
                raise StopAsyncIteration

        return Stream()


SDK = SimpleNamespace(Session=RecordingSession, Runtime=lambda **kw: kw,
                      SessionConfig=lambda **kw: kw, Autonomy=SimpleNamespace(LOW="low"))


class RecordingTransport:
    prompts = []

    def __init__(self, argv, cwd, env):
        self.frames = queue.Queue()

    def send(self, request):
        if request["type"] == "prompt":
            RecordingTransport.prompts.append(request["message"])
        data = {"sessionId": "omp-session", "isSettled": True} if request["type"] == "get_state" else {}
        self.frames.put({"type": "response", "id": request["id"], "success": True, "data": data})
        if request["type"] == "prompt":
            self.frames.put({"type": "prompt_result", "id": request["id"], "status": "completed",
                             "sessionSettled": True, "agentInvoked": True})

    def receive(self, timeout):
        return self.frames.get(timeout=timeout)

    def close(self):
        pass


def factory(tmp_path):
    RecordingSession.prompts.clear()
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True}, sdk=SDK)
    return runtime


def test_factory_prompt_carries_checkpoint_progress_and_handoff(tmp_path):
    runtime = factory(tmp_path)
    task, context = task_and_context(
        handoff=accepted_handoff(),
        checkpoint={"id": "checkpoint-current", "summary": "Half the files migrated",
                    "evidence_refs": ["artifact-2"], "recorded_at": 2.0})
    assert runtime.execute(task, context, None)["status"] == "completed"
    prompt = RecordingSession.prompts[-1]
    payload = json.loads(prompt[prompt.index("{"):])
    resume = payload["resume"]
    assert resume["provenance"] == "canonical_hivemind_task_record"
    assert resume["trust"] == "untrusted_evidence"
    assert resume["restore_guarantee"] == "none"
    assert "not restored vendor state" in resume["restore_note"]
    assert resume["resumed"] is True and resume["progress"] == 40
    assert resume["checkpoint"]["summary"] == "Half the files migrated"
    assert resume["checkpoint"]["provenance"] == "canonical_execution_checkpoint"
    assert resume["handoff"]["provenance"] == "canonical_scheduling_handoff"
    assert resume["handoff"]["from_execution_id"] == "execution-prior"
    assert resume["handoff"]["prior_checkpoint"]["summary"] == "Phase one done"
    assert "untrusted evidence" in prompt
    runtime.close()


def test_omp_prompt_carries_the_same_resume_block(tmp_path):
    RecordingTransport.prompts.clear()
    runtime = OhMyPiRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True},
                            transport_factory=RecordingTransport)
    task, context = task_and_context(handoff=accepted_handoff())
    assert runtime.execute(task, context, None)["status"] == "completed"
    payload = json.loads(RecordingTransport.prompts[-1][RecordingTransport.prompts[-1].index("{"):])
    assert payload["resume"]["handoff"]["id"] == "handoff-1"
    assert payload["resume"]["progress"] == 40
    runtime.close()


def test_config_credentials_are_redacted_from_prompt(tmp_path):
    runtime = factory(tmp_path)
    task, context = task_and_context(
        handoff=accepted_handoff(summary="saved token tok-test-secret-1234 and key=abc123xyz"),
        checkpoint={"id": "c", "summary": "used api_key=hunter2hunter2 and Bearer abcdef123456",
                    "evidence_refs": [], "recorded_at": 1.0})
    runtime.execute(task, context, None)
    prompt = RecordingSession.prompts[-1]
    assert "tok-test-secret-1234" not in prompt
    assert "hunter2hunter2" not in prompt
    assert "abcdef123456" not in prompt
    assert "[redacted-credential]" in prompt
    # Private state files must not leak raw vendor text either.
    assert "tok-test-secret-1234" not in runtime.path.read_text()
    runtime.close()


def test_stale_or_unaccepted_handoff_fails_closed(tmp_path):
    runtime = factory(tmp_path)
    for handoff in ({"state": "pending", "from_execution_id": "execution-prior"},
                    {**accepted_handoff(), "accepted_execution_id": "execution-other"},
                    {**accepted_handoff(), "from_execution_id": "execution"}):
        task, context = task_and_context(handoff=handoff)
        before = len(RecordingSession.prompts)
        with pytest.raises(UnknownOutcome):
            runtime.execute(task, context, None)
        assert len(RecordingSession.prompts) == before
        assert runtime.state.get("outcome") != "running"
    runtime.close()


def test_execution_from_another_execution_is_rejected(tmp_path):
    runtime = factory(tmp_path)
    task, context = task_and_context()
    task["execution"]["id"] = "execution-stale"
    with pytest.raises(UnknownOutcome):
        runtime.execute(task, context, None)
    runtime.close()


def test_oversized_handoff_is_trimmed_to_envelope(tmp_path):
    runtime = factory(tmp_path)
    huge = "x" * 100000
    task, context = task_and_context(
        handoff=accepted_handoff(summary=huge),
        checkpoint={"id": "c", "summary": huge, "evidence_refs": [], "recorded_at": 1.0})
    assert runtime.execute(task, context, None)["status"] == "completed"
    prompt = RecordingSession.prompts[-1]
    payload = json.loads(prompt[prompt.index("{"):])
    resume = payload["resume"]
    assert resume["byte_length"] <= RESUME_CONTEXT_BUDGET_BYTES
    assert len(prompt.encode()) <= 262144
    assert len(json.dumps(payload["resume"]).encode()) <= RESUME_CONTEXT_BUDGET_BYTES + 1024
    runtime.close()


def test_resume_context_budget_option_is_bounded(tmp_path):
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                              "resume_context_budget_bytes": 10}, sdk=SDK)
    task, context = task_and_context(handoff=accepted_handoff(summary="y" * 50000),
                                     checkpoint={"id": "c", "summary": "z" * 50000,
                                                 "evidence_refs": [], "recorded_at": 1.0})
    block = resume_context(task, context, budget=runtime.options["resume_context_budget_bytes"])
    assert block["byte_length"] <= 65536
    runtime.execute(task, context, None)
    payload = json.loads(RecordingSession.prompts[-1][RecordingSession.prompts[-1].index("{"):])
    assert payload["resume"]["byte_length"] <= 65536
    runtime.close()


def test_fresh_execution_claims_no_restore(tmp_path):
    runtime = factory(tmp_path)
    task, context = task_and_context(handoff=None, checkpoint=None, progress=0, resumed=False)
    assert runtime.execute(task, context, None)["status"] == "completed"
    payload = json.loads(RecordingSession.prompts[-1][RecordingSession.prompts[-1].index("{"):])
    resume = payload["resume"]
    assert resume["resumed"] is False and resume["progress"] == 0
    assert resume["checkpoint"] is None and resume["handoff"] is None
    assert resume["restore_guarantee"] == "none"
    runtime.close()


def test_evidence_unavailable_checkpoints_are_not_consumed(tmp_path):
    runtime = factory(tmp_path)
    handoff = accepted_handoff()
    handoff["checkpoint"] = {"id": "disputed", "summary": "should not appear",
                             "evidence_unavailable": True}
    task, context = task_and_context(
        handoff=handoff,
        checkpoint={"id": "disputed-current", "summary": "also hidden",
                    "evidence_unavailable": True})
    assert runtime.execute(task, context, None)["status"] == "completed"
    prompt = RecordingSession.prompts[-1]
    assert "should not appear" not in prompt and "also hidden" not in prompt
    payload = json.loads(prompt[prompt.index("{"):])
    assert payload["resume"]["checkpoint"] is None
    assert payload["resume"]["handoff"]["prior_checkpoint"] is None
    runtime.close()


def test_progress_is_clamped_and_checkpoint_summary_clipped():
    task, context = task_and_context(progress=250)
    task["execution"]["checkpoint"] = {"id": "c", "summary": "s" * 9000,
                                       "evidence_refs": ["r"] * 100, "recorded_at": 1.0}
    block = resume_context(task, context)
    assert block["progress"] == 100
    assert len(block["checkpoint"]["summary"]) <= 4096 + len("...[truncated]")
    assert len(block["checkpoint"]["evidence_refs"]) == 20
    assert block["checkpoint"]["summary"].endswith("...[truncated]")


def test_prompt_boundary_still_enforced(tmp_path):
    runtime = factory(tmp_path)
    task, context = task_and_context()
    task["description"] = "d" * 300000
    with pytest.raises(ValueError):
        runtime.execute(task, context, None)
    runtime.close()
