"""All supported protocol/vendor prompts share this bounded evidence envelope."""
import pytest

from hivemind.recovery import UnknownOutcome
from hivemind.storage import canonical
from hivemind.vendor_runtimes import resume_context


def records():
    task = {"id": "task_one", "execution": {"id": "execution_new", "progress": 0}}
    context = {"run_id": "run_recipient", "execution_id": "execution_new",
               "structured_handoff": {
                   "task_id": "task_one", "handoff_id": "structured_handoff_one",
                   "recipient_run_id": "run_recipient", "recipient_execution_id": "execution_new",
                   "sender_agent_id": "agent_sender", "source_execution_id": "execution_old",
                   "summary": "Prior attempt found a missing artifact.",
                   "context_refs": [{"kind": "task", "id": "task_source", "revision": 1}],
                   "deliverables": [{"kind": "artifact", "id": "artifact_one", "revision": 2,
                                     "note": "password=not-a-real-credential"}],
                   "checkpoint": {"id": "checkpoint_old", "summary": "Checkpoint evidence"},
                   "unresolved_questions": ["Which output is required?"],
                   "authority": "canonical_accepted_handoff"}}
    return task, context


def test_recipient_accepted_facts_are_labeled_evidence_not_native_resume():
    task, context = records()
    block = resume_context(task, context)
    transferred = block["structured_handoff"]
    assert transferred["summary"] == context["structured_handoff"]["summary"]
    assert transferred["unresolved_questions"] == ["Which output is required?"]
    assert transferred["trust"] == "untrusted_evidence"
    assert transferred["restore_guarantee"] == "none"
    assert "not-a-real-credential" not in canonical(block)


@pytest.mark.parametrize("field,value", [
    ("task_id", "task_other"), ("recipient_run_id", "run_other"),
    ("recipient_execution_id", "execution_other"), ("authority", "self_report")])
def test_wrong_binding_never_reaches_a_vendor_prompt(field, value):
    task, context = records()
    context["structured_handoff"][field] = value
    with pytest.raises(UnknownOutcome):
        resume_context(task, context)


def test_context_overflow_is_explicit_and_bounded():
    task, context = records()
    context["structured_handoff"]["summary"] = "Evidence " * 800
    context["structured_handoff"]["unresolved_questions"] = ["Question " * 200] * 8
    block = resume_context(task, context, budget=1024)
    assert len(canonical(block).encode()) <= 1024
    assert ("structured_handoff" in block
            or block.get("structured_handoff_omitted") == "resume_context_budget")
