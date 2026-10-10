import json

import pytest
from pydantic import ValidationError

from hivemind.runtime_results import structured_result
from hivemind.vendor_runtimes import FactoryRuntime


def test_explicit_message_contract_is_bounded_and_strict():
    result = structured_result("message", '{"handled":true,"reply_text":"token=private"}',
                               lambda text: text.replace("private", "[REDACTED]"))
    assert result == {"handled": True, "reply_text": "token=[REDACTED]"}
    for text in ('{"handled":"true"}', '{"handled":true,"extra":"oops"}'):
        with pytest.raises(ValidationError):
            structured_result("message", text, lambda text: text)
    with pytest.raises(ValueError):
        structured_result("message", "x" * 16385, lambda text: text)


def test_vendor_default_remains_hash_only_and_review_mode_is_explicit(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    options = {"workspace": str(workspace), "state_file": str(tmp_path / "runtime.json")}
    runtime = FactoryRuntime(options)
    runtime._save(run_id="run_test", task_id="task_test", execution_id="execution_test")
    assert "private output" not in runtime._settled("completed", "private output")["summary"]
    structured = FactoryRuntime({**options, "result_contract": "git_review", "read_only_tools": True})
    assert set(structured.tool_names) == {"Read", "Grep", "Glob", "LS"}
    verdict = {"decision": "approve", "reason": "Checked",
               "source_oid": "a" * 40, "target_oid": "b" * 40, "candidate_oid": "c" * 40}
    result = structured._settled("completed", json.dumps(verdict))
    assert result["review_verdict"]["candidate_oid"] == "c" * 40
    assert "Checked" not in (tmp_path / "runtime.json").read_text()
    no_tools = FactoryRuntime({**options, "no_tools": True})
    assert no_tools.tool_names == []
