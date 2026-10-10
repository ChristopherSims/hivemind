import sys

from scripts.factory_integration import run_command


def test_bounded_command_can_be_interrupted_and_records_it(tmp_path):
    sentinel = tmp_path / "ready"
    result = run_command(
        [sys.executable, "-c",
         "import pathlib,time; pathlib.Path(" + repr(str(sentinel)) + ").write_text('ready'); time.sleep(30)"],
        tmp_path, None, tmp_path / "interrupted", 10, interrupt_when=sentinel.exists)
    assert result["interrupted"] and not result["timed_out"]
    assert result["exit_code"] != 0
    assert result["duration_seconds"] < 10


def test_normal_completion_is_not_reported_as_interruption(tmp_path):
    result = run_command([sys.executable, "-c", "print('complete')"],
                         tmp_path, None, tmp_path / "complete", 10)
    assert result["exit_code"] == 0
    assert not result["interrupted"] and not result["timed_out"]
