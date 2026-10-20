"""PowerShell guard checks only: never provision users, ACLs, or directories."""
import shutil
import subprocess
import sys
import os
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "provision-isolation-test.ps1"
PWSH = shutil.which("pwsh")
pytestmark = pytest.mark.skipif(not PWSH or sys.platform != "win32",
                               reason="Windows PowerShell provisioning guards")


def check(source):
    completed = subprocess.run(
        [PWSH, "-NoProfile", "-NonInteractive", "-Command", source],
        capture_output=True, text=True, timeout=20)
    assert completed.returncode == 0, completed.stderr
    return completed.stdout


@pytest.mark.parametrize("filename", ["provision-isolation-test.ps1", "verify-isolation-test.ps1"])
def test_isolation_test_provisioner_parses(filename):
    script = SCRIPT.with_name(filename)
    check(f"""
$tokens=$null; $errors=$null
[Management.Automation.Language.Parser]::ParseFile('{script}', [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count) {{ throw 'Parse failed' }}
""")


def test_native_acl_probe_compiles_without_execution(tmp_path):
    compiler = Path(os.environ.get("SystemRoot", r"C:\Windows")) / (
        "Microsoft.NET/Framework64/v4.0.30319/csc.exe")
    if not compiler.is_file():
        pytest.skip("Windows .NET Framework compiler unavailable")
    output = tmp_path / "probe.exe"
    completed = subprocess.run(
        [str(compiler), "/nologo", "/target:exe", f"/out:{output}",
         str(SCRIPT.with_name("isolation-probe.cs"))],
        capture_output=True, text=True, timeout=20)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert output.is_file()  # Never invoke the privileged-root probe from pytest.


@pytest.mark.parametrize("guard", ["account", "directory", "confirmation"])
def test_isolation_test_provisioner_refuses_before_mutation(guard):
    check(f"""
$ErrorActionPreference='Stop'
$global:writes=0
function Get-LocalUser {{ param($Name); {'[pscustomobject]@{Name=$Name}' if guard == 'account' else '$null'} }}
function Test-Path {{ param($LiteralPath); {'$true' if guard == 'directory' else '$false'} }}
function New-LocalUser {{ $global:writes++; throw 'Unexpected mutation' }}
function New-Item {{ $global:writes++; throw 'Unexpected mutation' }}
function Set-Acl {{ $global:writes++; throw 'Unexpected mutation' }}
$caught=$false
try {{ & '{SCRIPT}' -Apply }}
catch {{ $caught=$true }}
if (-not $caught -or $global:writes -ne 0) {{ throw 'Provisioning guard failed' }}
""")
