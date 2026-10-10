import shutil
import subprocess
import os
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parent.parent


def test_windows_installer_supplies_standard_git_when_missing_without_identity_changes(tmp_path):
    pwsh = shutil.which("pwsh")
    if not pwsh or os.name != "nt":
        pytest.skip("PowerShell deployment flow requires pwsh")
    harness = tmp_path / "installer-test.ps1"
    installer = str(ROOT / "scripts/install.ps1").replace("'", "''")
    harness.write_text("""
$ErrorActionPreference = 'Stop'
$global:installed = $false
$global:installationArguments = @()
function Get-Command {
    param($Name, $ErrorAction)
    if ($Name -eq 'git.exe' -and -not $global:installed) { return $null }
    Microsoft.PowerShell.Core\\Get-Command $Name -ErrorAction SilentlyContinue
}
function winget.exe {
    $global:installed = $true
    $global:installationArguments = $args
    $global:LASTEXITCODE = 0
}
& '""" + installer + """' -GitOnly
if (-not $global:installed -or $global:installationArguments -notcontains 'Git.Git' -or
    $global:installationArguments -notcontains '--silent') {
    throw 'Missing Git was not installed through the standard package manager'
}
Write-Output 'MOCK_INSTALL_VERIFIED'
""", encoding="utf-8")
    completed = subprocess.run([pwsh, "-NoProfile", "-File", str(harness)], capture_output=True, timeout=30)
    assert completed.returncode == 0, completed.stderr.decode(errors="replace")
    assert b"MOCK_INSTALL_VERIFIED" in completed.stdout


def test_git_deployment_missing_dependency_has_actionable_error(monkeypatch):
    from hivemind.git_collaboration import git_executable
    from hivemind.storage import Problem
    monkeypatch.setattr(shutil, "which", lambda _: None)
    with pytest.raises(Problem) as error:
        git_executable()
    assert error.value.code == "git_not_installed"
