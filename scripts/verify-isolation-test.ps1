#requires -Version 7.0
# Explicit repair/verification for this task's newly created test fixtures only.
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $ExpectedServiceSid,
    [Parameter(Mandatory)] [string] $ExpectedAgentSid,
    [Parameter(Mandatory)] [string] $EvidenceFile,
    [string] $ProbeSourceFile,
    [string] $ExpectedProbeSha256,
    [switch] $ConfirmOwnedTest
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = 'G:\Hivemind-Isolated'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
if (-not ([Security.Principal.WindowsPrincipal]::new($identity)).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)) { throw 'Administrator token required.' }
if (-not $ConfirmOwnedTest) { throw 'Explicit owned-test confirmation required.' }
if (Test-Path -LiteralPath $EvidenceFile) { throw 'Evidence output must be new.' }
$users = @(Get-LocalUser -Name 'hivemind-svc'; Get-LocalUser -Name 'hivemind-agent')
if ($users[0].SID.Value -ne $ExpectedServiceSid -or $users[1].SID.Value -ne $ExpectedAgentSid) {
    throw 'Test-account SID changed; refusing.'
}
foreach ($user in $users) {
    if ($user.Enabled -or $user.Description -ne 'Hivemind approved OS isolation test identity') {
        throw 'Refusing changed or active test identity.'
    }
}
foreach ($path in @($root, "$root\operator", "$root\tools", "$root\service", "$root\agent")) {
    if ((Get-Item -LiteralPath $path -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'Refusing reparse-point test resources.'
    }
}
$prior = Get-Content -LiteralPath "$root\operator\provision-report.json" -Raw | ConvertFrom-Json
if ($prior.scope -ne 'approved-windows-isolation-test' -or $prior.root -ne $root -or
    $prior.success -or ($prior.created_accounts -join ',') -ne 'hivemind-svc,hivemind-agent') {
    throw 'Protected provisioning evidence does not match this failed test.'
}
$probe = "$root\tools\probe.ps1"
$acl = Get-Acl -LiteralPath $probe
$allowed = @('S-1-5-18', 'S-1-5-32-544', $ExpectedServiceSid, $ExpectedAgentSid)
foreach ($rule in $acl.Access) {
    $sid = $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
    if ($sid -notin $allowed) { throw 'Unexpected trusted-probe access principal.' }
    if ($sid -in @($ExpectedServiceSid,$ExpectedAgentSid) -and
        ([int] $rule.FileSystemRights -band 0xD0116)) { throw 'Probe is writable by a test identity.' }
}
$pwsh = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
if ($ProbeSourceFile) {
    $sourceBytes = [IO.File]::ReadAllBytes($ProbeSourceFile)
    if (-not $ExpectedProbeSha256 -or
        [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($sourceBytes)) -ne $ExpectedProbeSha256) {
        throw 'Native probe source hash mismatch.'
    }
    $native = "$root\tools\isolation-probe-v2.exe"
    if (Test-Path -LiteralPath $native) { throw 'Refusing an existing compiled probe.' }
    $compiler = Join-Path $env:SystemRoot 'Microsoft.NET\Framework64\v4.0.30319\csc.exe'
    if (-not (Test-Path -LiteralPath $compiler)) { throw 'Trusted system compiler unavailable.' }
    $protectedSource = "$root\tools\isolation-probe-v2.cs"
    if (Test-Path -LiteralPath $protectedSource) { throw 'Refusing an existing probe source.' }
    [IO.File]::WriteAllBytes($protectedSource, $sourceBytes)
    & $compiler /nologo /target:exe "/out:$native" $protectedSource
    if ($LASTEXITCODE -ne 0) { throw 'Native probe compilation failed.' }
    $pwsh = $native
}
$report = [ordered]@{ scope='approved-windows-isolation-test'; root=$root; success=$false;
    probes=@(); sandbox_verified=$false;
    launcher=$(if ($ProbeSourceFile) { 'Windows .NET Framework native console probe' } else { 'Windows system PowerShell' }) }
$children = @()
$enabled = @()
try {
    for ($index=0; $index -lt 2; $index++) {
        $role = if ($index -eq 0) { 'service' } else { 'agent' }
        $expected = if ($index -eq 0) { $ExpectedServiceSid } else { $ExpectedAgentSid }
        $credential = Import-Clixml -LiteralPath "$root\operator\$role.credential.xml"
        if ($credential -isnot [Management.Automation.PSCredential] -or
            $credential.UserName -ne "$env:COMPUTERNAME\$($users[$index].Name)") {
            throw 'Protected credential identity mismatch.'
        }
        $resultPath = "$root\$role\$(if ($ProbeSourceFile) { 'probe-native-result.json' } else { 'probe-result.json' })"
        if (Test-Path -LiteralPath $resultPath) {
            throw 'Probe already produced evidence; inspect rather than repeat.'
        }
        Enable-LocalUser -InputObject $users[$index]
        $enabled += $users[$index].Name
        $arguments = if ($ProbeSourceFile) { @($role) } else {
            @('-NoProfile','-NonInteractive','-File',$probe,'-Role',$role) }
        $process = Start-Process -FilePath $pwsh -Credential $credential `
            -ArgumentList $arguments `
            -WorkingDirectory "$root\$role" -PassThru
        $children += $process
        $observed = Get-CimInstance Win32_Process -Filter "ProcessId=$($process.Id)"
        if (-not $observed) { throw 'Cannot observe probe process.' }
        $owner = Invoke-CimMethod -InputObject $observed -MethodName GetOwnerSid
        if ($owner.ReturnValue -ne 0 -or $owner.Sid -ne $expected) {
            throw 'Kernel process-owner attestation failed.'
        }
        if (-not $process.WaitForExit(30000)) { throw 'Probe deadline exceeded.' }
        if ($process.ExitCode -ne 0) { throw 'ACL probe failed.' }
        $result = Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
        if ($result.sid -ne $expected -or $result.pid -ne $process.Id -or
            -not ($result.own_write -and $result.cross_write_denied -and
                  $result.service_secret_read_denied -and $result.trusted_tools_write_denied)) {
            throw 'Probe evidence mismatch.'
        }
        $report.probes += $result
    }
    $report.success = $true
} catch {
    $report.error_type = $_.Exception.GetType().Name
    if ($_.InvocationInfo.MyCommand) { $report.error_command = $_.InvocationInfo.MyCommand.Name }
    $report.error_line = $_.InvocationInfo.ScriptLineNumber
    $exception = $_.Exception
    while ($exception.InnerException) { $exception = $exception.InnerException }
    if ($exception -is [ComponentModel.Win32Exception]) {
        $report.native_error_code = $exception.NativeErrorCode
    }
    foreach ($name in $enabled) { Disable-LocalUser -Name $name }
    $report.failed_accounts_disabled = $true
} finally {
    foreach ($child in $children) {
        if (-not $child.HasExited) { $child.Kill($true); $child.WaitForExit(10000) | Out-Null }
    }
    $report | ConvertTo-Json -Depth 8 |
        Out-File -LiteralPath "$root\operator\$(Split-Path -Leaf $EvidenceFile)" -NoClobber -Encoding utf8
    $report | ConvertTo-Json -Depth 8 | Out-File -LiteralPath $EvidenceFile -NoClobber -Encoding utf8
}
$report | ConvertTo-Json -Depth 8
exit ($report.success ? 0 : 1)
