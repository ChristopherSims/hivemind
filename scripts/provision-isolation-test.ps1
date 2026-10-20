#requires -Version 7.0
<#
Creates only the explicitly approved disposable Windows isolation-test identities
and root. No existing accounts/directories are modified, no firewall or logon
policy is changed. Run elevated with -Apply -ConfirmProvisioning.
Passwords are generated in memory and saved only as DPAPI-encrypted PSCredentials
under an Administrators/SYSTEM-only directory. Never print or copy these files.
This tests identities and ACLs, not vendor filesystem/network sandboxing.
#>
[CmdletBinding()]
param(
    [switch] $Apply,
    [switch] $ConfirmProvisioning,
    [string] $EvidenceFile
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Root = 'G:\Hivemind-Isolated'
$Names = @('hivemind-svc', 'hivemind-agent')
$Admin = [Security.Principal.SecurityIdentifier]::new('S-1-5-32-544')
$System = [Security.Principal.SecurityIdentifier]::new('S-1-5-18')
$Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$Elevated = ([Security.Principal.WindowsPrincipal]::new($Identity)).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)

# Resolve/validate everything before creating any security principal.
foreach ($name in $Names) {
    if (Get-LocalUser -Name $name -ErrorAction SilentlyContinue) {
        throw "Refusing existing account: $name"
    }
}
if (Test-Path -LiteralPath $Root) { throw "Refusing existing isolation-test root." }
if (-not $Apply) {
    [ordered]@{ root = $Root; accounts = $Names; elevated = $Elevated; apply = $false } |
        ConvertTo-Json
    exit 0
}
if (-not $ConfirmProvisioning) { throw 'Apply requires -ConfirmProvisioning.' }
if (-not $Elevated) { throw 'Apply requires an administrator token.' }
if ((Get-Item -LiteralPath 'G:\').Attributes -band [IO.FileAttributes]::ReparsePoint) {
    throw 'Refusing a reparse-point drive root.'
}
if ($EvidenceFile) {
    $EvidenceFile = [IO.Path]::GetFullPath($EvidenceFile)
    if (Test-Path -LiteralPath $EvidenceFile) { throw 'Evidence output must be new.' }
    if (-not (Test-Path -LiteralPath (Split-Path -Parent $EvidenceFile) -PathType Container)) {
        throw 'Evidence output parent must already exist.'
    }
}

function Set-ExactAcl([string] $Path, [hashtable] $Grants, [object] $Owner = $Admin) {
    $acl = [Security.AccessControl.DirectorySecurity]::new()
    $acl.SetAccessRuleProtection($true, $false)
    $acl.SetOwner($Owner)
    foreach ($entry in $Grants.GetEnumerator()) {
        $sid = [Security.Principal.SecurityIdentifier]::new([string] $entry.Key)
        $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
            $sid, $entry.Value, 'ContainerInherit,ObjectInherit', 'None', 'Allow'))
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}

function New-Password {
    $bytes = [Security.Cryptography.RandomNumberGenerator]::GetBytes(48)
    # Required character classes without weakening the random component.
    ConvertTo-SecureString ('Aa1!' + [Convert]::ToBase64String($bytes)) -AsPlainText -Force
}

$created = @()
$children = @()
$report = [ordered]@{
    scope = 'approved-windows-isolation-test'; root = $Root; accounts = $Names
    success = $false; created_accounts = @(); probes = @()
    sandbox_verified = $false; credentials = 'DPAPI encrypted; operator-only ACL'
}
try {
    New-Item -ItemType Directory -Path $Root | Out-Null
    Set-ExactAcl $Root @{ $Admin.Value = 'FullControl'; $System.Value = 'FullControl' }
    $credentials = @()
    foreach ($name in $Names) {
        $password = New-Password
        $user = New-LocalUser -Name $name -Password $password `
            -Description 'Hivemind approved OS isolation test identity' `
            -UserMayNotChangePassword:$true
        $created += $name
        $report.created_accounts = $created
        # Standard Users only, never Administrators or a privileged group.
        Add-LocalGroupMember -SID 'S-1-5-32-545' -Member $user
        $credentials += [Management.Automation.PSCredential]::new(
            "$env:COMPUTERNAME\$name", $password)
    }
    $svc = (Get-LocalUser -Name $Names[0]).SID
    $agent = (Get-LocalUser -Name $Names[1]).SID
    if ($svc -eq $agent) { throw 'Identities are not distinct.' }
    $base = @{ $Admin.Value = 'FullControl'; $System.Value = 'FullControl' }
    # Public traversal does not grant access to protected descendants.
    Set-ExactAcl $Root ($base + @{ $svc.Value = 'ReadAndExecute'; $agent.Value = 'ReadAndExecute' })
    foreach ($dir in @('service', 'agent', 'operator', 'tools')) {
        New-Item -ItemType Directory -Path "$Root\$dir" | Out-Null
    }
    Set-ExactAcl "$Root\service" ($base + @{ $svc.Value = 'FullControl' }) $svc
    Set-ExactAcl "$Root\agent" ($base + @{ $agent.Value = 'FullControl' }) $agent
    Set-ExactAcl "$Root\operator" $base
    Set-ExactAcl "$Root\tools" ($base + @{ $svc.Value = 'ReadAndExecute'; $agent.Value = 'ReadAndExecute' })
    foreach ($dir in @('git', 'state', 'credentials')) {
        New-Item -ItemType Directory -Path "$Root\service\$dir" | Out-Null
    }
    Set-Content -LiteralPath "$Root\service\credentials\sentinel.txt" -Value 'ACL_TEST_NOT_A_SECRET'
    $credentials[0] | Export-Clixml -LiteralPath "$Root\operator\service.credential.xml"
    $credentials[1] | Export-Clixml -LiteralPath "$Root\operator\agent.credential.xml"

    $probe = @'
param([ValidateSet('service','agent')] [string] $Role)
$ErrorActionPreference = 'Stop'
$root = 'G:\Hivemind-Isolated'
$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
# Give the trusted elevated parent time to independently attest this PID's owner.
Start-Sleep -Seconds 2
$own = "$root\$Role"
Set-Content -LiteralPath "$own\own-write.txt" -Value 'EXPECTED_OWN_WRITE'
$other = if ($Role -eq 'service') { "$root\agent" } else { "$root\service" }
$denied = $false
try { Set-Content -LiteralPath "$other\forbidden-write.txt" -Value 'MUST_NOT_WRITE' }
catch [UnauthorizedAccessException] { $denied = $true }
catch [System.Security.SecurityException] { $denied = $true }
$secretDenied = $false
if ($Role -eq 'agent') {
    try { $null = Get-Content -LiteralPath "$root\service\credentials\sentinel.txt" }
    catch [UnauthorizedAccessException] { $secretDenied = $true }
    catch [System.Security.SecurityException] { $secretDenied = $true }
} else { $secretDenied = $true }
$toolsDenied = $false
try { Set-Content -LiteralPath "$root\tools\forbidden-write.txt" -Value 'MUST_NOT_WRITE' }
catch [UnauthorizedAccessException] { $toolsDenied = $true }
catch [System.Security.SecurityException] { $toolsDenied = $true }
[ordered]@{ role=$Role; sid=$sid; pid=$PID; own_write=$true;
    cross_write_denied=$denied; service_secret_read_denied=$secretDenied;
    trusted_tools_write_denied=$toolsDenied } |
    ConvertTo-Json | Set-Content -LiteralPath "$own\probe-result.json"
if (-not ($denied -and $secretDenied -and $toolsDenied)) { exit 3 }
'@
    Set-Content -LiteralPath "$Root\tools\probe.ps1" -Value $probe
    # Packaged PowerShell in WindowsApps is registered per user and cannot be
    # launched under a freshly provisioned identity. The probe is PS 5.1 compatible.
    $pwsh = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    if (-not (Test-Path -LiteralPath $pwsh -PathType Leaf)) { throw 'System probe launcher is missing.' }
    for ($index = 0; $index -lt 2; $index++) {
        $role = if ($index -eq 0) { 'service' } else { 'agent' }
        $expected = if ($index -eq 0) { $svc.Value } else { $agent.Value }
        $process = Start-Process -FilePath $pwsh -Credential $credentials[$index] `
            -ArgumentList @('-NoProfile', '-NonInteractive', '-File', "$Root\tools\probe.ps1", '-Role', $role) `
            -WorkingDirectory "$Root\$role" -PassThru
        $children += $process
        $osProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$($process.Id)"
        if (-not $osProcess) { throw 'Cannot independently observe probe process.' }
        $owner = Invoke-CimMethod -InputObject $osProcess -MethodName GetOwnerSid
        if ($owner.ReturnValue -ne 0 -or $owner.Sid -ne $expected) {
            throw 'OS process-owner attestation failed.'
        }
        if (-not $process.WaitForExit(30000)) { throw 'Bounded identity probe timed out.' }
        if ($process.ExitCode -ne 0) { throw 'Identity/ACL probe failed.' }
        $result = Get-Content -LiteralPath "$Root\$role\probe-result.json" -Raw | ConvertFrom-Json
        if ($result.sid -ne $expected -or $result.pid -ne $process.Id) {
            throw 'Probe evidence does not match observed process identity.'
        }
        $report.probes += $result
    }
    $report.success = $true
} catch {
    # Keep partial artifacts for diagnosis. Never remove/rewrite preexisting state.
    # Disable only accounts created by this invocation on provisioning failure.
    foreach ($name in $created) { Disable-LocalUser -Name $name -ErrorAction SilentlyContinue }
    $report.error_type = $_.Exception.GetType().Name
    if ($_.InvocationInfo.MyCommand) { $report.error_command = $_.InvocationInfo.MyCommand.Name }
    $report.error_line = $_.InvocationInfo.ScriptLineNumber
    $report.failed_accounts_disabled = $true
} finally {
    foreach ($child in $children) {
        if (-not $child.HasExited) { $child.Kill($true); $child.WaitForExit(10000) | Out-Null }
    }
    if (Test-Path -LiteralPath "$Root\operator") {
        $report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath "$Root\operator\provision-report.json"
    }
    if ($EvidenceFile) {
        # Redacted result only. Never copy credentials, raw stderr, or passwords.
        $report | ConvertTo-Json -Depth 8 | Out-File -LiteralPath $EvidenceFile -NoClobber -Encoding utf8
    }
}
$report | ConvertTo-Json -Depth 8
exit ($report.success ? 0 : 1)
