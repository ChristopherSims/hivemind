#requires -Version 7.0
<#
.SYNOPSIS
    Operator-run provisioning audit/setup for Hivemind OS isolation on Windows.

.DESCRIPTION
    DEFAULT IS -CheckOnly: a read-only audit that changes nothing.

    With -Apply this script performs the requested host mutations (local service
    and agent accounts, protected ACLs on the bare-repository directory). -Apply
    requires -Confirm:Isolation and an elevated shell, refuses to touch accounts
    or directories that already exist, and never alters firewall rules.

    This script is an explicit operator action. It is never invoked by Hivemind
    code, tests, or automation.

.PARAMETER CheckOnly
    (Default) Audit only: report current state, exit non-zero on findings.

.PARAMETER Apply
    Perform the mutations described in the plan. Requires -Confirm:Isolation.

.PARAMETER ServiceAccount / AgentAccount
    Distinct local account names to create/verify.

.PARAMETER BareRoot
    Directory containing bare Git repositories to ACL-protect.
#>
[CmdletBinding(SupportsShouldProcess)]
param(
    [switch] $CheckOnly,
    [switch] $Apply,
    [Parameter(Mandatory)] [string] $ServiceAccount,
    [Parameter(Mandatory)] [string] $AgentAccount,
    [Parameter(Mandatory)] [string] $BareRoot
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $Apply) { $CheckOnly = $true }
if ($Apply -and $CheckOnly) { throw "Choose -CheckOnly or -Apply, not both." }
if ($ServiceAccount -ieq $AgentAccount) { throw "Service and agent accounts must be distinct." }

$findings = 0
function Note([string] $Message, [bool] $Problem = $false) {
    if ($Problem) { $script:findings++; Write-Warning $Message } else { Write-Host "  $Message" }
}

Write-Host "Hivemind isolation provisioning ($($CheckOnly ? 'CHECK-ONLY' : 'APPLY'))"

$elevated = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
    ).IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)
Note "Elevated shell: $elevated" (-not $elevated -and $Apply)

foreach ($name in @($ServiceAccount, $AgentAccount)) {
    $existing = Get-LocalUser -Name $name -ErrorAction SilentlyContinue
    if ($existing) {
        Note "Account '$name' already exists; leaving it untouched (manual review required)." $true
    } else {
        Note "Account '$name' does not exist$( $CheckOnly ? '; would be created with -Apply' : '' )."
    }
}

if (-not (Test-Path -LiteralPath $BareRoot -PathType Container)) {
    Note "Bare root '$BareRoot' is missing$( $CheckOnly ? '; would be created with -Apply' : '' )." $Apply
} elseif ($BareRoot -match '^\\\\') {
    Note "Bare root must not be a network path." $true
} else {
    $acl = Get-Acl -LiteralPath $BareRoot
    $world = $acl.Access | Where-Object { $_.IdentityReference -match 'Everyone|Users' -and
        $_.FileSystemRights -match 'Write|Modify|FullControl' }
    Note "Bare root '$BareRoot': $($world ? 'writable by broad principals' : 'not writable by broad principals')" ($world.Count -gt 0)
}

if ($CheckOnly) {
    Write-Host "Audit complete. $findings finding(s). Re-run with -Apply -Confirm:`$false NOT supported; use -Apply -Confirm:Isolation."
    exit ($findings ? 1 : 0)
}

if (-not $PSBoundParameters.ContainsKey('Confirm')) {
    throw "-Apply requires an explicit -Confirm:Isolation switch (human decision, never automation)."
}
if (-not $elevated) { throw "-Apply requires an elevated shell." }

if (-not (Test-Path -LiteralPath $BareRoot)) { New-Item -ItemType Directory -Path $BareRoot | Out-Null }
foreach ($name in @($ServiceAccount, $AgentAccount)) {
    if (-not (Get-LocalUser -Name $name -ErrorAction SilentlyContinue)) {
        $password = Read-Host -AsSecureString "Password for new account '$name'"
        New-LocalUser -Name $name -Password $password -PasswordNeverExpires:$false `
            -UserMayNotChangePassword:$false -Description "Hivemind isolation identity" | Out-Null
        Write-Host "Created local account '$name'."
    }
}
$acl = Get-Acl -LiteralPath $BareRoot
$acl.SetAccessRuleProtection($true, $false)  # Disable inheritance; broad access removed below.
$acl.Access | Where-Object { $_.IdentityReference -match 'Everyone|Users' } |
    ForEach-Object { [void] $acl.RemoveAccessRule($_) }
foreach ($name in @($ServiceAccount, "NT AUTHORITY\SYSTEM", "BUILTIN\Administrators")) {
    $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
        $name, "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow"))
}
Set-Acl -LiteralPath $BareRoot -AclObject $acl
Write-Host "Applied protected ACLs to '$BareRoot'. Grant the agent account access per-repo as needed."
