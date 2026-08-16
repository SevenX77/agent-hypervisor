#!/usr/bin/env pwsh
# SPDX-License-Identifier: MIT

[CmdletBinding()]
param(
    [switch]$Check,

    [switch]$Fix,

    [switch]$Yes,

    [switch]$Json,

    [switch]$Resume,

    [switch]$DryRun,

    [string]$Distro = 'Ubuntu',

    [string]$AhInstallUrl,

    [string]$ExpectedAhVersion = $env:AH_SETUP_EXPECTED_VERSION,

    [string]$StatePath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$modulePath = Join-Path $PSScriptRoot 'AhProvisioning.psm1'
Import-Module $modulePath -Force

$invokeArgs = @{
    Check = $Check
    Fix = $Fix
    Yes = $Yes
    Resume = $Resume
    DryRun = $DryRun
    SelectedDistro = $Distro
}

if (-not [string]::IsNullOrWhiteSpace($StatePath)) {
    $invokeArgs.StatePath = $StatePath
}
if (-not [string]::IsNullOrWhiteSpace($AhInstallUrl)) {
    $invokeArgs.AhInstallUrl = $AhInstallUrl
}
if (-not [string]::IsNullOrWhiteSpace($ExpectedAhVersion)) {
    $invokeArgs.ExpectedAhVersion = $ExpectedAhVersion
}

$envelope = Invoke-AhPhase2Provisioning @invokeArgs

if ($Json) {
    $envelope | ConvertTo-Json -Depth 16
} else {
    Write-Output "ah setup phase2: $($envelope.overall_status)"
    if ($null -ne $envelope.next_action -and -not [string]::IsNullOrWhiteSpace($envelope.next_action.message)) {
        Write-Output $envelope.next_action.message
    }
    if ($null -ne $envelope.next_action -and -not [string]::IsNullOrWhiteSpace($envelope.next_action.command)) {
        Write-Output "next: $($envelope.next_action.command)"
    }
    if (-not [string]::IsNullOrWhiteSpace($envelope.resume_command)) {
        Write-Output "resume: $($envelope.resume_command)"
    }
    foreach ($step in @($envelope.steps)) {
        Write-Output "$($step.status) $($step.id): $($step.detail)"
    }
}

switch ($envelope.overall_status) {
    'pass' { exit 0 }
    'fixed' { exit 0 }
    'needs_wsl_shutdown' { exit 10 }
    'needs_windows_reboot' { exit 11 }
    'needs_distro_first_launch' { exit 12 }
    'permission_denied' { exit 13 }
    'unsupported' { exit 2 }
    default { exit 1 }
}
