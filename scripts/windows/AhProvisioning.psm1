Set-StrictMode -Version Latest

$script:SchemaVersion = 1
$script:PhaseName = 'phase2_windows_host'
$script:RequiredFeatureNames = @(
    'Microsoft-Windows-Subsystem-Linux',
    'VirtualMachinePlatform'
)

function Get-AhRequiredFeatureNames {
    [CmdletBinding()]
    param()

    return @($script:RequiredFeatureNames)
}

function New-AhOperationId {
    [CmdletBinding()]
    param()

    return [guid]::NewGuid().ToString()
}

function Get-AhDefaultStatePath {
    [CmdletBinding()]
    param()

    $root = $env:LOCALAPPDATA
    if ([string]::IsNullOrWhiteSpace($root)) {
        $root = Join-Path ([System.IO.Path]::GetTempPath()) 'ah-localappdata'
    }

    return Join-Path (Join-Path $root 'ah') 'setup-state.json'
}

function Read-AhSetupState {
    [CmdletBinding()]
    param(
        [string]$Path = (Get-AhDefaultStatePath)
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    $raw = Get-Content -LiteralPath $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return $null
    }

    return $raw | ConvertFrom-Json
}

function Write-AhSetupState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$State,

        [string]$Path = (Get-AhDefaultStatePath)
    )

    $dir = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }

    $tmp = "$Path.tmp.$PID"
    $State | ConvertTo-Json -Depth 16 | Set-Content -LiteralPath $tmp -Encoding UTF8
    Move-Item -LiteralPath $tmp -Destination $Path -Force
}

function Clear-AhSetupState {
    [CmdletBinding()]
    param(
        [string]$Path = (Get-AhDefaultStatePath)
    )

    Remove-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
}

function New-AhSetupStep {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Id,

        [Parameter(Mandatory = $true)]
        [string]$Status,

        [string]$Owner = 'AhRuntime',
        [bool]$FixAvailable = $false,
        [string]$Privilege = 'user',
        [string]$Boundary = 'windows-host',
        [string]$Restart = 'none',
        [string]$Detail = '',
        [string]$Suggestion = '',
        [string]$ResumeToken = $null
    )

    return [ordered]@{
        id           = $Id
        status       = $Status
        owner        = $Owner
        fix_available = $FixAvailable
        privilege    = $Privilege
        boundary     = $Boundary
        restart      = $Restart
        detail       = $Detail
        suggestion   = $Suggestion
        resume_token = $ResumeToken
    }
}

function New-AhNextAction {
    [CmdletBinding()]
    param(
        [string]$Kind = 'none',
        [string]$Message = '',
        [string]$Command = $null
    )

    return [ordered]@{
        kind    = $Kind
        message = $Message
        command = $Command
    }
}

function Get-AhResumeCommand {
    [CmdletBinding()]
    param(
        [string]$ScriptPath = '.\provision-ah-wsl.ps1'
    )

    return "powershell.exe -NoProfile -ExecutionPolicy Bypass -File $ScriptPath --resume"
}

function New-AhSetupEnvelope {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$OperationId,

        [Parameter(Mandatory = $true)]
        [string]$OverallStatus,

        [string]$SelectedDistro = $null,

        [object]$NextAction = $null,

        [string]$ResumeCommand = $null,

        [object[]]$Steps = @()
    )

    if ($null -eq $NextAction) {
        $NextAction = New-AhNextAction
    }

    return [ordered]@{
        schema_version = $script:SchemaVersion
        operation_id   = $OperationId
        overall_status = $OverallStatus
        phase          = $script:PhaseName
        selected_distro = $SelectedDistro
        next_action    = $NextAction
        resume_command = $ResumeCommand
        steps          = @($Steps)
    }
}

function New-AhWindowsHostState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$OperationId,

        [string]$SelectedDistro = 'Ubuntu',
        [string]$PendingRestart = 'none',
        [string]$LastCompletedStep = $null,
        [object]$FeatureSteps = $null,
        [object]$AhInstall = $null,
        [string]$LastError = $null
    )

    $now = [DateTimeOffset]::UtcNow.ToString('o')
    if ($null -eq $FeatureSteps) {
        $FeatureSteps = [ordered]@{}
    }
    if ($null -eq $AhInstall) {
        $AhInstall = [ordered]@{}
    }

    return [ordered]@{
        schema_version = $script:SchemaVersion
        operation_id   = $OperationId
        helper_version = 'p2-0'
        phase          = $script:PhaseName
        boundary       = 'windows-host'
        selected_distro = $SelectedDistro
        requested_default_wsl_version = 2
        feature_steps  = $FeatureSteps
        selected_distro_wsl_version = $null
        pending_restart = $PendingRestart
        ah_install     = $AhInstall
        last_completed_step = $LastCompletedStep
        created_at     = $now
        updated_at     = $now
        last_error     = $LastError
    }
}

function Get-AhWindowsOptionalFeature {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    try {
        $feature = Get-WindowsOptionalFeature `
            -Online `
            -FeatureName $Name `
            -ErrorAction Stop
    } catch [System.UnauthorizedAccessException] {
        return [pscustomobject]@{
            Name = $Name
            State = 'AccessDenied'
        }
    } catch {
        if ($_.Exception.Message -match '(?i)elevat|access.*denied|permission|拒绝|提升') {
            return [pscustomobject]@{
                Name = $Name
                State = 'AccessDenied'
            }
        }
        throw
    }

    return [pscustomobject]@{
        Name = $Name
        State = [string]$feature.State
    }
}

function New-AhDismEnableFeatureArguments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    return @(
        '/online',
        '/enable-feature',
        "/featurename:$Name",
        '/all',
        '/norestart'
    )
}

function Invoke-AhDismEnableFeature {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $arguments = New-AhDismEnableFeatureArguments -Name $Name
    $output = & dism.exe @arguments 2>&1
    $exitCode = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 1 }

    return [pscustomobject]@{
        feature = $Name
        arguments = @($arguments)
        exit_code = $exitCode
        output = @($output | ForEach-Object { [string]$_ })
    }
}

function Invoke-AhWsl {
    [CmdletBinding()]
    param(
        [string[]]$Arguments = @()
    )

    $output = & wsl.exe @Arguments 2>&1
    $exitCode = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 1 }

    return [pscustomobject]@{
        arguments = @($Arguments)
        exit_code = $exitCode
        output = @($output | ForEach-Object { [string]$_ })
    }
}

function Read-AhLxssRegistry {
    [CmdletBinding()]
    param(
        [string]$DistroName = 'Ubuntu'
    )

    $root = 'Registry::HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\Lxss'
    if (-not (Test-Path -LiteralPath $root)) {
        return $null
    }

    foreach ($key in Get-ChildItem -LiteralPath $root) {
        $item = Get-ItemProperty -LiteralPath $key.PSPath
        if ($item.PSObject.Properties.Name -contains 'DistributionName' -and $item.DistributionName -eq $DistroName) {
            $defaultUid = $null
            if ($item.PSObject.Properties.Name -contains 'DefaultUid') {
                $defaultUid = [int]$item.DefaultUid
            }
            return [pscustomobject]@{
                DistributionName = $item.DistributionName
                DefaultUid = $defaultUid
                RegistryPath = $key.PSPath
            }
        }
    }

    return $null
}

function Start-AhElevatedFeatureChild {
    [CmdletBinding()]
    param(
        [string[]]$FeatureNames = @(),
        [Parameter(Mandatory = $true)]
        [string]$OperationId,
        [Parameter(Mandatory = $true)]
        [string]$ResultPath
    )

    $command = New-AhElevatedFeatureChildCommand `
        -FeatureNames $FeatureNames `
        -OperationId $OperationId `
        -ResultPath $ResultPath

    $resultDir = Split-Path -Parent $ResultPath
    if (-not [string]::IsNullOrWhiteSpace($resultDir)) {
        New-Item -ItemType Directory -Path $resultDir -Force | Out-Null
    }

    try {
        $process = Start-Process `
            -FilePath $command.FilePath `
            -ArgumentList $command.ArgumentList `
            -Verb RunAs `
            -Wait `
            -PassThru
    } catch {
        return [pscustomobject]@{
            status = 'permission_denied'
            exit_code = $null
            result_path = $ResultPath
            error = $_.Exception.Message
        }
    }

    if ($null -eq $process) {
        return [pscustomobject]@{
            status = 'permission_denied'
            exit_code = $null
            result_path = $ResultPath
            error = 'elevated child did not start'
        }
    }

    if (Test-Path -LiteralPath $ResultPath) {
        return Get-Content -LiteralPath $ResultPath -Raw | ConvertFrom-Json
    }

    if ($process.ExitCode -eq 0) {
        return [pscustomobject]@{
            status = 'reprobe_required'
            exit_code = $process.ExitCode
            result_path = $ResultPath
        }
    }

    return [pscustomobject]@{
        status = 'fail'
        exit_code = $process.ExitCode
        result_path = $ResultPath
        error = 'elevated child exited without result file'
    }
}

function New-AhElevatedFeatureChildCommand {
    [CmdletBinding()]
    param(
        [string[]]$FeatureNames = @(),
        [Parameter(Mandatory = $true)]
        [string]$OperationId,
        [Parameter(Mandatory = $true)]
        [string]$ResultPath,
        [string]$ChildScriptPath = (Join-Path $PSScriptRoot 'enable-ah-wsl-features.ps1')
    )

    $arguments = @(
        '-NoProfile',
        '-ExecutionPolicy',
        'Bypass',
        '-File',
        $ChildScriptPath,
        '-OperationId',
        $OperationId,
        '-ResultPath',
        $ResultPath
    )

    foreach ($feature in @($FeatureNames)) {
        $arguments += '-FeatureName'
        $arguments += $feature
    }

    return [pscustomobject]@{
        FilePath = 'powershell.exe'
        ArgumentList = @($arguments)
    }
}

function Get-AhFeatureStatusValue {
    [CmdletBinding()]
    param(
        [object]$Feature
    )

    if ($null -eq $Feature) {
        return 'Unknown'
    }
    if ($Feature -is [string]) {
        return $Feature
    }
    if ($Feature.PSObject.Properties.Name -contains 'State') {
        return [string]$Feature.State
    }
    if ($Feature.PSObject.Properties.Name -contains 'Status') {
        return [string]$Feature.Status
    }
    return 'Unknown'
}

function New-AhNeedsWindowsRebootEnvelope {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$OperationId,

        [string]$SelectedDistro = 'Ubuntu',
        [string]$Reason = 'Windows features are pending reboot.',
        [string]$StatePath = (Get-AhDefaultStatePath)
    )

    $resume = Get-AhResumeCommand
    $step = New-AhSetupStep `
        -Id 'windows:wsl-feature' `
        -Status 'needs_windows_reboot' `
        -FixAvailable $true `
        -Privilege 'admin' `
        -Restart 'needs_windows_reboot' `
        -Detail $Reason `
        -Suggestion "Reboot Windows, then rerun: $resume"

    $next = New-AhNextAction `
        -Kind 'reboot_windows' `
        -Message "Windows must reboot before WSL provisioning can continue. State: $StatePath" `
        -Command 'Restart-Computer'

    return New-AhSetupEnvelope `
        -OperationId $OperationId `
        -OverallStatus 'needs_windows_reboot' `
        -SelectedDistro $SelectedDistro `
        -NextAction $next `
        -ResumeCommand $resume `
        -Steps @($step)
}

function ConvertFrom-AhWslDistroList {
    [CmdletBinding()]
    param(
        [string[]]$Lines = @()
    )

    $distros = @()
    foreach ($rawLine in @($Lines)) {
        $line = ([string]$rawLine).Replace("`0", '').Trim()
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        if ($line -match '^\s*NAME\s+STATE\s+VERSION\s*$') {
            continue
        }

        $isDefault = $line.StartsWith('*')
        if ($isDefault) {
            $line = $line.Substring(1).Trim()
        }

        $match = [regex]::Match($line, '^(?<name>.+?)\s+(?<state>Running|Stopped|Installing|Uninstalling|Converting|Unknown)\s+(?<version>[12])\s*$')
        if (-not $match.Success) {
            continue
        }

        $distros += [pscustomobject]@{
            name = $match.Groups['name'].Value.Trim()
            state = $match.Groups['state'].Value
            version = [int]$match.Groups['version'].Value
            default = $isDefault
        }
    }

    return @($distros)
}

function Find-AhWslDistro {
    [CmdletBinding()]
    param(
        [object[]]$Distros = @(),
        [string]$SelectedDistro = 'Ubuntu'
    )

    foreach ($distro in @($Distros)) {
        if ($null -eq $distro) {
            continue
        }

        $nameProperty = $distro.PSObject.Properties['name']
        if ($null -eq $nameProperty) {
            $nameProperty = $distro.PSObject.Properties['Name']
        }
        if ($null -eq $nameProperty) {
            continue
        }

        if ([string]$nameProperty.Value -eq $SelectedDistro) {
            return $distro
        }
    }

    return $null
}

function New-AhWsl1ConversionStep {
    [CmdletBinding()]
    param(
        [string]$SelectedDistro = 'Ubuntu',
        [string]$Status = 'fail',
        [string]$Detail = ''
    )

    if ([string]::IsNullOrWhiteSpace($Detail)) {
        $Detail = "Selected distro '$SelectedDistro' is WSL1. Back up important distro data before converting to WSL2."
    }

    return New-AhSetupStep `
        -Id 'windows:wsl-distro-version' `
        -Status $Status `
        -FixAvailable $true `
        -Privilege 'user' `
        -Boundary 'windows-host' `
        -Restart 'none' `
        -Detail $Detail `
        -Suggestion "Back up important data, then run: wsl.exe --set-version $SelectedDistro 2"
}

function New-AhDistroInstallStep {
    [CmdletBinding()]
    param(
        [string]$SelectedDistro = 'Ubuntu',
        [string]$Status = 'fail',
        [string]$Detail = ''
    )

    if ([string]::IsNullOrWhiteSpace($Detail)) {
        $Detail = "Selected distro '$SelectedDistro' is not installed."
    }

    return New-AhSetupStep `
        -Id 'windows:wsl-distro' `
        -Status $Status `
        -FixAvailable $true `
        -Privilege 'user' `
        -Boundary 'windows-host' `
        -Restart 'none' `
        -Detail $Detail `
        -Suggestion "Run this helper with --fix to install: wsl.exe --install -d $SelectedDistro --no-launch"
}

function New-AhNeedsDistroFirstLaunchEnvelope {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$OperationId,

        [string]$SelectedDistro = 'Ubuntu',
        [string]$Reason = 'Distro first launch is required.',
        [string]$StatePath = (Get-AhDefaultStatePath)
    )

    $resume = Get-AhResumeCommand
    $step = New-AhSetupStep `
        -Id 'windows:wsl-distro-first-launch' `
        -Status 'needs_distro_first_launch' `
        -FixAvailable $false `
        -Privilege 'user' `
        -Boundary 'windows-host' `
        -Restart 'needs_distro_first_launch' `
        -Detail "$Reason Open the distro once and create the Linux username/password in the WSL prompt. Do not enter Linux credentials into ah." `
        -Suggestion "Run: wsl.exe -d $SelectedDistro"

    $next = New-AhNextAction `
        -Kind 'open_distro_first_launch' `
        -Message "Open '$SelectedDistro' once to create the Linux username/password. ah will not ask for or store those credentials. State: $StatePath" `
        -Command "wsl.exe -d $SelectedDistro"

    return New-AhSetupEnvelope `
        -OperationId $OperationId `
        -OverallStatus 'needs_distro_first_launch' `
        -SelectedDistro $SelectedDistro `
        -NextAction $next `
        -ResumeCommand $resume `
        -Steps @($step)
}

function Get-AhWslDefaultVersion {
    [CmdletBinding()]
    param(
        [string[]]$Lines = @()
    )

    foreach ($line in @($Lines)) {
        $clean = ([string]$line).Replace("`0", '').Trim()
        $match = [regex]::Match($clean, 'Default\s+Version:\s*(?<version>[12])', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($match.Success) {
            return [int]$match.Groups['version'].Value
        }
    }

    return $null
}

function Test-AhDistroFirstLaunchInitialized {
    [CmdletBinding()]
    param(
        [string]$SelectedDistro = 'Ubuntu'
    )

    $registry = Read-AhLxssRegistry -DistroName $SelectedDistro
    $defaultUid = $null
    if ($null -ne $registry -and ($registry.PSObject.Properties.Name -contains 'DefaultUid')) {
        $defaultUid = $registry.DefaultUid
    }

    if ($null -eq $defaultUid -or [int]$defaultUid -eq 0) {
        return [pscustomobject]@{
            initialized = $false
            reason = 'DefaultUid is missing or root.'
            default_uid = $defaultUid
            user = $null
            sudo_n_exit_code = $null
        }
    }

    $id = Invoke-AhWsl -Arguments @('-d', $SelectedDistro, '--', 'id', '-un')
    $user = ''
    if ($id.exit_code -eq 0 -and @($id.output).Count -gt 0) {
        $user = ([string]@($id.output)[0]).Trim()
    }

    if ($id.exit_code -ne 0 -or [string]::IsNullOrWhiteSpace($user) -or $user -eq 'root') {
        return [pscustomobject]@{
            initialized = $false
            reason = 'Default Linux user is missing or root.'
            default_uid = $defaultUid
            user = $user
            sudo_n_exit_code = $null
        }
    }

    $sudo = Invoke-AhWsl -Arguments @('-d', $SelectedDistro, '--', 'sh', '-lc', 'sudo -n true >/dev/null 2>&1')

    return [pscustomobject]@{
        initialized = $true
        reason = 'Distro has a non-root default Linux user.'
        default_uid = $defaultUid
        user = $user
        sudo_n_exit_code = $sudo.exit_code
    }
}

function ConvertTo-AhShSingleQuoted {
    [CmdletBinding()]
    param(
        [string]$Value = ''
    )

    return "'" + ($Value -replace "'", "'\''") + "'"
}

function New-AhInDistroAhInstallCommand {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$InstallUrl
    )

    $quotedUrl = ConvertTo-AhShSingleQuoted -Value $InstallUrl
    return "AH_SETUP_INSTALL_URL=$quotedUrl; export AH_SETUP_INSTALL_URL; export AH_INSTALL_DIR=`"`$HOME/.local`"; export AH_NO_MODIFY_PATH=1; curl -fsSL `"`$AH_SETUP_INSTALL_URL`" | sh; `"`$HOME/.local/bin/ah`" --version"
}

function New-AhInDistroAhVersionCommand {
    [CmdletBinding()]
    param()

    return '"$HOME/.local/bin/ah" --version'
}

function Test-AhVersionOutput {
    [CmdletBinding()]
    param(
        [string[]]$Output = @(),
        [string]$ExpectedVersion = ''
    )

    $joined = (@($Output) | ForEach-Object { [string]$_ }) -join "`n"
    if ([string]::IsNullOrWhiteSpace($ExpectedVersion)) {
        return -not [string]::IsNullOrWhiteSpace($joined)
    }

    return $joined -match [regex]::Escape($ExpectedVersion)
}

function Invoke-AhInDistroAhInstall {
    [CmdletBinding()]
    param(
        [string]$SelectedDistro = 'Ubuntu',
        [Parameter(Mandatory = $true)]
        [string]$InstallUrl,
        [string]$ExpectedVersion = ''
    )

    $homeProbe = Invoke-AhWsl -Arguments @('-d', $SelectedDistro, '--', 'sh', '-lc', 'printf %s "$HOME"')
    $linuxHome = ''
    if ($homeProbe.exit_code -eq 0 -and @($homeProbe.output).Count -gt 0) {
        $linuxHome = ((@($homeProbe.output) | ForEach-Object { [string]$_ }) -join "`n").Trim()
    }

    if ($homeProbe.exit_code -ne 0 -or [string]::IsNullOrWhiteSpace($linuxHome)) {
        return [pscustomobject]@{
            status = 'fail'
            attempts = 0
            linux_home = $linuxHome
            install_dir = $null
            version_output = @()
            error = "Could not probe Linux HOME in '$SelectedDistro'."
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($ExpectedVersion)) {
        $versionCommand = New-AhInDistroAhVersionCommand
        $existingVersion = Invoke-AhWsl -Arguments @('-d', $SelectedDistro, '--', 'sh', '-lc', $versionCommand)
        if ($existingVersion.exit_code -eq 0 -and (Test-AhVersionOutput -Output @($existingVersion.output) -ExpectedVersion $ExpectedVersion)) {
            return [pscustomobject]@{
                status = 'pass'
                attempts = 0
                linux_home = $linuxHome
                install_dir = "$linuxHome/.local/bin"
                version_output = @($existingVersion.output)
                error = $null
            }
        }
    }

    $installCommand = New-AhInDistroAhInstallCommand -InstallUrl $InstallUrl
    $lastInstall = $null
    for ($attempt = 1; $attempt -le 2; $attempt++) {
        $lastInstall = Invoke-AhWsl -Arguments @('-d', $SelectedDistro, '--', 'sh', '-lc', $installCommand)
        if ($lastInstall.exit_code -eq 0 -and (Test-AhVersionOutput -Output @($lastInstall.output) -ExpectedVersion $ExpectedVersion)) {
            return [pscustomobject]@{
                status = 'pass'
                attempts = $attempt
                linux_home = $linuxHome
                install_dir = "$linuxHome/.local/bin"
                version_output = @($lastInstall.output)
                error = $null
            }
        }
    }

    $exitCode = if ($null -ne $lastInstall) { $lastInstall.exit_code } else { $null }
    return [pscustomobject]@{
        status = 'fail'
        attempts = 2
        linux_home = $linuxHome
        install_dir = "$linuxHome/.local/bin"
        version_output = if ($null -ne $lastInstall) { @($lastInstall.output) } else { @() }
        error = "In-distro ah install or version verification failed with exit code $exitCode."
    }
}

function New-AhDistroLocalSetupCommand {
    [CmdletBinding()]
    param()

    return '"$HOME/.local/bin/ah" setup --resume --fix --json'
}

function Invoke-AhDistroLocalSetup {
    [CmdletBinding()]
    param(
        [string]$SelectedDistro = 'Ubuntu'
    )

    $command = New-AhDistroLocalSetupCommand
    $result = Invoke-AhWsl -Arguments @('-d', $SelectedDistro, '--', 'sh', '-lc', $command)
    $raw = (@($result.output) | ForEach-Object { [string]$_ }) -join "`n"

    if ($result.exit_code -ne 0) {
        return [pscustomobject]@{
            status = 'fail'
            exit_code = $result.exit_code
            raw_json = $raw
            envelope = $null
            error = "Distro-local ah setup failed with exit code $($result.exit_code)."
        }
    }

    try {
        $envelope = $raw | ConvertFrom-Json
    } catch {
        return [pscustomobject]@{
            status = 'fail'
            exit_code = $result.exit_code
            raw_json = $raw
            envelope = $null
            error = "Distro-local ah setup returned invalid JSON: $($_.Exception.Message)"
        }
    }

    $status = 'fail'
    if ($null -ne $envelope -and ($envelope.PSObject.Properties.Name -contains 'overall_status')) {
        $status = [string]$envelope.overall_status
    }

    return [pscustomobject]@{
        status = $status
        exit_code = $result.exit_code
        raw_json = $raw
        envelope = $envelope
        error = $null
    }
}

function New-AhInDistroAhInstallStep {
    [CmdletBinding()]
    param(
        [string]$Status = 'fail',
        [string]$Detail = '',
        [bool]$FixAvailable = $true
    )

    if ([string]::IsNullOrWhiteSpace($Detail)) {
        $Detail = 'Install or update ah inside the selected WSL distro.'
    }

    return New-AhSetupStep `
        -Id 'windows:in-distro-ah-install' `
        -Status $Status `
        -FixAvailable $FixAvailable `
        -Privilege 'user' `
        -Boundary 'wsl-distro' `
        -Restart 'none' `
        -Detail $Detail `
        -Suggestion 'Rerun this helper with --fix --resume after correcting the install URL or network issue.'
}

function New-AhDistroLocalSetupStep {
    [CmdletBinding()]
    param(
        [string]$Status = 'fail',
        [string]$Detail = '',
        [bool]$FixAvailable = $true
    )

    if ([string]::IsNullOrWhiteSpace($Detail)) {
        $Detail = 'Run distro-local ah setup inside the selected WSL distro.'
    }

    return New-AhSetupStep `
        -Id 'windows:distro-local-setup' `
        -Status $Status `
        -FixAvailable $FixAvailable `
        -Privilege 'user' `
        -Boundary 'wsl-distro' `
        -Restart 'none' `
        -Detail $Detail `
        -Suggestion 'Rerun this helper with --fix --resume after correcting the distro-local setup issue.'
}

function New-AhNeedsWslShutdownEnvelope {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$OperationId,

        [string]$SelectedDistro = 'Ubuntu',
        [object[]]$Steps = @(),
        [string]$StatePath = (Get-AhDefaultStatePath)
    )

    $resume = Get-AhResumeCommand
    $next = New-AhNextAction `
        -Kind 'shutdown_wsl' `
        -Message "Distro-local setup changed WSL boot configuration. wsl.exe --shutdown terminates all running WSL distros, not only '$SelectedDistro'. State: $StatePath" `
        -Command 'wsl.exe --shutdown'

    return New-AhSetupEnvelope `
        -OperationId $OperationId `
        -OverallStatus 'needs_wsl_shutdown' `
        -SelectedDistro $SelectedDistro `
        -NextAction $next `
        -ResumeCommand $resume `
        -Steps @($Steps)
}

function Invoke-AhPhase2Provisioning {
    [CmdletBinding()]
    param(
        [switch]$Check,
        [switch]$Fix,
        [switch]$Yes,
        [switch]$Resume,
        [switch]$DryRun,
        [string]$SelectedDistro = 'Ubuntu',
        [string]$StatePath = (Get-AhDefaultStatePath),
        [string]$AhInstallUrl = $env:AH_SETUP_INSTALL_URL,
        [string]$ExpectedAhVersion = $env:AH_SETUP_EXPECTED_VERSION
    )

    $state = Read-AhSetupState -Path $StatePath

    $operationId = New-AhOperationId
    if ($null -ne $state -and ($state.PSObject.Properties.Name -contains 'operation_id')) {
        $operationId = [string]$state.operation_id
    }

    if ($DryRun) {
        $step = New-AhSetupStep `
            -Id 'p2-0:contract' `
            -Status 'pass' `
            -Detail 'Dry-run validated the Phase 2 contract without probing host state.'
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'pass' `
            -SelectedDistro $SelectedDistro `
            -Steps @($step)
    }

    if (-not $Resume -and $null -ne $state -and ($state.PSObject.Properties.Name -contains 'pending_restart') -and $state.pending_restart -eq 'windows_reboot') {
        return New-AhNeedsWindowsRebootEnvelope `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -Reason 'Previous run stopped at a Windows reboot boundary.' `
            -StatePath $StatePath
    }

    if (-not $Resume -and $null -ne $state -and ($state.PSObject.Properties.Name -contains 'pending_restart') -and $state.pending_restart -eq 'distro_first_launch') {
        return New-AhNeedsDistroFirstLaunchEnvelope `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -Reason 'Previous run stopped at the distro first-launch boundary.' `
            -StatePath $StatePath
    }

    if (-not $Resume -and $null -ne $state -and ($state.PSObject.Properties.Name -contains 'pending_restart') -and $state.pending_restart -eq 'wsl_shutdown') {
        $step = New-AhDistroLocalSetupStep `
            -Status 'needs_wsl_shutdown' `
            -Detail 'Previous run stopped at the WSL shutdown boundary. Resume after running PowerShell: wsl --shutdown.'
        return New-AhNeedsWslShutdownEnvelope `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -StatePath $StatePath `
            -Steps @($step)
    }

    if (-not $Resume -and -not $Fix -and $null -ne $state -and ($state.PSObject.Properties.Name -contains 'pending_restart') -and -not [string]::IsNullOrWhiteSpace([string]$state.pending_restart) -and $state.pending_restart -ne 'none') {
        $resumeCommand = Get-AhResumeCommand
        $step = New-AhSetupStep `
            -Id 'windows:resume-required' `
            -Status 'fail' `
            -Detail "Previous run stopped at '$($state.pending_restart)'. Rerun with -Resume to continue from the saved boundary. State: $StatePath"
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'fail' `
            -SelectedDistro $SelectedDistro `
            -NextAction (New-AhNextAction `
                -Kind 'resume' `
                -Command $resumeCommand `
                -Message 'Resume the saved Windows provisioning operation before starting a new one.') `
            -ResumeCommand $resumeCommand `
            -Steps @($step)
    }

    $featureNames = Get-AhRequiredFeatureNames

    $featureStatuses = @()
    foreach ($featureName in $featureNames) {
        $feature = Get-AhWindowsOptionalFeature -Name $featureName
        $featureStatuses += [pscustomobject]@{
            Name = $featureName
            State = Get-AhFeatureStatusValue -Feature $feature
        }
    }

    $pending = @($featureStatuses | Where-Object { $_.State -eq 'EnablePending' })
    if ($pending.Count -gt 0) {
        $newState = New-AhWindowsHostState `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -PendingRestart 'windows_reboot' `
            -LastCompletedStep 'windows_feature_enable'
        Write-AhSetupState -State $newState -Path $StatePath
        return New-AhNeedsWindowsRebootEnvelope `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -Reason 'One or more Windows features are EnablePending.' `
            -StatePath $StatePath
    }

    $accessDenied = @($featureStatuses | Where-Object { $_.State -eq 'AccessDenied' })
    if ($accessDenied.Count -gt 0 -and -not $Fix) {
        $steps = foreach ($item in $accessDenied) {
            New-AhSetupStep `
                -Id "windows:feature:$($item.Name)" `
                -Status 'permission_denied' `
                -FixAvailable $true `
                -Privilege 'admin' `
                -Detail 'Windows requires elevation to inspect this optional feature.' `
                -Suggestion 'Rerun this helper with -Fix; it will request UAC elevation for the feature operation.'
        }
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'permission_denied' `
            -SelectedDistro $SelectedDistro `
            -Steps @($steps)
    }

    $unknown = @($featureStatuses | Where-Object { $_.State -ne 'Enabled' -and $_.State -ne 'Disabled' -and $_.State -ne 'EnablePending' -and $_.State -ne 'AccessDenied' })
    if ($unknown.Count -gt 0) {
        $steps = foreach ($item in $unknown) {
            New-AhSetupStep `
                -Id "windows:feature:$($item.Name)" `
                -Status 'fail' `
                -FixAvailable $false `
                -Privilege 'user' `
                -Detail "Windows optional feature probe returned unknown state '$($item.State)'." `
                -Suggestion 'Inspect Windows optional feature status, then rerun this helper.'
        }
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'fail' `
            -SelectedDistro $SelectedDistro `
            -Steps @($steps)
    }

    $disabled = @($featureStatuses | Where-Object { $_.State -ne 'Enabled' })
    if ($disabled.Count -gt 0 -and -not $Fix) {
        $steps = foreach ($item in $disabled) {
            New-AhSetupStep `
                -Id "windows:feature:$($item.Name)" `
                -Status 'fail' `
                -FixAvailable $true `
                -Privilege 'admin' `
                -Detail "Windows optional feature is $($item.State)." `
                -Suggestion 'Run this helper with --fix to enable missing WSL2 features.'
        }
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'fail' `
            -SelectedDistro $SelectedDistro `
            -Steps @($steps)
    }

    if ($disabled.Count -gt 0 -and $Fix) {
        $resultPath = Join-Path (Split-Path -Parent $StatePath) "setup-elevated-result.$operationId.json"
        $child = Start-AhElevatedFeatureChild `
            -FeatureNames @($disabled.Name) `
            -OperationId $operationId `
            -ResultPath $resultPath

        $childStatus = 'fail'
        if ($null -ne $child -and ($child.PSObject.Properties.Name -contains 'status')) {
            $childStatus = [string]$child.status
        }

        if ($childStatus -eq 'permission_denied') {
            $step = New-AhSetupStep `
                -Id 'windows:feature-elevation' `
                -Status 'permission_denied' `
                -FixAvailable $true `
                -Privilege 'admin' `
                -Detail 'User cancelled or denied UAC elevation.'
            return New-AhSetupEnvelope `
                -OperationId $operationId `
                -OverallStatus 'permission_denied' `
                -SelectedDistro $SelectedDistro `
                -Steps @($step)
        }

        if ($childStatus -eq 'needs_windows_reboot' -or $childStatus -eq 'partial_enable') {
            $featureSteps = [ordered]@{}
            if ($null -ne $child -and ($child.PSObject.Properties.Name -contains 'features')) {
                foreach ($feature in @($child.features)) {
                    $featureSteps[$feature.name] = $feature
                }
            }
            $newState = New-AhWindowsHostState `
                -OperationId $operationId `
                -SelectedDistro $SelectedDistro `
                -PendingRestart 'windows_reboot' `
                -LastCompletedStep 'windows_feature_enable' `
                -FeatureSteps $featureSteps
            if ($childStatus -eq 'partial_enable') {
                $newState['partial_enable'] = $true
            }
            Write-AhSetupState -State $newState -Path $StatePath
            return New-AhNeedsWindowsRebootEnvelope `
                -OperationId $operationId `
                -SelectedDistro $SelectedDistro `
                -Reason "Elevated feature child returned $childStatus." `
                -StatePath $StatePath
        }

        if ($childStatus -eq 'reprobe_required') {
            $reprobedStatuses = @()
            foreach ($featureName in $featureNames) {
                $feature = Get-AhWindowsOptionalFeature -Name $featureName
                $reprobedStatuses += [pscustomobject]@{
                    Name = $featureName
                    State = Get-AhFeatureStatusValue -Feature $feature
                }
            }

            $reprobedPending = @($reprobedStatuses | Where-Object { $_.State -eq 'EnablePending' })
            if ($reprobedPending.Count -gt 0) {
                $newState = New-AhWindowsHostState `
                    -OperationId $operationId `
                    -SelectedDistro $SelectedDistro `
                    -PendingRestart 'windows_reboot' `
                    -LastCompletedStep 'windows_feature_enable'
                Write-AhSetupState -State $newState -Path $StatePath
                return New-AhNeedsWindowsRebootEnvelope `
                    -OperationId $operationId `
                    -SelectedDistro $SelectedDistro `
                    -Reason 'Elevated child returned success without result file; re-probe found EnablePending.' `
                    -StatePath $StatePath
            }

            $reprobedNotEnabled = @($reprobedStatuses | Where-Object { $_.State -ne 'Enabled' })
            if ($reprobedNotEnabled.Count -eq 0) {
                $featureStatuses = $reprobedStatuses
                $disabled = @()
            } else {
                $step = New-AhSetupStep `
                    -Id 'windows:feature-enable' `
                    -Status 'fail' `
                    -FixAvailable $true `
                    -Privilege 'admin' `
                    -Detail 'Elevated child returned success without a result file; re-probe did not find all features enabled or pending.'
                return New-AhSetupEnvelope `
                    -OperationId $operationId `
                    -OverallStatus 'fail' `
                    -SelectedDistro $SelectedDistro `
                    -Steps @($step)
            }
        }

        if ($disabled.Count -gt 0) {
            $step = New-AhSetupStep `
                -Id 'windows:feature-enable' `
                -Status 'fail' `
                -FixAvailable $true `
                -Privilege 'admin' `
                -Detail "Elevated feature child returned $childStatus."
            return New-AhSetupEnvelope `
                -OperationId $operationId `
                -OverallStatus 'fail' `
                -SelectedDistro $SelectedDistro `
                -Steps @($step)
        }
    }

    $passSteps = foreach ($item in $featureStatuses) {
        New-AhSetupStep `
            -Id "windows:feature:$($item.Name)" `
            -Status 'pass' `
            -Detail "Windows optional feature is $($item.State)."
    }

    $wslStatus = Invoke-AhWsl -Arguments @('--status')
    if ($wslStatus.exit_code -ne 0) {
        $steps = @($passSteps)
        $steps += New-AhSetupStep `
            -Id 'windows:wsl-status' `
            -Status 'fail' `
            -Detail "wsl.exe --status failed with exit code $($wslStatus.exit_code)." `
            -Suggestion 'Inspect WSL status in PowerShell, then rerun this helper.'
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'fail' `
            -SelectedDistro $SelectedDistro `
            -Steps @($steps)
    }

    $passSteps += New-AhSetupStep `
        -Id 'windows:wsl-status' `
        -Status 'pass' `
        -Detail 'wsl.exe --status completed.'

    $defaultVersion = Get-AhWslDefaultVersion -Lines @($wslStatus.output)
    if ($defaultVersion -eq 2) {
        $passSteps += New-AhSetupStep `
            -Id 'windows:wsl-default-version' `
            -Status 'pass' `
            -Detail 'WSL default version is already 2.'
    } else {
        $setDefault = Invoke-AhWsl -Arguments @('--set-default-version', '2')
        if ($setDefault.exit_code -ne 0) {
            $steps = @($passSteps)
            $steps += New-AhSetupStep `
                -Id 'windows:wsl-default-version' `
                -Status 'fail' `
                -Detail "wsl.exe --set-default-version 2 failed with exit code $($setDefault.exit_code)." `
                -Suggestion 'Inspect WSL default version in PowerShell, then rerun this helper.'
            return New-AhSetupEnvelope `
                -OperationId $operationId `
                -OverallStatus 'fail' `
                -SelectedDistro $SelectedDistro `
                -Steps @($steps)
        }

        $passSteps += New-AhSetupStep `
            -Id 'windows:wsl-default-version' `
            -Status 'pass' `
            -Detail 'wsl.exe --set-default-version 2 completed for future distro installs.'
    }

    $distroList = Invoke-AhWsl -Arguments @('-l', '-v')
    if ($distroList.exit_code -ne 0) {
        $steps = @($passSteps)
        $steps += New-AhSetupStep `
            -Id 'windows:wsl-distro-list' `
            -Status 'fail' `
            -Detail "wsl.exe -l -v failed with exit code $($distroList.exit_code)." `
            -Suggestion 'Inspect installed WSL distros, then rerun this helper.'
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'fail' `
            -SelectedDistro $SelectedDistro `
            -Steps @($steps)
    }

    $distros = ConvertFrom-AhWslDistroList -Lines @($distroList.output)
    $selected = Find-AhWslDistro -Distros $distros -SelectedDistro $SelectedDistro

    if ($null -ne $selected -and $selected.version -eq 1) {
        if (-not $Fix) {
            $steps = @($passSteps)
            $steps += New-AhWsl1ConversionStep -SelectedDistro $SelectedDistro
            return New-AhSetupEnvelope `
                -OperationId $operationId `
                -OverallStatus 'fail' `
                -SelectedDistro $SelectedDistro `
                -Steps @($steps)
        }

        $convert = Invoke-AhWsl -Arguments @('--set-version', $SelectedDistro, '2')
        $newState = New-AhWindowsHostState `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -PendingRestart 'distro_setup' `
            -LastCompletedStep 'wsl2_conversion_requested'
        $newState['selected_distro_wsl_version'] = 1
        $newState['pending_conversion'] = 'wsl2'
        Write-AhSetupState -State $newState -Path $StatePath

        $steps = @($passSteps)
        if ($convert.exit_code -ne 0) {
            $steps += New-AhWsl1ConversionStep `
                -SelectedDistro $SelectedDistro `
                -Status 'fail' `
                -Detail "WSL2 conversion for '$SelectedDistro' failed with exit code $($convert.exit_code). Back up important distro data before retrying."
            return New-AhSetupEnvelope `
                -OperationId $operationId `
                -OverallStatus 'fail' `
                -SelectedDistro $SelectedDistro `
                -Steps @($steps)
        }

        $steps += New-AhWsl1ConversionStep `
            -SelectedDistro $SelectedDistro `
            -Status 'fixed' `
            -Detail "Requested WSL2 conversion for '$SelectedDistro'. Back up important distro data before retrying if conversion does not complete."
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'fixed' `
            -SelectedDistro $SelectedDistro `
            -Steps @($steps)
    }

    if ($null -eq $selected) {
        if (-not $Fix) {
            $steps = @($passSteps)
            $steps += New-AhDistroInstallStep -SelectedDistro $SelectedDistro
            return New-AhSetupEnvelope `
                -OperationId $operationId `
                -OverallStatus 'fail' `
                -SelectedDistro $SelectedDistro `
                -Steps @($steps)
        }

        $install = Invoke-AhWsl -Arguments @('--install', '-d', $SelectedDistro, '--no-launch')
        if ($install.exit_code -ne 0) {
            $newState = New-AhWindowsHostState `
                -OperationId $operationId `
                -SelectedDistro $SelectedDistro `
                -PendingRestart 'distro_install' `
                -LastCompletedStep 'wsl2_default_version' `
                -LastError "wsl.exe --install failed with exit code $($install.exit_code)"
            Write-AhSetupState -State $newState -Path $StatePath

            $steps = @($passSteps)
            $steps += New-AhDistroInstallStep `
                -SelectedDistro $SelectedDistro `
                -Status 'unsupported' `
                -Detail "Could not install '$SelectedDistro' with wsl.exe --install -d $SelectedDistro --no-launch. Store or enterprise policy may block distro install."
            return New-AhSetupEnvelope `
                -OperationId $operationId `
                -OverallStatus 'unsupported' `
                -SelectedDistro $SelectedDistro `
                -Steps @($steps)
        }

        $newState = New-AhWindowsHostState `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -PendingRestart 'distro_first_launch' `
            -LastCompletedStep 'distro_install'
        Write-AhSetupState -State $newState -Path $StatePath

        return New-AhNeedsDistroFirstLaunchEnvelope `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -Reason "Installed '$SelectedDistro' with --no-launch." `
            -StatePath $StatePath
    }

    if ($null -ne $selected) {
        $passSteps += New-AhSetupStep `
            -Id 'windows:wsl-distro-version' `
            -Status 'pass' `
            -Detail "Selected distro '$SelectedDistro' is WSL$($selected.version)."
    }

    $firstLaunch = Test-AhDistroFirstLaunchInitialized -SelectedDistro $SelectedDistro
    if (-not $firstLaunch.initialized) {
        $newState = New-AhWindowsHostState `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -PendingRestart 'distro_first_launch' `
            -LastCompletedStep 'distro_present'
        $newState['selected_distro_wsl_version'] = $selected.version
        Write-AhSetupState -State $newState -Path $StatePath

        return New-AhNeedsDistroFirstLaunchEnvelope `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -Reason $firstLaunch.reason `
            -StatePath $StatePath
    }

    $passSteps += New-AhSetupStep `
        -Id 'windows:wsl-distro-first-launch' `
        -Status 'pass' `
        -Detail "Selected distro '$SelectedDistro' has default Linux user '$($firstLaunch.user)'. sudo -n exit code: $($firstLaunch.sudo_n_exit_code)."

    if ($Fix -and [string]::IsNullOrWhiteSpace($AhInstallUrl)) {
        $passSteps += New-AhInDistroAhInstallStep `
            -Status 'fail' `
            -Detail 'Missing ah installer URL. Pass --ah-install-url or set AH_SETUP_INSTALL_URL.' `
            -FixAvailable $true
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'fail' `
            -SelectedDistro $SelectedDistro `
            -Steps @($passSteps)
    }

    if (-not $Fix) {
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'pass' `
            -SelectedDistro $SelectedDistro `
            -Steps @($passSteps)
    }

    $ahInstall = Invoke-AhInDistroAhInstall `
        -SelectedDistro $SelectedDistro `
        -InstallUrl $AhInstallUrl `
        -ExpectedVersion $ExpectedAhVersion

    if ($ahInstall.status -ne 'pass') {
        $newState = New-AhWindowsHostState `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -PendingRestart 'in_distro_ah_install' `
            -LastCompletedStep 'distro_first_launch' `
            -LastError $ahInstall.error
        $newState['selected_distro_wsl_version'] = $selected.version
        $newState['ah_install'] = [ordered]@{
            status = 'AhInstallFailed'
            install_url = $AhInstallUrl
            expected_version = $ExpectedAhVersion
            linux_home = $ahInstall.linux_home
            install_dir = $ahInstall.install_dir
            attempts = $ahInstall.attempts
            version_output = @($ahInstall.version_output)
        }
        Write-AhSetupState -State $newState -Path $StatePath

        $passSteps += New-AhInDistroAhInstallStep `
            -Status 'fail' `
            -Detail "AhInstallFailed: $($ahInstall.error)"
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'fail' `
            -SelectedDistro $SelectedDistro `
            -Steps @($passSteps)
    }

    $passSteps += New-AhInDistroAhInstallStep `
        -Status 'fixed' `
        -Detail "Installed or verified ah at $($ahInstall.install_dir) in '$SelectedDistro'. Attempts: $($ahInstall.attempts)."

    $distroSetup = Invoke-AhDistroLocalSetup -SelectedDistro $SelectedDistro
    if ($distroSetup.status -eq 'needs_wsl_shutdown') {
        $newState = New-AhWindowsHostState `
            -OperationId $operationId `
            -SelectedDistro $SelectedDistro `
            -PendingRestart 'wsl_shutdown' `
            -LastCompletedStep 'distro_local_setup_needs_wsl_shutdown'
        $newState['selected_distro_wsl_version'] = $selected.version
        $newState['ah_install'] = [ordered]@{
            status = 'Installed'
            install_url = $AhInstallUrl
            expected_version = $ExpectedAhVersion
            linux_home = $ahInstall.linux_home
            install_dir = $ahInstall.install_dir
            attempts = $ahInstall.attempts
            version_output = @($ahInstall.version_output)
        }
        Write-AhSetupState -State $newState -Path $StatePath

        $steps = @($passSteps)
        $steps += New-AhDistroLocalSetupStep `
            -Status 'needs_wsl_shutdown' `
            -Detail 'Distro-local ah setup requires wsl.exe --shutdown before continuing.'

        if (-not $Yes) {
            return New-AhNeedsWslShutdownEnvelope `
                -OperationId $operationId `
                -SelectedDistro $SelectedDistro `
                -Steps @($steps) `
                -StatePath $StatePath
        }

        $shutdown = Invoke-AhWsl -Arguments @('--shutdown')
        if ($shutdown.exit_code -ne 0) {
            $steps += New-AhSetupStep `
                -Id 'windows:wsl-shutdown' `
                -Status 'fail' `
                -FixAvailable $true `
                -Privilege 'user' `
                -Detail "wsl.exe --shutdown failed with exit code $($shutdown.exit_code)."
            return New-AhSetupEnvelope `
                -OperationId $operationId `
                -OverallStatus 'fail' `
                -SelectedDistro $SelectedDistro `
                -Steps @($steps)
        }

        $steps += New-AhSetupStep `
            -Id 'windows:wsl-shutdown' `
            -Status 'fixed' `
            -Detail 'Ran wsl.exe --shutdown; all running WSL distros were terminated.'
        $distroSetup = Invoke-AhDistroLocalSetup -SelectedDistro $SelectedDistro
        $passSteps = $steps
    }

    if ($distroSetup.status -eq 'pass' -or $distroSetup.status -eq 'fixed') {
        Clear-AhSetupState -Path $StatePath
        $passSteps += New-AhDistroLocalSetupStep `
            -Status 'pass' `
            -Detail "Distro-local ah setup completed with status '$($distroSetup.status)'."
        return New-AhSetupEnvelope `
            -OperationId $operationId `
            -OverallStatus 'pass' `
            -SelectedDistro $SelectedDistro `
            -Steps @($passSteps)
    }

    $newState = New-AhWindowsHostState `
        -OperationId $operationId `
        -SelectedDistro $SelectedDistro `
        -PendingRestart 'distro_local_setup' `
        -LastCompletedStep 'in_distro_ah_install' `
        -LastError $distroSetup.error
    $newState['selected_distro_wsl_version'] = $selected.version
    Write-AhSetupState -State $newState -Path $StatePath

    $passSteps += New-AhDistroLocalSetupStep `
        -Status 'fail' `
        -Detail "Distro-local ah setup returned '$($distroSetup.status)'. $($distroSetup.error)"
    return New-AhSetupEnvelope `
        -OperationId $operationId `
        -OverallStatus 'fail' `
        -SelectedDistro $SelectedDistro `
        -Steps @($passSteps)
}

Export-ModuleMember -Function @(
    'New-AhOperationId',
    'Get-AhDefaultStatePath',
    'Read-AhSetupState',
    'Write-AhSetupState',
    'Clear-AhSetupState',
    'New-AhSetupStep',
    'New-AhNextAction',
    'Get-AhResumeCommand',
    'New-AhSetupEnvelope',
    'New-AhWindowsHostState',
    'Get-AhRequiredFeatureNames',
    'Get-AhWindowsOptionalFeature',
    'New-AhDismEnableFeatureArguments',
    'Invoke-AhDismEnableFeature',
    'Invoke-AhWsl',
    'Read-AhLxssRegistry',
    'Start-AhElevatedFeatureChild',
    'New-AhElevatedFeatureChildCommand',
    'ConvertFrom-AhWslDistroList',
    'Find-AhWslDistro',
    'New-AhWsl1ConversionStep',
    'Get-AhWslDefaultVersion',
    'New-AhDistroInstallStep',
    'New-AhNeedsDistroFirstLaunchEnvelope',
    'Test-AhDistroFirstLaunchInitialized',
    'ConvertTo-AhShSingleQuoted',
    'New-AhInDistroAhInstallCommand',
    'Test-AhVersionOutput',
    'Invoke-AhInDistroAhInstall',
    'New-AhInDistroAhInstallStep',
    'New-AhDistroLocalSetupCommand',
    'Invoke-AhDistroLocalSetup',
    'New-AhDistroLocalSetupStep',
    'New-AhNeedsWslShutdownEnvelope',
    'Invoke-AhPhase2Provisioning'
)
