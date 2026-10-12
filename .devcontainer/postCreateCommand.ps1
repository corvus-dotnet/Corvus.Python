#! /usr/bin/pwsh
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
if (!$env:TF_BUILD) {
    # Skip this on the CI server as it will run too early in the process, the build script will handle it
    & poetry install
}
& mkdir -p "/root/.config/powershell/"
Write-Output ". .venv/bin/activate.ps1" > "/root/.config/powershell/Microsoft.VSCode_profile.ps1"