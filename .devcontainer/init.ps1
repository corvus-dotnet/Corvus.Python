#!/bin/pwsh

# The '.claude' mounts in devcontainer.json resolve the host home directory as
# '${localEnv:HOME}${localEnv:USERPROFILE}', which gives the right path as long as only one
# of the two is set. Windows always sets USERPROFILE, so HOME is the anomaly here: Git Bash
# and msys set it, and VS Code inherits it when launched from such a shell, leaving both set
# and concatenating the two paths into an invalid one.
if ($env:HOME -and $env:USERPROFILE) {
    Write-Warning @"
Both HOME ($env:HOME) and USERPROFILE ($env:USERPROFILE) are set on this host.
The dev container resolves your home directory by concatenating the two, so it will
mount '$env:HOME$env:USERPROFILE/.claude/' — which does not exist. Claude Code will not
be authenticated inside the container.
To fix: clear HOME for the session VS Code is launched from, or launch VS Code from
somewhere other than a Git Bash / msys shell.
"@
}

# Ensure a claude code user settings file is available on the host
New-Item -ItemType Directory -Force -Path (Join-Path $HOME '.claude') | Out-Null
if (!(Test-Path -LiteralPath (Join-Path $HOME '.claude.json'))) {
    Set-Content -LiteralPath (Join-Path $HOME '.claude.json') -Value '{}' -Encoding UTF8 -NoNewline
}
