#!/bin/bash

# The '.claude' mounts in devcontainer.json resolve the host home directory as
# '${localEnv:HOME}${localEnv:USERPROFILE}', which gives the right path as long as only one
# of the two is set. USERPROFILE is not normally set outside Windows, so its presence here
# is the anomaly worth warning about — whether or not HOME is also set, the resulting mount
# path is wrong.
if [ -n "$USERPROFILE" ]; then
    cat >&2 <<EOF
WARNING: USERPROFILE is set on this host (HOME='$HOME', USERPROFILE='$USERPROFILE').
The dev container resolves your home directory as HOME followed by USERPROFILE, so it
will mount '$HOME$USERPROFILE/.claude/' — which does not exist. Claude Code will not be
authenticated inside the container. Unset USERPROFILE before launching VS Code.
EOF
fi

# Ensure a claude code user settings file is available on the host
mkdir -p "$HOME/.claude"
if [ ! -f "$HOME/.claude.json" ]; then
    printf '{}' > "$HOME/.claude.json"
fi
