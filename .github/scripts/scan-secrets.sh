#!/usr/bin/env bash
#
# Fails when a tracked file contains a secret that is not recorded in .secrets.baseline.
# Shared by detect-secrets.yml and build-and-test.yml. The tool reports only the file, the line
# and the detector type, never the value: do not add verbose or JSON options here.
set -euo pipefail
pipx install --force detect-secrets==1.5.0
git ls-files -z | xargs -0 detect-secrets-hook --baseline .secrets.baseline
