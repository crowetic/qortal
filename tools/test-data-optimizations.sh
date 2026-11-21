#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${JAVA_HOME:?Set JAVA_HOME to an OpenJDK 11 installation}"
case "$("${JAVA_HOME}/bin/java" -version 2>&1 | head -1)" in
  *'version "11.'*) ;;
  *) printf '%s\n' 'This qualification suite requires OpenJDK 11.' >&2; exit 2 ;;
esac
export PATH="${JAVA_HOME}/bin:${PATH}"
cd "${repo_root}"
test_selection="$(paste -sd, tools/data-optimization-tests.txt)"
exec mvn -Dmaven.gitcommitid.nativegit=true -DskipJUnitTests=false "-Dtest=${test_selection}" "$@" test
