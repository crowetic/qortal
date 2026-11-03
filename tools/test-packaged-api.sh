#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
	printf 'Usage: %s <qortal.jar>\n' "$0" >&2
	exit 2
fi
[[ -f $1 ]] || { printf 'JAR does not exist: %s\n' "$1" >&2; exit 1; }
artifact=$(cd -- "$(dirname -- "$1")" && printf '%s/%s' "$PWD" "$(basename -- "$1")")
script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
work_directory=$(mktemp -d "${TMPDIR:-/tmp}/qortal-packaged-api.XXXXXX")
trap 'rm -rf -- "$work_directory"' EXIT
cd -- "$work_directory"

# The JAR is the only application/dependency classpath. The temporary working
# directory prevents loading the owner's settings or writing to their node.
# Java source-file mode requires a JDK (11+), as used to build Core.
java -Djava.awt.headless=true -cp "$artifact" \
	"$script_directory/PackagedApiSmokeTest.java" "$artifact"
