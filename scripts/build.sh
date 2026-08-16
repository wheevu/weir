#!/bin/sh
set -eu
preset=${1:-default}
cmake --preset "$preset"
cmake --build --preset "$preset"
ctest --preset "$preset" --output-on-failure
