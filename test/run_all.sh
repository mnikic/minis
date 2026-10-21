#!/bin/bash
# Every test, in release, asan and ubsan (PROFILES="..." picks others):
# the C suites and the binary protocol cases against a server of the same
# profile (make test), then the redis-py suite in RESP2 and RESP3 (make
# resp-test). Then the JNI test, once. A failure does not stop the run;
# the summary lists them all and the exit status is 1.
#
#     make test-all
#     PROFILES=asan make test-all

set -u
cd "$(dirname "$0")/.."

PROFILES=${PROFILES:-"release asan ubsan"}
# A UBSan report must fail the run, not just be printed
export UBSAN_OPTIONS=${UBSAN_OPTIONS:-halt_on_error=1:print_stacktrace=1}
MAKE="make --no-print-directory"
SANITIZER_REPORT="runtime error|ERROR: AddressSanitizer|ERROR: LeakSanitizer"

failed=()
passed=()

step () {
  local name=$1
  shift
  echo
  echo "=== $name"
  if "$@"; then
    passed+=("$name")
  else
    failed+=("$name")
  fi
}

free_port () {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

wait_for_port () {
  python3 - "$1" <<'EOF'
import socket, sys, time
deadline = time.time() + 10
while time.time() < deadline:
    try:
        socket.create_connection(("127.0.0.1", int(sys.argv[1])), 0.2).close()
        sys.exit(0)
    except OSError:
        time.sleep(0.05)
sys.exit(1)
EOF
}

# make test against a server of the profile, started in a temporary
# directory so that its snapshots stay out of the tree
make_test_with_server () {
  local profile=$1
  local dir port pid status=0 server_status
  dir=$(mktemp -d -t minis-test-all-XXXXXX)
  port=$(free_port)

  (cd "$dir" && exec "$OLDPWD/build/bin/$profile/minis" -p "$port") \
    > "$dir/server.log" 2>&1 &
  pid=$!
  if ! wait_for_port "$port"; then
    echo "The $profile server did not start, its log:"
    cat "$dir/server.log"
    kill "$pid" 2> /dev/null
    wait "$pid" 2> /dev/null
    return 1
  fi

  MINIS_PORT=$port $MAKE PROFILE="$profile" test || status=1

  # The server saves and exits on SIGTERM; a sanitizer makes it fail
  kill -TERM "$pid"
  wait "$pid"
  server_status=$?
  if [ "$server_status" -ne 0 ]; then
    echo "The $profile server exited with status $server_status"
    status=1
  fi
  if grep -qE "$SANITIZER_REPORT" "$dir/server.log"; then
    echo "The $profile server reported:"
    grep -E -A20 "$SANITIZER_REPORT" "$dir/server.log" | head -60
    status=1
  fi

  if [ "$status" -eq 0 ]; then
    rm -rf "$dir"
  else
    echo "Server log kept in $dir/server.log"
  fi
  return "$status"
}

start=$(date +%s)
for profile in $PROFILES; do
  step "$profile: build" $MAKE -j"$(nproc)" PROFILE="$profile" all \
    "build/bin/$profile/heap_test" "build/bin/$profile/cache_test" \
    "build/bin/$profile/persistence_test"
  step "$profile: C suites and binary protocol cases" \
    make_test_with_server "$profile"
  step "$profile: redis-py, RESP2 and RESP3" $MAKE PROFILE="$profile" resp-test
done

if command -v javac > /dev/null; then
  step "JNI" $MAKE jni-test
else
  echo
  echo "=== JNI: skipped, no javac"
fi

echo
echo "========================================"
echo "test-all: ${#passed[@]} passed, ${#failed[@]} failed" \
  "in $(( $(date +%s) - start ))s"
for name in "${passed[@]}"; do
  echo "  ok    $name"
done
for name in "${failed[@]}"; do
  echo "  FAIL  $name"
done
echo "========================================"
[ ${#failed[@]} -eq 0 ]
