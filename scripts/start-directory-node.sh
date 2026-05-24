#!/bin/sh
set -eu

tor -f /etc/tor/torrc &
tor_pid="$!"

for _ in $(seq 1 60); do
    if ! kill -0 "$tor_pid" 2>/dev/null; then
        wait "$tor_pid"
        exit $?
    fi
    if python3 - <<'PY'
import socket

with socket.create_connection(("127.0.0.1", 9051), timeout=1):
    pass
PY
    then
        exec python3 /src/scripts/start-dn.py "$@"
    fi
    sleep 1
done

echo "Timed out waiting for Tor control port 9051" >&2
kill "$tor_pid" 2>/dev/null || true
exit 1
