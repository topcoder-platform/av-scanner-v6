#!/bin/sh
set -eu
# Database files are shipped by the pinned upstream image. No root initialization
# or writable configuration is needed; freshclam maintains the writable database.
test -r /var/lib/clamav/main.cvd -o -r /var/lib/clamav/main.cld
freshclam --daemon --foreground --stdout --checks=24 &
updater=$!
clamd --foreground &
scanner=$!
cleanup() { kill "$updater" "$scanner" 2>/dev/null || true; wait || true; }
trap cleanup EXIT
trap 'exit 143' TERM INT
# A failed daemon must stop the essential container and trigger ECS rollback.
while kill -0 "$updater" 2>/dev/null && kill -0 "$scanner" 2>/dev/null; do sleep 2; done
exit 1
