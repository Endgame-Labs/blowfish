#!/usr/bin/env bash
# Wait until a server log has been quiet for a while, which for a server started with
# BF_TRACE_STAGES=1 means indexing and compaction have finished (every cycle prints "idx ..." lines).
# Usage: wait_idle.sh SERVER_LOG [QUIET_SECONDS=90]; prints the log's last-write time (epoch seconds).
log="$1"; quiet="${2:-90}"
while true; do
  now=$(date +%s); mtime=$(stat -c %Y "$log")
  if [ $((now - mtime)) -ge "$quiet" ]; then echo "$mtime"; exit 0; fi
  sleep 10
done
