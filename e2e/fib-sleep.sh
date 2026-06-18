#!/usr/bin/env bash
# fib-sleep.sh — deterministic long-running child process for the t3 sub-agent
# e2e (scenario c, the ~1h fan-out+wait proof).
#
# Prints the Fibonacci sequence 1 1 2 3 5 8 13 21 and, after printing each
# number n, sleeps n "minutes". The unit is controlled by FIB_SCALE (seconds
# per Fibonacci unit, default 60 => real minutes). The cumulative sleep is
#   (1+1+2+3+5+8+13+21) * FIB_SCALE = 54 * FIB_SCALE seconds
# so the default run keeps this process — and therefore the t3 turn that
# launched it — alive for ~54 minutes.
#
# Examples:
#   ./fib-sleep.sh                 # 54 minutes (FIB_SCALE=60)
#   FIB_SCALE=1 ./fib-sleep.sh     # 54-second fast dry-run
#   FIB_SCALE=0.05 ./fib-sleep.sh  # ~2.7s smoke test
#
# Every line is timestamped so a transcript / log tail can prove the process
# stayed alive across the whole budget. Exits 0 only after the final sleep.

set -euo pipefail

FIB_SCALE="${FIB_SCALE:-60}"

FIBS=(1 1 2 3 5 8 13 21)

ts() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }

cumulative=0
echo "[$(ts)] fib-sleep start pid=$$ FIB_SCALE=${FIB_SCALE} sequence=${FIBS[*]}"

for n in "${FIBS[@]}"; do
  cumulative=$((cumulative + n))
  # Compute sleep seconds = n * FIB_SCALE, supporting fractional FIB_SCALE.
  secs="$(awk "BEGIN{printf \"%.3f\", ${n} * ${FIB_SCALE}}")"
  echo "[$(ts)] fib=${n} sleeping ${secs}s (cumulative ${cumulative} units)"
  sleep "${secs}"
  echo "[$(ts)] fib=${n} woke after ${secs}s"
done

echo "[$(ts)] fib-sleep done pid=$$ total_units=${cumulative} (= 54)"
