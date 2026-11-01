# One scenario run at a time on the shared compose stand, across every worktree of this repo. lib.sh sources this file;
# ad-hoc load does the same and calls stand_acquire. Run this file directly to self-check it.
# ponytail: waiters retry every 5 s with no FIFO; add a queue if more than two runs ever wait at once.
STAND_LOCK=${STAND_LOCK:-/tmp/kora-outbox-bench.lock}

# The lock is a symlink: creating one is atomic and its target carries the owner, "PID|start time|directory". A PID with
# a different start time was reused, so that owner is gone. A parent scenario's lock covers the scenarios it starts.
stand_acquire() {
  [ -z "${STAND_LOCK_HELD:-}" ] || return 0
  local me waited=0 owner pid rest
  me="$$|$(ps -o lstart= -p $$)|$PWD"
  while ! ln -s "$me" "$STAND_LOCK" 2> /dev/null; do
    owner=$(readlink "$STAND_LOCK" 2> /dev/null) || { command sleep 1; continue; }
    pid=${owner%%|*}
    rest=${owner#*|}
    if [ "$(ps -o lstart= -p "$pid" 2> /dev/null)" != "${rest%%|*}" ]; then
      # Rename first, so that of several waiters only one reclaims; a lock taken meanwhile goes back.
      mv "$STAND_LOCK" "$STAND_LOCK.$$" 2> /dev/null || continue
      if [ "$(readlink "$STAND_LOCK.$$")" = "$owner" ]; then rm -f "$STAND_LOCK.$$"; else mv "$STAND_LOCK.$$" "$STAND_LOCK"; fi
      continue
    fi
    if [ $((waited % 60)) -eq 0 ]; then echo "stand busy: pid $pid in ${rest#*|}, waited ${waited}s" >&2; fi
    command sleep 5
    waited=$((waited + 5))
  done
  STAND_LOCK_OWNED=1
  export STAND_LOCK_HELD=1
}

stand_release() {
  if [ -z "${STAND_LOCK_OWNED:-}" ]; then return 0; fi
  rm -f "$STAND_LOCK"
  STAND_LOCK_OWNED=
  unset STAND_LOCK_HELD
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  set -u
  self=$0
  STAND_LOCK=$(mktemp -u)
  export STAND_LOCK
  fail() { echo "FAIL: $1" >&2; exit 1; }

  # Taken, then free again after release.
  stand_acquire; [ -L "$STAND_LOCK" ] || fail "not taken"
  stand_release; [ ! -L "$STAND_LOCK" ] || fail "not released"

  # An owner that is gone does not block: this start time matches no live process.
  ln -s "1|never|/nowhere" "$STAND_LOCK"
  stand_acquire
  [ "$(readlink "$STAND_LOCK")" != "1|never|/nowhere" ] || fail "stale owner not reclaimed"
  stand_release

  # A nested scenario neither waits nor releases its parent's lock.
  stand_acquire
  bash -c 'source "$1"; [ "${STAND_LOCK_HELD:-}" = 1 ] && stand_acquire && stand_release' _ "$self" || fail "nested run"
  [ -L "$STAND_LOCK" ] || fail "nested run released the lock"
  stand_release

  # A second run waits until the first one exits, then takes the lock.
  order=$(mktemp)
  bash -c 'source "$1"; stand_acquire; echo first >> "$2"; sleep 2; echo first-done >> "$2"; stand_release' _ "$self" "$order" &
  sleep 1
  bash -c 'source "$1"; stand_acquire; echo second >> "$2"; stand_release' _ "$self" "$order" 2> /dev/null
  wait
  [ "$(tr '\n' ' ' < "$order")" = "first first-done second " ] || fail "order: $(cat "$order")"
  rm -f "$order"
  echo "stand-lock: ok"
fi
