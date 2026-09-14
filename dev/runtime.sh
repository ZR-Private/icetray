#!/usr/bin/env bash
# The container runtime, needed only by the container-gated catalog test.
# Everything else in the build runs without one.
#
#   dev/runtime.sh up      start it and print what answered
#   dev/runtime.sh down    stop it, leaving the VM on disk
#   dev/runtime.sh status  is a runtime reachable?
#
# macOS has no container runtime of its own, so this starts a small Linux VM
# (colima) and points the docker client at it. On
# Linux the daemon is already there and `up` is a no-op.
set -euo pipefail

CPUS=4
MEMORY_GIB=8

usage() { sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-1}"; }

reachable() { docker info >/dev/null 2>&1; }

case "${1:-}" in
up)
  if reachable; then
    docker info --format 'runtime already up: {{.ServerVersion}} via {{.Name}}'
    exit 0
  fi
  if [ "$(uname -s)" != "Darwin" ]; then
    echo "no container runtime reachable, and this is not macOS: start the daemon your distribution ships" >&2
    exit 1
  fi
  # colima writes the docker client config here and fails on a missing directory.
  mkdir -p "${DOCKER_CONFIG:-$HOME/.docker}"
  # vz is Apple's own hypervisor; rosetta lets amd64 images run on Apple silicon,
  # which the Iceberg REST fixture image needs.
  colima start --cpu "$CPUS" --memory "$MEMORY_GIB" --vm-type vz --vz-rosetta --mount-inotify
  docker info --format 'runtime: {{.ServerVersion}} via {{.Name}}'
  ;;
down)
  [ "$(uname -s)" = "Darwin" ] || { echo "nothing to stop: the daemon is not ours to manage"; exit 0; }
  colima stop
  ;;
status)
  if reachable; then
    docker info --format 'up: {{.ServerVersion}} via {{.Name}}'
  else
    echo "down (run: dev/runtime.sh up)"
    exit 1
  fi
  ;;
*) usage ;;
esac
