#!/usr/bin/env bash
# The conformance run of RFD 012: the binary started the way an orchestrator
# starts it (the configuration path from ICETRAY_CONFIG, a JSON file, no
# flags) and every element of the surface checked: the JSON logs under the
# configured field names, the gRPC health service answering while tables
# load, readiness turning on after they serve, the metrics scrape, and the
# drain on SIGTERM to exit 0. It is one test of the binary's suite, run here
# by name so the run has a gate of its own; the workspace tests include it.
#
#   dev/conformance.sh
set -euo pipefail

cargo test -p icetray --locked --test cli test_serve_conformance_from_the_environment
