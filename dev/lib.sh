# shellcheck shell=bash
# Values two dev/ scripts share, sourced by both so each has one home; not a
# command of its own.

# The port the REST catalog fixture listens on, inside and outside its
# container: what dev/rest-fixture.sh publishes and what dev/test-int.sh
# probes when ICETRAY_REST_URI is not set. README quotes the URI as the
# default.
REST_CATALOG_PORT=8181
REST_CATALOG_URI="http://127.0.0.1:$REST_CATALOG_PORT"
