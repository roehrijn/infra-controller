#!/usr/bin/env bash
# A [patch] whose declared version no longer satisfies the dependent's semver
# requirement is silently ignored by cargo: the build succeeds and ships the
# unpatched crates.io copy instead of the vendored one. Cargo only emits a
# warning ("patch ... was not used in the crate graph"), which scrolls by
# unread in a container build; for the vendored nv-redfish crates that means
# losing the iDRAC8 and null-Members fixes with no build-time signal at all,
# and the first symptom is a failed BMC explore in production. Turn the
# warning into a hard failure instead.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! warnings=$(cargo metadata --format-version 1 --locked 2>&1 >/dev/null); then
  echo "$warnings" >&2
  exit 1
fi

if grep -q 'was not used in the crate graph' <<<"$warnings"; then
  echo "$warnings" >&2
  echo >&2
  echo "ERROR: a [patch] in Cargo.toml was not used in the crate graph." >&2
  echo "A vendored crate's declared version most likely no longer satisfies" >&2
  echo "the dependent's requirement; the build would silently use the" >&2
  echo "unpatched upstream copy. Align the vendored crate's version with the" >&2
  echo "workspace dependency before building." >&2
  exit 1
fi
