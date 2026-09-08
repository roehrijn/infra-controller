#!/bin/sh
#
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Wraps the bf.cfg that gets appended to forge.bfb in inert padding, and
# asserts that the appended tail of a built forge.bfb is exactly that.
#
# Why the padding exists: the appended bf.cfg reaches the DPU installer through
# the rshim boot FIFO, 8 bytes per word, and on a BlueField-2 behind a
# BF-24.10 BMC that path was observed to drop 232 consecutive bytes (29 words)
# out of the middle of a 4894-byte cfg, at a fixed position relative to the
# stream rather than to the content. The hole took out the `bfb_modify_os() {`
# opener; the installer's `bash -n` then rejected the whole file
# ("syntax error near unexpected token `}'"), logged "No bf.cfg provided",
# and finished as stock DOCA with no forge-scout and no agent, i.e. an Arm that
# looks healthy and never phones home. Nothing on the consumer side can
# recover lost words, so the cfg is surrounded by padding that absorbs the
# hole wherever it lands: numbered comment lines of exactly 8 bytes each, so
# one line is one FIFO word, any word-aligned loss removes whole lines, and a
# cut can at worst join two comments into one. The padded file must parse
# identically to the unpadded one and stays 8-byte aligned so no NUL padding
# is ever needed on the wire.
#
# The trailing pad is a fixed byte string (lines #100001..#100512). Downstream
# tooling keys on its digest to tell an image that pads from one that does
# not; do not change its shape without changing that contract.
#
# Usage:
#   bfcfg-pad.sh wrap   UNPADDED_CFG OUT_CFG
#   bfcfg-pad.sh assert FORGE_BFB    CFG        UNPADDED_CFG
set -eu

LEAD_LINES=1024   # 8 KiB before the cfg
TRAIL_LINES=512   # 4 KiB after it
TRAIL_BASE=100000 # trailing lines are numbered #100001..#100512
WORD=8
# md5 of the trailing pad as a literal, deliberately NOT derived from the
# generator above: if the pad's shape drifts, both sides of a regenerated
# comparison drift together and the assertion would still pass. The other end
# of this contract is the boot-artifacts-aarch64 init container in the homelab's
# ansible/roles/nico_core/templates/nico-core.yaml.j2 (nico-homelab repo), which
# hashes the last 4096 bytes of the served forge.bfb and, on this value,
# reports `padded by-image` and leaves the artifact alone; on anything else it
# re-pads the tail itself. Recognition rests on this hash only. The unpadded
# cfg's own digest is intentionally not asserted here: that side only uses it
# to find the cfg it must pad when the image does not, and pinning it would
# fail the build on every legitimate edit to pxe/templates/bmc_fw_update.
TRAIL_MD5=67c0f15a711141dadbb941bb13656043

die() {
  echo "bfcfg-pad: $*" >&2
  exit 1
}

# pad COUNT BASE: COUNT lines of the form '#%06d\n' numbered BASE+1..BASE+COUNT.
pad() {
  i=1
  while [ "$i" -le "$1" ]; do
    printf '#%06d\n' "$(( $2 + i ))"
    i=$((i + 1))
  done
}

# fill LEN: the newlines that bring LEN up to the next multiple of WORD.
fill() {
  n=$(( (WORD - $1 % WORD) % WORD ))
  i=0
  while [ "$i" -lt "$n" ]; do
    printf '\n'
    i=$((i + 1))
  done
}

wrap() {
  in=$1
  out=$2
  [ -s "$in" ] || die "wrap: $in is missing or empty"
  bash -n "$in" || die "wrap: $in does not parse before padding"
  len=$(wc -c < "$in")
  {
    pad "$LEAD_LINES" 0
    cat "$in"
    fill "$len"
    pad "$TRAIL_LINES" "$TRAIL_BASE"
  } > "$out"
}

assert() {
  bfb=$1
  cfg=$2
  unpadded=$3
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT

  [ -s "$bfb" ] || die "assert: $bfb is missing or empty"
  [ -s "$cfg" ] || die "assert: $cfg is missing or empty"
  [ -s "$unpadded" ] || die "assert: $unpadded is missing or empty"

  size=$(wc -c < "$cfg")
  [ $((size % WORD)) -eq 0 ] || die "assert: $cfg is $size bytes, not a multiple of $WORD"

  # The BFB really ends in the cfg; nothing trusted the concatenation.
  tail -c "$size" "$bfb" > "$tmp/tail"
  cmp -s "$tmp/tail" "$cfg" || die "assert: the last $size bytes of $bfb are not $cfg"

  # The cfg is lead pad + unpadded cfg + alignment + trail pad, byte for byte.
  pad "$LEAD_LINES" 0 > "$tmp/lead"
  pad "$TRAIL_LINES" "$TRAIL_BASE" > "$tmp/trail"
  ulen=$(wc -c < "$unpadded")
  { cat "$unpadded"; fill "$ulen"; } > "$tmp/body"
  cat "$tmp/lead" "$tmp/body" "$tmp/trail" > "$tmp/expected"
  cmp -s "$tmp/expected" "$cfg" || die "assert: $cfg is not lead pad + $unpadded + trail pad"
  lead_len=$(wc -c < "$tmp/lead")
  trail_len=$(wc -c < "$tmp/trail")
  [ "$lead_len" -eq $((LEAD_LINES * WORD)) ] || die "assert: lead pad is $lead_len bytes"
  [ "$trail_len" -eq $((TRAIL_LINES * WORD)) ] || die "assert: trail pad is $trail_len bytes"
  trail_md5=$(tail -c "$trail_len" "$bfb" | md5sum | cut -d' ' -f1)
  [ "$trail_md5" = "$TRAIL_MD5" ] || die "assert: the last $trail_len bytes of $bfb hash to $trail_md5, contract is $TRAIL_MD5"

  # Padding changed nothing the installer cares about.
  bash -n "$cfg" || die "assert: $cfg does not parse"
  grep -q '^  bfb_modify_os() {' "$cfg" || die "assert: $cfg does not define bfb_modify_os"

  echo "bfcfg-pad: ok, $bfb ends in a $size-byte cfg (lead $lead_len, body $ulen, trail $trail_len)"
}

case "${1:-}" in
  wrap)   [ $# -eq 3 ] || die "usage: $0 wrap UNPADDED_CFG OUT_CFG";        wrap "$2" "$3" ;;
  assert) [ $# -eq 4 ] || die "usage: $0 assert FORGE_BFB CFG UNPADDED_CFG"; assert "$2" "$3" "$4" ;;
  *)      die "usage: $0 wrap UNPADDED_CFG OUT_CFG | assert FORGE_BFB CFG UNPADDED_CFG" ;;
esac
