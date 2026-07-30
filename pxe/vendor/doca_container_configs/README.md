# Vendored HBN container configs (BlueField-2)

The DOCA HBN container configs the aarch64 BFB build stages into
`/opt/forge/doca_container_configs`. `bf.cfg` needs two of them at install time:
`scripts/hbn-dpu-setup.sh` (run via `ip vrf exec mgmt`) and
`configs/doca_hbn.yaml` (the HBN static-pod manifest).

**Why these are vendored rather than downloaded.** The build fetches this bundle
from NGC at `.../resources/org/nvidia/team/doca/doca_hbn/${DOCA_HBN_VERSION}/files`,
and NGC serves only recent versions. BlueField-2 needs the last BF2-capable
train, and **`2.4.1` now returns 404** — as does `2.4.0` and `2.4.3`. An upstream
URL that rots by design cannot be a build input, so the files live here.

**Content is from the `2.4.2` resource; the directory names say `2.4.1`.** That
looks inconsistent and is deliberate:

- `2.4.2` is the nearest version NGC still serves (verified: `2.4.2` -> 200,
  `2.4.1` -> 404).
- The build's `DOCA_HBN_VERSION` pin stays `2.4.1` because it also resolves the
  HBN *image* tag — `doca_hbn:${DOCA_HBN_VERSION}-doca${DOCA_VERSION}` =
  `doca_hbn:2.4.1-doca2.9.1`, which exists and is the image that has actually run
  HBN on this DPU. Bumping the pin to `2.4.2` would produce
  `doca_hbn:2.4.2-doca2.9.1`, which does **not** exist (the real tag is
  `2.4.2-doca2.9.2-32`, built for a DOCA base we do not run).
- So the pin drives the image, and these files supply the configs the pinned
  version can no longer fetch. `hbn-dpu-setup.sh` is byte-identical between
  `2.4.2` and `3.2.2`, i.e. version-agnostic.

**`configs/doca_hbn.yaml` is modified in one way:** the image reference is
retagged from `doca_hbn:2.4.2-doca2.9.2-32` to `doca_hbn:2.4.1-doca2.9.1` so it
matches the image the BFB actually carries. Everything else is upstream. NICo's
own `patch-hbn-manifest.py` still applies its init-sfs and `mgmt.intf` fixes at
install time — five of its six fixes match this manifest, and the sixth targets a
`journalctl -u sfc.service` call that HBN 2.4.x does not have, so its marker
check was widened to treat "nothing to patch" as satisfied.

### Refreshing these

Fetch the file list, then the signed URLs it contains:

```sh
curl -s https://api.ngc.nvidia.com/v2/resources/org/nvidia/team/doca/doca_hbn/2.4.2/files
```

Re-apply the image retag afterwards, and check whether
`patch-hbn-manifest.py`'s markers still match the new manifest before trusting a
build with it.
