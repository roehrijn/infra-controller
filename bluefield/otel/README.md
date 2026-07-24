# otelcol-contrib: updating the pinned module graph

The files `ocb-generated/go.mod` and `ocb-generated/go.sum` are
generated once by running OCB locally and committed to the repo. Both
the Docker build (`bluefield/containers/otelcol-contrib/Dockerfile`)
and the cargo-make `build-otelcol` task copy these files in before
compiling, making the dependency graph reproducible across builds.

They must be regenerated whenever:

- `otelcol_builder_config_yaml.txt` changes (new component, version bump)
- Any custom processor/receiver version (`version.go`) changes
- `otelcol_version.txt` is bumped

## Prerequisites

Install the OpenTelemetry Collector Builder (`ocb`) matching the
version in `otelcol_version.txt`. Download the binary 
from the [releases page](https://github.com/open-telemetry/opentelemetry-collector-releases/releases)
and place it on your PATH.

## Running the update

From the repo root:

```sh
bash bluefield/otel/update-ocb-modules.sh
```

The script substitutes version placeholders into the config template,
runs `ocb --skip-compilation` (which executes `go mod tidy` with network
access), and copies the resulting `go.mod` and `go.sum` into
`bluefield/otel/ocb-generated/`.

Verify the output before committing:

```sh
head -3 bluefield/otel/ocb-generated/go.mod
# must show: module otelcol-contrib
```

Then commit `ocb-generated/go.mod` and `ocb-generated/go.sum`. The
Docker build and cargo-make `build-otelcol` task will use the updated
module graph automatically on the next run.

