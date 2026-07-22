# Machine-a-tron Kubernetes Controller

A Kubernetes controller that reconciles Services for machine-a-tron mock BMC endpoints.

## Overview

When machine-a-tron runs in Kubernetes, mock BMCs need stable network exposure so that
NICo components can reach Redfish and IPMI/SOL endpoints.
This controller polls machine-a-tron status and reconciles one Service per mock BMC.

Machine-a-tron itself remains Kubernetes-agnostic. This controller bridges the gap.

## Features

- Polls machine-a-tron `GET /machines/status` API
- Creates one Service per mock BMC (hosts and DPUs)
- Exposes Redfish TCP (port 443 → internal listen port)
- Exposes IPMI UDP when enabled (port 623 → internal listen port)
- Labels and annotates Services with machine/BMC identity
- Updates Services when machine status changes
- Deletes stale Services when machines disappear

## Build

```bash
# Build binary
go build -o mat-k8s-controller ./cmd/mat-k8s-controller

# Build container
docker build -t mat-k8s-controller:latest .

# Load to kind cluster
kind load docker-image mat-k8s-controller:latest --name <cluster>
```

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--mat-url` | `MAT_URL` | `https://nico-machine-a-tron-bmc-mock:1266` | Machine-a-tron base URL |
| `--namespace` | `NAMESPACE` | `nico-system` | Namespace for Services |
| `--sync-interval` | `SYNC_INTERVAL` | `30s` | Reconciliation interval |
| `--target-selector` | `TARGET_SELECTOR` | `app.kubernetes.io/name=nico-machine-a-tron` | Pod selector |
| `--insecure-skip-verify` | `INSECURE_SKIP_VERIFY` | `true` | Skip TLS verification |
| `--log-level` | `LOG_LEVEL` | `info` | Log level |
| `--cluster-ip-prefix` | `CLUSTER_IP_PREFIX` | (none) | Static ClusterIP prefix |

## Helm Deployment

The controller is a subchart of `nico-machine-a-tron`. When enabled, it dynamically
manages BMC Services instead of static Helm-generated ones.

```bash
helm upgrade --install nico-machine-a-tron ./helm/charts/nico-machine-a-tron \
  --namespace nico-system \
  --create-namespace \
  --set machineATron.enableIpmiSimulation=true \
  --set mat-k8s-controller.enabled=true \
  --set mat-k8s-controller.image.pullPolicy=Never
```

Configuration is inherited from the parent chart:
- `matUrl` → derived from release name
- `namespace` → release namespace
- `targetSelector` → matches machine-a-tron pods

## Service Structure

Each Service has:

**Labels:**
- `app.kubernetes.io/managed-by: mat-k8s-controller`
- `machine-a-tron.nvidia.com/mat-id`
- `machine-a-tron.nvidia.com/machine-type` (host/dpu)

**Annotations:**
- `machine-a-tron.nvidia.com/bmc-ip`
- `machine-a-tron.nvidia.com/api-state`
- `machine-a-tron.nvidia.com/power-state`

**Ports:**
- `redfish` TCP 443 → targetPort (always)
- `ipmi` UDP 623 → targetPort (when IPMI enabled)

## Development

```bash
# Run tests
go test ./...

# Run locally
./mat-k8s-controller --kubeconfig ~/.kube/config --mat-url https://localhost:1266
```

## Architecture

```mermaid
flowchart LR
    subgraph MAT[machine-a-tron]
        API["/machines/status"]
    end

    subgraph K8s[Kubernetes]
        Controller[mat-k8s-controller]
        subgraph Services
            HostSvc[mat-bmc-host-xxx]
            DpuSvc[mat-bmc-dpu-yyy]
        end
    end

    Controller -->|polls| API
    Controller -->|creates/updates/deletes| Services
```

## Related

- [GitHub Issue #3380](https://github.com/NVIDIA/infra-controller/issues/3380) - Feature request
- [GitHub Issue #3379](https://github.com/NVIDIA/infra-controller/issues/3379) - Status API
