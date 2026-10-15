# InfluxDB 3 Kubernetes Log Viewer

A Go 1.25 / controller-runtime application that follows container logs from Kubernetes, batches them into InfluxDB 3 Enterprise, and provides a real-time searchable web UI.

## What it does

- Reconciles namespaced `LogPipeline` CRs (`observability.influxdata.com/v1alpha1`).
- Discovers running pods and follows every application container through the Kubernetes logs API.
- Writes escaped line protocol to `POST /api/v3/write_lp?db=logs`.
- Queries with named parameters through `POST /api/v3/query_sql`.
- Streams successfully persisted records to browsers with resumable Server-Sent Events.
- Exposes standard-library HTTP endpoints:
  - `/healthz`
  - `/readyz`
  - `/metrics`
  - `/api/v1/logs`
  - `/api/v1/stream`

The InfluxDB table is created automatically on the first write:

```text
kubernetes_logs,namespace=...,pod=...,container=...,stream=... message="..." <nanoseconds>
```

## Prerequisites

- Go 1.25 (validated with Go 1.25.12)
- Kubernetes access
- An existing InfluxDB 3 Enterprise `logs` database
- A token that can write and query `logs`

The checked-in deployment targets the local `influxdb3` namespace and uses:

- InfluxDB write URL: `http://db3-influxdb3-enterprise-ingester.influxdb3.svc.cluster.local:8181`
- InfluxDB query URL: `http://db3-influxdb3-enterprise-querier.influxdb3.svc.cluster.local:8181`
- Secret: `influxdb3-admin-token`, key `TOKEN`
- Local image: `influx-log-viewer:local`, pinned to node `getup` for the current containerd import workflow

For production, replace the admin token with a resource token scoped to query/write the `logs` database.

The application runs as a one-replica StatefulSet with stable pod identity (`influx-log-viewer-0`). It has no PVC because its durable state is stored in InfluxDB; a headless governing Service provides StatefulSet DNS while the existing ClusterIP Service remains the UI/API entry point.

Non-secret runtime settings are loaded from ConfigMap `influx-log-viewer-config`. The InfluxDB token remains isolated in Secret `influxdb3-admin-token`; never place credentials in the ConfigMap. The runtime image includes Alpine `/bin/sh` and `curl` for in-pod diagnostics while retaining a non-root user, dropped capabilities, a read-only root filesystem, and a writable ephemeral `/tmp`.

## Build and test

```bash
make test vet build
make docker-build IMG=influx-log-viewer:local
```

## Deploy

Review the rendered base resources first. The sample `LogPipeline` is intentionally separate because `kubectl --dry-run=client` cannot discover a CRD that is only being created in the same command:

```bash
kubectl kustomize config
kubectl apply --dry-run=client -k config
```

Then install the CRD and application, wait for API discovery, and apply the sample:

```bash
kubectl apply -k config
kubectl wait --for=condition=Established crd/logpipelines.observability.influxdata.com --timeout=60s
kubectl apply --dry-run=client -k config/samples
kubectl apply -k config/samples
kubectl -n influxdb3 rollout status statefulset/influx-log-viewer
kubectl -n influxdb3 get logpipeline influxdb3-logs
kubectl -n influxdb3 port-forward svc/influx-log-viewer 8080:8080
```

Open `http://127.0.0.1:8080`.

If the cluster uses containerd and the image was built only in local Docker, import it on the node before deployment:

```bash
docker save influx-log-viewer:local | sudo ctr -n k8s.io images import -
```

No resources are applied by the build or test targets.

## Change runtime configuration

Edit non-secret settings through the ConfigMap:

```bash
kubectl -n influxdb3 edit configmap influx-log-viewer-config
kubectl -n influxdb3 rollout restart statefulset/influx-log-viewer
kubectl -n influxdb3 rollout status statefulset/influx-log-viewer
```

Supported ConfigMap keys are `HTTP_ADDR`, `INFLUXDB_URL`, `INFLUXDB_QUERY_URL`, and `INFLUXDB_DATABASE`. Environment variables sourced from a ConfigMap are read when the container starts, so restart the StatefulSet after changes.

For in-pod diagnostics:

```bash
kubectl -n influxdb3 exec -it influx-log-viewer-0 -- /bin/sh
id
curl -fsS http://127.0.0.1:8080/healthz
```

The shell runs as the unprivileged `app` user. The root filesystem remains read-only; `/tmp` is an ephemeral writable `emptyDir`.

## Configure collection

Edit `config/samples/observability_v1alpha1_logpipeline.yaml`:

```yaml
spec:
  targetNamespace: influxdb3
  podSelector:
    app.kubernetes.io/name: influxdb3-enterprise
  database: logs
  batchSize: 500
  flushInterval: 1s
```

An empty `podSelector` collects all running pod application containers in the target namespace.

## Search API

```bash
curl 'http://127.0.0.1:8080/api/v1/logs?q=error&namespace=influxdb3&since=1h&limit=200'
```

Supported filters: `q`, `namespace`, `pod`, `container`, `stream`, `since`, and `limit` (1-1000). Search values are passed to InfluxDB as named query parameters rather than interpolated SQL.

Search terms are combined with AND. Prefix a term with `-` to exclude messages containing that term:

```bash
curl --get 'http://127.0.0.1:8080/api/v1/logs' \
  --data-urlencode 'q=ingest -pacha' \
  --data-urlencode 'namespace=influxdb3' \
  --data-urlencode 'since=1h'
```

This returns messages containing `ingest` while omitting messages containing `pacha`. Exclusion matching is case-insensitive.

## Operational checks

```bash
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:8080/metrics
```

## Security and RBAC

The controller can list/watch pods, read `pods/log`, reconcile `LogPipeline` resources, and update their status. It cannot mutate pods or read arbitrary Kubernetes Secrets. The token enters the pod through a single explicit `secretKeyRef`.
