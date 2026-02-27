# traefik-registrator

Small Docker-to-Consul registrator focused on Traefik labels.

It listens to Docker container events, registers/deregisters services in Consul, and converts each `traefik.*` label into a Consul tag (`key=value`).

## Behavior

- Registers one Consul service per running container.
- Watches Docker events (`start`, `stop`, `die`, `destroy`) and syncs immediately.
- Runs a periodic full resync as a safety fallback.
- Registers a heartbeat service per registrator owner (`traefik-registrator-owner`).
- Periodically sweeps stale registrations cluster-wide:
  - removes services with missing `owner-id` after `ORPHAN_GRACE_PERIOD`
  - removes services whose owner heartbeat is not passing after `OWNER_DOWN_GRACE_PERIOD`
- By default, only containers with `traefik.enable=true` are registered.
- Service ID: `<SERVICE_ID_PREFIX><container-id-12chars>`.
- Service metadata:
  - `managed-by=traefik-registrator`
  - `owner-id=<OWNER_ID or hostname fallback>`
- Service name selection order:
  - `SERVICE_NAME_LABEL` (default: `com.docker.compose.service`)
  - `com.docker.compose.service`
  - first Docker container name
  - `DEFAULT_SERVICE_NAME`
  - container short ID
- Service address selection order:
  - label from `SERVICE_ADDRESS_LABEL` (default: `consul.address`)
  - first container network IP found
- Service port selection order:
  - label from `SERVICE_PORT_LABEL` (default: `consul.port`)
  - first Traefik label matching `traefik.http.services.*.loadbalancer.server.port`
  - first exposed private container port
- Deregisters services when containers disappear.

## Environment

- `CONSUL_HTTP_ADDR` (default: `http://127.0.0.1:8500`)
- `CONSUL_HTTP_TOKEN` (optional)
- `DOCKER_SOCKET` (default: `/var/run/docker.sock`)
- `POLL_INTERVAL` (default: `5m`, periodic full-resync interval)
- `REQUIRE_TRAEFIK_ENABLE` (default: `true`)
- `OWNER_ID` (optional, recommended: stable per-server ID; fallback: hostname)
- `OWNER_HEARTBEAT_TTL` (default: `30s`)
- `OWNER_HEARTBEAT_PASS_INTERVAL` (default: `10s`, must be lower than TTL)
- `GC_INTERVAL` (default: `1m`)
- `ORPHAN_GRACE_PERIOD` (default: `10m`)
- `OWNER_DOWN_GRACE_PERIOD` (default: `10m`)
- `SERVICE_ID_PREFIX` (default: `docker-`)
- `SERVICE_NAME_LABEL` (default: `com.docker.compose.service`)
- `SERVICE_PORT_LABEL` (default: `consul.port`)
- `SERVICE_ADDRESS_LABEL` (default: `consul.address`)
- `DEFAULT_SERVICE_NAME` (default: `container`)

## Build Image

```bash
docker build -t traefik-registrator:local .
```

## Run Container

```bash
docker run -d \
  --name traefik-registrator \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e CONSUL_HTTP_ADDR=http://consul.service.consul:8500 \
  -e OWNER_ID=server-a \
  -e ORPHAN_GRACE_PERIOD=10m \
  -e OWNER_DOWN_GRACE_PERIOD=10m \
  -e SERVICE_ID_PREFIX=docker- \
  --restart unless-stopped \
  traefik-registrator:local
```

If your workload does not set `traefik.enable=true`, disable that filter with:

```bash
-e REQUIRE_TRAEFIK_ENABLE=false
```

## Local End-to-End Test

`docker-compose.yml` starts:

- `consul` in dev mode (UI/API on `http://localhost:8500`)
- `traefik` configured from `traefik.yml` with Consul Catalog provider (`http://localhost:8080`)
- `whoami` test app with Traefik labels
- `registrator` built from this repository

Start the stack:

```bash
docker compose up --build -d
```

Check registrator logs:

```bash
docker compose logs -f registrator
```

Verify that the test app was registered:

```bash
curl -s http://localhost:8500/v1/agent/services
```

The entry for `whoami` should include tags generated from labels, for example:

- `traefik.enable=true`
- `traefik.http.routers.whoami.rule=Host(\`whoami.local\`)`
- `traefik.http.services.whoami.loadbalancer.server.port=80`

Verify Traefik discovers and routes from Consul tags:

```bash
curl -s -H 'Host: whoami.local' http://localhost:8080
```

You should get the `whoami` response body.
If the first call returns `404`, wait about 15 seconds and retry (Consul Catalog refresh interval).

Stop and remove the stack:

```bash
docker compose down -v
```

## GitHub Actions

Workflow file: `.github/workflows/docker.yml`

- Runs on pull requests (build only).
- Runs on pushes to `main` and tags matching `v*`.
- Pushes multi-arch images (`linux/amd64`, `linux/arm64`) to:
  - `ghcr.io/<owner>/<repo>`
- Generates tags from branch, tag, commit SHA, and `latest` on the default branch.
