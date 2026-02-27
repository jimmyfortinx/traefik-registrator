# traefik-registrator

Docker/file to Consul registrator focused on Traefik tags.

In `docker` mode, it listens to Docker container events and converts each `traefik.*` label into a Consul tag (`key=value`).
In `file` mode, it reads service definitions from a file or directory and keeps Consul in sync.

## Behavior

- Supports two discovery modes:
  - `SOURCE_MODE=docker` (default): discover services from running containers.
  - `SOURCE_MODE=file`: discover services from YAML/YML file(s).
- Runs a periodic full resync as a safety fallback in both modes.
- Registers a heartbeat service per registrator owner (`traefik-registrator-owner`).
- Periodically sweeps stale registrations cluster-wide:
  - removes services with missing `owner-id` after `ORPHAN_GRACE_PERIOD`
  - removes services whose owner heartbeat is not passing after `OWNER_DOWN_GRACE_PERIOD`
- In `docker` mode:
  - Watches Docker events (`start`, `stop`, `die`, `destroy`) and syncs immediately.
  - By default, only containers with `traefik.enable=true` are registered.
- Service ID in `docker` mode: `<SERVICE_ID_PREFIX><container-id-12chars>`.
- Service ID in `file` mode: explicit `id` from file or generated deterministic ID with `SERVICE_ID_PREFIX`.
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
- In `file` mode:
  - Watches source path changes via filesystem events and triggers sync automatically.
  - Reads services from a YAML/YML file or all `.yaml`/`.yml` files in a directory.
  - If source files are temporarily unavailable or invalid, sync fails and current Consul registrations are preserved.
- Deregisters services only when a sync successfully computes the desired state without those services.

## Environment

- `CONSUL_HTTP_ADDR` (default: `http://127.0.0.1:8500`)
- `CONSUL_HTTP_TOKEN` (optional)
- `SOURCE_MODE` (default: `docker`, values: `docker`, `file`)
- `DOCKER_SOCKET` (default: `/var/run/docker.sock`, docker mode only)
- `FILE_SOURCE_PATH` (default: `/etc/traefik-registrator/services.d`, file mode only)
- `POLL_INTERVAL` (default: `5m`, periodic full-resync interval)
- `REQUIRE_TRAEFIK_ENABLE` (default: `true`, docker mode only)
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

## File Mode Schema

`FILE_SOURCE_PATH` can point to:

- a single `.yaml` or `.yml` file, or
- a directory containing `.yaml`/`.yml` files.

Supported YAML shape:

- Object with `services` keyed map:

```yaml
services:
  whoami:
    address: whoami-file
    port: 80
    tags:
      - traefik.enable=true
      - traefik.http.routers.whoami-file.rule=Host(`whoami-file.local`)
```

Fields:

- `address` and `port` are required.
- `id` is optional.
- `name` is optional.
- Missing `name` defaults to the map key.
- Missing `id` defaults to `<SERVICE_ID_PREFIX><key>`.
- You do not need to duplicate the port in a Traefik `...loadbalancer.server.port` tag unless you explicitly want to override behavior in Traefik.
- `tags` and `meta` are optional.

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

Run in file mode (single central config path mounted read-only):

```bash
docker run -d \
  --name traefik-registrator-file \
  -v /srv/traefik-registrator/services.d:/etc/traefik-registrator/services.d:ro \
  -e SOURCE_MODE=file \
  -e FILE_SOURCE_PATH=/etc/traefik-registrator/services.d \
  -e CONSUL_HTTP_ADDR=http://consul.service.consul:8500 \
  -e OWNER_ID=file-source-a \
  -e SERVICE_ID_PREFIX=file- \
  --restart unless-stopped \
  traefik-registrator:local
```

## Local End-to-End Test

`docker-compose.yml` starts:

- `consul` in dev mode (UI/API on `http://localhost:8500`)
- `traefik` configured from `traefik.yml` with Consul Catalog provider (`http://localhost:8080`)
- `whoami` test app with Traefik labels
- `whoami-file` test app without Docker labels (registered by file mode)
- `registrator` built from this repository
- `registrator-file` built from this repository using file mode

Start the stack:

```bash
docker compose up --build -d
```

Check registrator logs:

```bash
docker compose logs -f registrator
```

Check file-mode registrator logs:

```bash
docker compose logs -f registrator-file
```

Verify that services were registered:

```bash
curl -s http://localhost:8500/v1/agent/services
```

The `whoami` entry should include tags generated from Docker labels, for example:

- `traefik.enable=true`
- `traefik.http.routers.whoami.rule=Host(\`whoami.local\`)`
- `traefik.http.services.whoami.loadbalancer.server.port=80`

The `file-whoami` entry should include tags loaded from `examples/services.d/whoami-file.yml`, including:

- `traefik.http.routers.whoami-file.rule=Host(\`whoami-file.local\`)`

Verify Traefik discovers and routes from Consul tags:

```bash
curl -s -H 'Host: whoami.local' http://localhost:8080
curl -s -H 'Host: whoami-file.local' http://localhost:8080
```

You should get the `whoami` response body.
If the first call returns `404`, wait about 15 seconds and retry (Consul Catalog refresh interval).

Stop and remove the stack:

```bash
docker compose down -v
```

## Automated Tests

The project now ships with:

- Go unit tests for registration/sync/GC logic.
- Compose integration tests that run against real Docker + Consul stacks.

Run unit tests only:

```bash
go test ./...
```

Run compose integration scenarios from your host:

```bash
RUN_COMPOSE_TESTS=1 go test -v ./...
```

Run all tests in an isolated container (no published ports):

```bash
docker compose -f tests/docker-compose.test.yml run --rm tests
```

## Compose Test Examples

All scenario files are in `examples/` and intentionally publish no ports, so multiple worktrees can run them concurrently.

- `examples/basic-registration`: registers a Traefik-enabled service, checks tags/meta, checks owner heartbeat service.
- `examples/traefik-enable-filter`: verifies `REQUIRE_TRAEFIK_ENABLE=true` filtering.
- `examples/custom-label-overrides`: verifies `SERVICE_*_LABEL` overrides and `REQUIRE_TRAEFIK_ENABLE=false`.
- `examples/gc-stale-services`: seeds stale catalog entries and verifies GC cleanup for orphan and dead-owner services.

## GitHub Actions

Workflow file: `.github/workflows/docker.yml`

- Runs on pull requests (build only).
- Runs on pushes to `main` and tags matching `v*`.
- Pushes multi-arch images (`linux/amd64`, `linux/arm64`) to:
  - `ghcr.io/<owner>/<repo>`
- Generates tags from branch, tag, commit SHA, and `latest` on the default branch.
