# remote-consul-debug

Non-swarm Docker Compose setup for debugging file-mode registrations against external Consul servers over the published HTTP API port.

## Usage

```bash
cp .env.example .env
docker compose up --build -d
docker compose logs -f registrator-file
```

The stack runs:

- `registrator-file`: this repository's image in `SOURCE_MODE=file`, pointed directly at `CONSUL_HTTP_ADDR`.
- Optional `CONSUL_OVERLAY_HTTP_PORT` support for peer-node cleanup/pinning when overlay API traffic must use a different port than `CONSUL_HTTP_ADDR`.

Note: if only Consul HTTP (`:18500`) is exposed and gossip (`:8301`) is not, an external Consul client cannot join the cluster. This setup intentionally avoids joining and uses HTTP API access only.

## Helpful checks

```bash
curl -s "$CONSUL_HTTP_ADDR/v1/agent/self" | jq '.Config.NodeName,.Member.Addr'
curl -s "$CONSUL_HTTP_ADDR/v1/agent/services" | jq 'keys'
curl -s "$CONSUL_HTTP_ADDR/v1/catalog/service/debug-app" | jq '[.[] | {Node, ServiceID, Address, ServiceAddress}]'
```
