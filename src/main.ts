import { watch } from "node:fs";
import { promises as fs } from "node:fs";
import http from "node:http";
import path from "node:path";
import process from "node:process";
import { parse as parseYaml } from "yaml";

type SourceMode = "docker" | "file";

type Config = {
  consulHTTPAddr: string;
  consulToken: string;
  sourceMode: SourceMode;
  dockerSocket: string;
  fileSourcePath: string;
  resyncIntervalMs: number;
  requireTraefikEnable: boolean;
  ownerID: string;
  ownerHeartbeatTTLMs: number;
  ownerHeartbeatPassMs: number;
  gcIntervalMs: number;
  orphanGraceMs: number;
  ownerDownGraceMs: number;
  serviceIDPrefix: string;
  serviceNameLabel: string;
  servicePortLabel: string;
  serviceAddressLabel: string;
  defaultServiceName: string;
};

type DockerPort = { PrivatePort?: number };
type DockerNetworkEndpoint = { IPAddress?: string };
type DockerContainer = {
  Id?: string;
  Names?: string[];
  Labels?: Record<string, string>;
  Ports?: DockerPort[];
  NetworkSettings?: { Networks?: Record<string, DockerNetworkEndpoint> };
};

type ConsulServiceRegistration = {
  ID: string;
  Name: string;
  Address?: string;
  Port?: number;
  Tags?: string[];
  Meta?: Record<string, string>;
  Check?: {
    Name?: string;
    TTL?: string;
    DeregisterCriticalServiceAfter?: string;
  };
};

type AgentService = { ID?: string; Meta?: Record<string, string> };
type OwnerCheck = { Node?: string; ServiceID?: string; Status?: string };
type CatalogInstance = {
  Node?: string;
  ServiceID?: string;
  ServiceName?: string;
  ServiceMeta?: Record<string, string>;
};

type FileServiceDefinition = {
  id?: string;
  name?: string;
  address?: string;
  port?: number;
  tags?: string[];
  meta?: Record<string, string>;
};

const sourceModeDocker: SourceMode = "docker";
const sourceModeFile: SourceMode = "file";
const managedByMetaKey = "managed-by";
const managedByMetaValue = "traefik-registrator";
const ownerIDMetaKey = "owner-id";
const ownerHeartbeatService = "traefik-registrator-owner";
const ownerHeartbeatIDPrefix = "traefik-registrator-owner-";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function env(name: string, fallback: string): string {
  const value = process.env[name];
  return value === undefined || value.trim() === "" ? fallback : value.trim();
}

function parseBool(raw: string): boolean {
  return ["1", "true", "yes", "on"].includes(raw.trim().toLowerCase());
}

function parseDurationMs(raw: string): number {
  const value = raw.trim();
  if (value === "") {
    throw new Error("duration cannot be empty");
  }

  const match = value.match(/^(-?\d+(?:\.\d+)?)(ms|s|m|h)$/i);
  if (!match) {
    throw new Error(`invalid duration ${value}; expected format like 500ms, 2s, 5m, 1h`);
  }

  const amount = Number(match[1]);
  const unit = match[2].toLowerCase();
  const multipliers: Record<string, number> = {
    ms: 1,
    s: 1000,
    m: 60_000,
    h: 3_600_000,
  };

  return Math.floor(amount * multipliers[unit]);
}

function durationToConsulString(ms: number): string {
  if (ms % 3_600_000 === 0) return `${ms / 3_600_000}h`;
  if (ms % 60_000 === 0) return `${ms / 60_000}m`;
  if (ms % 1000 === 0) return `${ms / 1000}s`;
  return `${ms}ms`;
}

function loadConfig(): Config {
  const ownerFromHost = process.env.HOSTNAME?.trim() ?? "";
  return {
    consulHTTPAddr: env("CONSUL_HTTP_ADDR", "http://127.0.0.1:8500"),
    consulToken: env("CONSUL_HTTP_TOKEN", ""),
    sourceMode: env("SOURCE_MODE", "docker") === "file" ? sourceModeFile : sourceModeDocker,
    dockerSocket: env("DOCKER_SOCKET", "/var/run/docker.sock"),
    fileSourcePath: env("FILE_SOURCE_PATH", "/etc/traefik-registrator/services.d"),
    resyncIntervalMs: parseDurationMs(env("POLL_INTERVAL", "5m")),
    requireTraefikEnable: parseBool(env("REQUIRE_TRAEFIK_ENABLE", "true")),
    ownerID: env("OWNER_ID", ownerFromHost),
    ownerHeartbeatTTLMs: parseDurationMs(env("OWNER_HEARTBEAT_TTL", "30s")),
    ownerHeartbeatPassMs: parseDurationMs(env("OWNER_HEARTBEAT_PASS_INTERVAL", "10s")),
    gcIntervalMs: parseDurationMs(env("GC_INTERVAL", "1m")),
    orphanGraceMs: parseDurationMs(env("ORPHAN_GRACE_PERIOD", "10m")),
    ownerDownGraceMs: parseDurationMs(env("OWNER_DOWN_GRACE_PERIOD", "10m")),
    serviceIDPrefix: env("SERVICE_ID_PREFIX", "docker-"),
    serviceNameLabel: env("SERVICE_NAME_LABEL", "com.docker.compose.service"),
    servicePortLabel: env("SERVICE_PORT_LABEL", "consul.port"),
    serviceAddressLabel: env("SERVICE_ADDRESS_LABEL", "consul.address"),
    defaultServiceName: env("DEFAULT_SERVICE_NAME", "container"),
  };
}

async function validateConfig(cfg: Config): Promise<void> {
  if (!cfg.ownerID.trim()) {
    throw new Error("OWNER_ID resolved to empty value");
  }
  if (cfg.ownerHeartbeatPassMs >= cfg.ownerHeartbeatTTLMs) {
    throw new Error("OWNER_HEARTBEAT_PASS_INTERVAL must be smaller than OWNER_HEARTBEAT_TTL");
  }

  if (cfg.sourceMode === sourceModeDocker) {
    if (!cfg.dockerSocket.trim()) {
      throw new Error("DOCKER_SOCKET cannot be empty when SOURCE_MODE=docker");
    }
    return;
  }

  if (!cfg.fileSourcePath.trim()) {
    throw new Error("FILE_SOURCE_PATH cannot be empty when SOURCE_MODE=file");
  }

  try {
    const stat = await fs.stat(cfg.fileSourcePath);
    if (!stat.isDirectory() && !isSupportedFileSourcePath(cfg.fileSourcePath)) {
      throw new Error("FILE_SOURCE_PATH must be a .yaml/.yml file or directory containing .yaml/.yml files");
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      throw error;
    }
  }
}

function dockerRequest(cfg: Config, method: string, reqPath: string, body?: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        socketPath: cfg.dockerSocket,
        path: reqPath,
        method,
        headers: {
          Host: "docker",
          ...(body ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) } : {}),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
        res.on("end", () => {
          const payload = Buffer.concat(chunks).toString("utf-8");
          const status = res.statusCode ?? 0;
          if (status < 200 || status >= 300) {
            reject(new Error(`docker API ${method} ${reqPath} returned ${status}: ${payload}`));
            return;
          }
          resolve(payload);
        });
      },
    );

    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

async function listRunningContainers(cfg: Config): Promise<DockerContainer[]> {
  const body = await dockerRequest(cfg, "GET", "/containers/json?all=0");
  const parsed = JSON.parse(body);
  return Array.isArray(parsed) ? parsed : [];
}

async function consulRequest(cfg: Config, method: string, requestPath: string, body?: unknown): Promise<Response> {
  const url = `${cfg.consulHTTPAddr.replace(/\/$/, "")}${requestPath}`;
  const headers: Record<string, string> = body === undefined ? {} : { "Content-Type": "application/json" };
  if (cfg.consulToken) headers["X-Consul-Token"] = cfg.consulToken;

  const response = await fetch(url, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`consul API ${method} ${requestPath} returned ${response.status}: ${text}`);
  }
  return response;
}

async function registerService(cfg: Config, reg: ConsulServiceRegistration): Promise<void> {
  await consulRequest(cfg, "PUT", "/v1/agent/service/register", reg);
}

async function deregisterService(cfg: Config, id: string): Promise<void> {
  await consulRequest(cfg, "PUT", `/v1/agent/service/deregister/${encodeURIComponent(id)}`);
}

async function listAgentServices(cfg: Config): Promise<Record<string, AgentService>> {
  const response = await consulRequest(cfg, "GET", "/v1/agent/services");
  return ((await response.json()) as Record<string, AgentService>) ?? {};
}

async function registerOwnerHeartbeatService(cfg: Config, serviceID: string): Promise<void> {
  await registerService(cfg, {
    ID: serviceID,
    Name: ownerHeartbeatService,
    Meta: {
      [managedByMetaKey]: managedByMetaValue,
      [ownerIDMetaKey]: cfg.ownerID,
      kind: "owner-heartbeat",
    },
    Check: {
      Name: "owner heartbeat",
      TTL: durationToConsulString(cfg.ownerHeartbeatTTLMs),
      DeregisterCriticalServiceAfter: durationToConsulString(cfg.ownerDownGraceMs),
    },
  });
}

async function passOwnerHeartbeat(cfg: Config, serviceID: string): Promise<void> {
  await consulRequest(cfg, "PUT", `/v1/agent/check/pass/${encodeURIComponent(`service:${serviceID}`)}`);
}

function shortID(raw: string): string {
  return raw.length > 12 ? raw.slice(0, 12) : raw;
}

function isTruthy(raw: string | undefined): boolean {
  if (!raw) return false;
  return ["1", "true", "yes", "on"].includes(raw.trim().toLowerCase());
}

function serviceName(cfg: Config, labels: Record<string, string>, names: string[] | undefined, containerID: string): string {
  const preferred = labels[cfg.serviceNameLabel]?.trim();
  if (preferred) return preferred;

  const composeService = labels["com.docker.compose.service"]?.trim();
  if (composeService) return composeService;

  const firstName = names?.find((entry) => entry.trim() !== "")?.replace(/^\//, "").trim();
  if (firstName) return firstName;

  if (cfg.defaultServiceName.trim()) return cfg.defaultServiceName.trim();
  return shortID(containerID);
}

function serviceAddress(
  cfg: Config,
  labels: Record<string, string>,
  networks: Record<string, DockerNetworkEndpoint> | undefined,
): string {
  const fromLabel = labels[cfg.serviceAddressLabel]?.trim();
  if (fromLabel) return fromLabel;

  if (!networks) return "";
  for (const key of Object.keys(networks).sort()) {
    const ip = networks[key]?.IPAddress?.trim() ?? "";
    if (ip) return ip;
  }
  return "";
}

function servicePort(cfg: Config, labels: Record<string, string>, ports: DockerPort[] | undefined): number {
  const custom = Number(labels[cfg.servicePortLabel] ?? "");
  if (Number.isInteger(custom) && custom > 0) return custom;

  const keys = Object.keys(labels).sort();
  for (const key of keys) {
    if (!key.startsWith("traefik.http.services.") || !key.endsWith(".loadbalancer.server.port")) {
      continue;
    }
    const candidate = Number(labels[key]);
    if (Number.isInteger(candidate) && candidate > 0) return candidate;
  }

  for (const port of ports ?? []) {
    if (Number.isInteger(port.PrivatePort) && (port.PrivatePort ?? 0) > 0) {
      return Number(port.PrivatePort);
    }
  }

  return 0;
}

function traefikTags(labels: Record<string, string>): string[] {
  const tags = Object.entries(labels)
    .filter(([key]) => key.startsWith("traefik."))
    .map(([key, value]) => `${key}=${value}`)
    .sort();
  return tags;
}

function buildDockerRegistration(cfg: Config, container: DockerContainer): ConsulServiceRegistration | null {
  const id = (container.Id ?? "").trim();
  if (!id) return null;

  const labels = container.Labels ?? {};
  if (cfg.requireTraefikEnable && !isTruthy(labels["traefik.enable"])) {
    return null;
  }

  const address = serviceAddress(cfg, labels, container.NetworkSettings?.Networks);
  if (!address) {
    console.log(`skip ${shortID(id)}: no container IP found`);
    return null;
  }

  const port = servicePort(cfg, labels, container.Ports);
  if (port <= 0) {
    console.log(`skip ${shortID(id)}: no service port found`);
    return null;
  }

  return {
    ID: `${cfg.serviceIDPrefix}${shortID(id)}`,
    Name: serviceName(cfg, labels, container.Names, id),
    Address: address,
    Port: port,
    Tags: traefikTags(labels),
    Meta: {
      [managedByMetaKey]: managedByMetaValue,
      [ownerIDMetaKey]: cfg.ownerID,
    },
  };
}

function normalizeFileServiceKey(raw: string): string {
  const value = raw.trim().toLowerCase();
  if (!value) return "";

  let out = "";
  let lastDash = false;
  for (const ch of value) {
    const allowed = /[a-z0-9_.-]/.test(ch);
    if (allowed) {
      out += ch;
      lastDash = false;
    } else if (!lastDash) {
      out += "-";
      lastDash = true;
    }
  }
  return out.replace(/^-+/, "").replace(/-+$/, "");
}

function hashString(input: string): string {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16);
}

function deriveFileServiceID(cfg: Config, sourceFile: string, key: string, svc: FileServiceDefinition): string {
  const seed = [
    sourceFile,
    key,
    svc.name?.trim() ?? "",
    svc.address?.trim() ?? "",
    String(svc.port ?? ""),
    (svc.tags ?? []).slice().sort().join(","),
  ].join("|");

  return `${cfg.serviceIDPrefix}file-${hashString(seed)}`;
}

async function collectFileSourcePaths(sourcePath: string): Promise<string[]> {
  const stat = await fs.stat(sourcePath);
  if (!stat.isDirectory()) {
    if (!isSupportedFileSourcePath(sourcePath)) {
      throw new Error(`unsupported FILE_SOURCE_PATH extension for ${sourcePath}`);
    }
    return [sourcePath];
  }

  const entries = await fs.readdir(sourcePath, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile())
    .map((entry) => path.join(sourcePath, entry.name))
    .filter((entryPath) => isSupportedFileSourcePath(entryPath))
    .sort();
}

function isSupportedFileSourcePath(entryPath: string): boolean {
  const ext = path.extname(entryPath).toLowerCase();
  return ext === ".yaml" || ext === ".yml";
}

async function parseFileServiceDefinitions(filePath: string): Promise<Array<{ key: string; svc: FileServiceDefinition }>> {
  const raw = await fs.readFile(filePath, "utf-8");
  if (!raw.trim()) return [];

  const doc = parseYaml(raw) as { services?: Record<string, FileServiceDefinition> };
  if (!doc || typeof doc !== "object" || !doc.services || typeof doc.services !== "object") {
    throw new Error(`expected YAML object with \"services\" keyed map in ${filePath}`);
  }

  return Object.keys(doc.services)
    .sort()
    .map((key) => ({ key, svc: doc.services?.[key] ?? {} }));
}

async function loadFileModeDesiredServices(cfg: Config): Promise<{ desired: Map<string, ConsulServiceRegistration>; configured: number }> {
  const desired = new Map<string, ConsulServiceRegistration>();
  let configured = 0;

  const files = await collectFileSourcePaths(cfg.fileSourcePath);
  for (const filePath of files) {
    const parsed = await parseFileServiceDefinitions(filePath);
    for (const { key, svc } of parsed) {
      const address = svc.address?.trim() ?? "";
      if (!address) throw new Error(`${filePath}:${key} address is required`);
      if (!Number.isInteger(svc.port) || (svc.port ?? 0) <= 0) {
        throw new Error(`${filePath}:${key} port must be > 0`);
      }

      const normalizedKey = normalizeFileServiceKey(key);
      const id = (svc.id?.trim() ?? "") || (normalizedKey ? `${cfg.serviceIDPrefix}${normalizedKey}` : deriveFileServiceID(cfg, filePath, key, svc));
      const name = (svc.name?.trim() ?? "") || key.trim() || cfg.defaultServiceName;

      const meta: Record<string, string> = {
        [managedByMetaKey]: managedByMetaValue,
        [ownerIDMetaKey]: cfg.ownerID,
        source: sourceModeFile,
        "source-file": path.basename(filePath),
      };
      for (const [metaKey, value] of Object.entries(svc.meta ?? {})) {
        if (metaKey.trim()) {
          meta[metaKey.trim()] = String(value);
        }
      }

      const tags = (svc.tags ?? [])
        .map((entry) => entry.trim())
        .filter((entry) => entry !== "")
        .sort();

      if (desired.has(id)) {
        throw new Error(`${filePath}:${key} duplicate service id ${id}`);
      }

      desired.set(id, {
        ID: id,
        Name: name,
        Address: address,
        Port: svc.port,
        Tags: tags,
        Meta: meta,
      });
      configured += 1;
    }
  }

  return { desired, configured };
}

async function computeFileSourceSignature(sourcePath: string): Promise<string> {
  const parts: string[] = [];

  try {
    const stat = await fs.stat(sourcePath);
    if (!stat.isDirectory()) {
      const fileStat = await fs.stat(sourcePath);
      return `${sourcePath}:${fileStat.size}:${fileStat.mtimeMs}`;
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return "missing";
    }
    throw error;
  }

  const files = await collectFileSourcePaths(sourcePath);
  for (const filePath of files) {
    const fileStat = await fs.stat(filePath);
    parts.push(`${filePath}:${fileStat.size}:${fileStat.mtimeMs}`);
  }

  return parts.join("|");
}

async function applyDesiredState(
  cfg: Config,
  managed: Set<string>,
  desired: Map<string, ConsulServiceRegistration>,
  sourceSummary: string,
): Promise<void> {
  for (const reg of desired.values()) {
    try {
      await registerService(cfg, reg);
      managed.add(reg.ID);
    } catch (error) {
      console.log(`register ${reg.ID} failed: ${(error as Error).message}`);
    }
  }

  for (const id of [...managed]) {
    if (desired.has(id)) continue;
    try {
      await deregisterService(cfg, id);
      managed.delete(id);
    } catch (error) {
      console.log(`deregister ${id} failed: ${(error as Error).message}`);
    }
  }

  let recovered = 0;
  try {
    const services = await listAgentServices(cfg);
    for (const [id, svc] of Object.entries(services)) {
      if (desired.has(id)) continue;
      const meta = svc.Meta ?? {};
      if (meta[managedByMetaKey] !== managedByMetaValue) continue;
      if ((meta.kind ?? "").trim() === "owner-heartbeat") continue;
      if ((meta[ownerIDMetaKey] ?? "").trim() !== cfg.ownerID) continue;

      try {
        await deregisterService(cfg, id);
        managed.delete(id);
        recovered += 1;
      } catch (error) {
        console.log(`reconcile stale ${id} failed: ${(error as Error).message}`);
      }
    }
  } catch (error) {
    console.log(`owner reconciliation skipped: ${(error as Error).message}`);
  }

  console.log(`sync complete: ${sourceSummary} registered_services=${desired.size} recovered_stale_services=${recovered}`);
}

async function syncDockerModeOnce(cfg: Config, managed: Set<string>): Promise<void> {
  const containers = await listRunningContainers(cfg);
  const desired = new Map<string, ConsulServiceRegistration>();

  for (const container of containers) {
    const reg = buildDockerRegistration(cfg, container);
    if (reg) desired.set(reg.ID, reg);
  }

  await applyDesiredState(cfg, managed, desired, `running_containers=${containers.length}`);
}

async function syncFileModeOnce(cfg: Config, managed: Set<string>): Promise<void> {
  const loaded = await loadFileModeDesiredServices(cfg);
  await applyDesiredState(
    cfg,
    managed,
    loaded.desired,
    `configured_services=${loaded.configured} source_path=${cfg.fileSourcePath}`,
  );
}

async function listOwnerChecks(cfg: Config): Promise<OwnerCheck[]> {
  const response = await consulRequest(cfg, "GET", `/v1/health/checks/${encodeURIComponent(ownerHeartbeatService)}`);
  const parsed = (await response.json()) as OwnerCheck[];
  return Array.isArray(parsed) ? parsed : [];
}

async function listCatalogServices(cfg: Config): Promise<Record<string, string[]>> {
  const response = await consulRequest(cfg, "GET", "/v1/catalog/services");
  return ((await response.json()) as Record<string, string[]>) ?? {};
}

async function listCatalogServiceInstances(cfg: Config, serviceName: string): Promise<CatalogInstance[]> {
  const response = await consulRequest(cfg, "GET", `/v1/catalog/service/${encodeURIComponent(serviceName)}`);
  const parsed = (await response.json()) as CatalogInstance[];
  return Array.isArray(parsed) ? parsed : [];
}

async function deregisterCatalogService(cfg: Config, node: string, serviceID: string): Promise<void> {
  await consulRequest(cfg, "PUT", "/v1/catalog/deregister", {
    Node: node,
    ServiceID: serviceID,
  });
}

function ownerStatusByNode(checks: OwnerCheck[]): Map<string, Map<string, string>> {
  const out = new Map<string, Map<string, string>>();

  for (const check of checks) {
    const serviceID = (check.ServiceID ?? "").trim();
    const node = (check.Node ?? "").trim();
    if (!node || !serviceID.startsWith(ownerHeartbeatIDPrefix)) continue;

    const owner = serviceID.slice(ownerHeartbeatIDPrefix.length).trim();
    if (!owner) continue;

    if (!out.has(owner)) out.set(owner, new Map<string, string>());
    out.get(owner)?.set(node, (check.Status ?? "").trim());
  }

  return out;
}

async function sweepStaleServices(cfg: Config, gcSeen: Map<string, number>): Promise<void> {
  const checks = await listOwnerChecks(cfg);
  const ownerByNode = ownerStatusByNode(checks);

  const catalogServices = await listCatalogServices(cfg);
  const now = Date.now();
  let removed = 0;

  const candidates: Array<{ key: string; node: string; serviceID: string; reason: string; graceMs: number }> = [];

  for (const serviceName of Object.keys(catalogServices).sort()) {
    const instances = await listCatalogServiceInstances(cfg, serviceName);

    for (const instance of instances) {
      const meta = instance.ServiceMeta ?? {};
      if (meta[managedByMetaKey] !== managedByMetaValue) continue;
      if ((meta.kind ?? "").trim() === "owner-heartbeat") continue;

      const node = (instance.Node ?? "").trim();
      const serviceID = (instance.ServiceID ?? "").trim();
      if (!node || !serviceID) continue;

      const owner = (meta[ownerIDMetaKey] ?? "").trim();
      if (!owner) {
        candidates.push({
          key: `${node}/${serviceID}`,
          node,
          serviceID,
          reason: "missing owner-id",
          graceMs: cfg.orphanGraceMs,
        });
        continue;
      }

      const statusOnNode = ownerByNode.get(owner)?.get(node) ?? "";
      if (statusOnNode.toLowerCase() === "passing") {
        gcSeen.delete(`${node}/${serviceID}`);
        continue;
      }

      candidates.push({
        key: `${node}/${serviceID}`,
        node,
        serviceID,
        reason: statusOnNode ? "owner heartbeat missing on node" : "owner heartbeat missing",
        graceMs: cfg.ownerDownGraceMs,
      });
    }
  }

  for (const candidate of candidates) {
    const firstSeen = gcSeen.get(candidate.key) ?? now;
    gcSeen.set(candidate.key, firstSeen);

    if (now-firstSeen < candidate.graceMs) continue;

    try {
      await deregisterCatalogService(cfg, candidate.node, candidate.serviceID);
      gcSeen.delete(candidate.key);
      removed += 1;
      console.log(
        `gc deregistered stale service service_id=${candidate.serviceID} node=${candidate.node} reason=${candidate.reason}`,
      );
    } catch (error) {
      console.log(
        `gc deregister failed service_id=${candidate.serviceID} node=${candidate.node} reason=${candidate.reason} err=${(error as Error).message}`,
      );
    }
  }

  console.log(`gc sweep complete: alive_owners=${ownerByNode.size} removed=${removed}`);
}

async function run(): Promise<void> {
  const cfg = loadConfig();
  await validateConfig(cfg);

  console.log(
    `starting event loop: source_mode=${cfg.sourceMode} consul=${cfg.consulHTTPAddr} resync_interval=${durationToConsulString(cfg.resyncIntervalMs)} gc_interval=${durationToConsulString(cfg.gcIntervalMs)} require_traefik_enable=${cfg.requireTraefikEnable} owner_id=${cfg.ownerID}`,
  );

  if (cfg.sourceMode === sourceModeDocker) {
    console.log(`docker mode configured: docker_socket=${cfg.dockerSocket}`);
  } else {
    console.log(`file mode configured: file_source_path=${cfg.fileSourcePath}`);
  }

  const managed = new Set<string>();
  const gcSeen = new Map<string, number>();

  const doSync = async (): Promise<void> => {
    if (cfg.sourceMode === sourceModeDocker) {
      await syncDockerModeOnce(cfg, managed);
    } else {
      await syncFileModeOnce(cfg, managed);
    }
  };

  try {
    await doSync();
  } catch (error) {
    console.log(`initial sync failed: ${(error as Error).message}`);
  }

  const ownerServiceID = `${ownerHeartbeatIDPrefix}${cfg.ownerID}`;
  try {
    await registerOwnerHeartbeatService(cfg, ownerServiceID);
  } catch (error) {
    console.log(`owner heartbeat registration failed: ${(error as Error).message}`);
  }
  try {
    await passOwnerHeartbeat(cfg, ownerServiceID);
  } catch (error) {
    console.log(`initial owner heartbeat pass failed: ${(error as Error).message}`);
  }

  let stopped = false;
  const stop = (): void => {
    stopped = true;
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);

  let fileWatchTrigger = 0;
  let closeWatcher: (() => void) | null = null;

  if (cfg.sourceMode === sourceModeFile) {
    const watchTargets = new Set<string>();
    watchTargets.add(path.dirname(cfg.fileSourcePath));
    try {
      const stat = await fs.stat(cfg.fileSourcePath);
      if (stat.isDirectory()) {
        watchTargets.add(cfg.fileSourcePath);
      }
    } catch {
      // ignore ENOENT and rely on parent dir watch.
    }

    const watchers = [...watchTargets].map((target) =>
      watch(target, { persistent: true }, (_eventType, filename) => {
        const fullPath = filename ? path.join(target, filename.toString()) : target;
        if (isFileSourceEventRelevant(cfg.fileSourcePath, fullPath)) {
          fileWatchTrigger = Date.now();
        }
      }),
    );

    closeWatcher = () => {
      for (const watcher of watchers) watcher.close();
    };
  }

  let lastResync = Date.now();
  let lastOwnerPass = Date.now();
  let lastGC = Date.now();
  let lastFileSync = 0;
  let lastFileProbe = 0;
  let lastFileSignature = cfg.sourceMode === sourceModeFile ? await computeFileSourceSignature(cfg.fileSourcePath) : "";

  while (!stopped) {
    const now = Date.now();

    if (cfg.sourceMode === sourceModeFile && now - lastFileProbe >= 1000) {
      try {
        const signature = await computeFileSourceSignature(cfg.fileSourcePath);
        if (signature !== lastFileSignature) {
          fileWatchTrigger = Date.now();
          lastFileSignature = signature;
        }
      } catch (error) {
        console.log(`file source probe failed: ${(error as Error).message}`);
      }
      lastFileProbe = Date.now();
    }

    if (cfg.sourceMode === sourceModeFile && fileWatchTrigger > lastFileSync) {
      try {
        await doSync();
      } catch (error) {
        console.log(`sync after file change failed: ${(error as Error).message}`);
      }
      lastFileSync = Date.now();
      lastResync = Date.now();
      try {
        lastFileSignature = await computeFileSourceSignature(cfg.fileSourcePath);
      } catch {
        // Ignore transient read errors after sync; next probe will retry.
      }
    }

    if (now - lastResync >= cfg.resyncIntervalMs) {
      try {
        await doSync();
      } catch (error) {
        console.log(`periodic resync failed: ${(error as Error).message}`);
      }
      lastResync = Date.now();
    }

    if (now - lastOwnerPass >= cfg.ownerHeartbeatPassMs) {
      try {
        await passOwnerHeartbeat(cfg, ownerServiceID);
      } catch (error) {
        console.log(`owner heartbeat pass failed: ${(error as Error).message}`);
      }
      lastOwnerPass = Date.now();
    }

    if (now - lastGC >= cfg.gcIntervalMs) {
      try {
        await sweepStaleServices(cfg, gcSeen);
      } catch (error) {
        console.log(`gc sweep failed: ${(error as Error).message}`);
      }
      lastGC = Date.now();
    }

    await sleep(200);
  }

  closeWatcher?.();
  console.log("shutdown signal received; leaving current Consul registrations unchanged");
}

function isFileSourceEventRelevant(sourcePath: string, eventPath: string): boolean {
  const cleanSource = path.resolve(sourcePath);
  const cleanEvent = path.resolve(eventPath);
  if (cleanSource === cleanEvent) return true;

  if (isSupportedFileSourcePath(cleanSource) && cleanEvent === cleanSource) {
    return true;
  }

  if (!isSupportedFileSourcePath(cleanSource)) {
    return path.dirname(cleanEvent) === cleanSource && isSupportedFileSourcePath(cleanEvent);
  }

  return false;
}

run().catch((error) => {
  console.error(`fatal: ${(error as Error).message}`);
  process.exit(1);
});
