import { watch } from "node:fs";
import { promises as fs } from "node:fs";
import http from "node:http";
import { isIP } from "node:net";
import path from "node:path";
import process from "node:process";
import { parse as parseYaml } from "yaml";

type SourceMode = "docker" | "file";

type Config = {
  consulHTTPAddr: string;
  consulOverlayHTTPPort: string;
  consulToken: string;
  sourceMode: SourceMode;
  dockerSocket: string;
  fileSourcePath: string;
  requireTraefikEnable: boolean;
  ownerID: string;
  localNodeName: string;
  verboseCatalogCleanup: boolean;
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

type DockerEvent = {
  Type?: string;
  Action?: string;
  status?: string;
  id?: string;
};

type ConsulServiceRegistration = {
  ID: string;
  Name: string;
  Address?: string;
  Port?: number;
  Tags?: string[];
  Meta?: Record<string, string>;
};

type AgentService = { ID?: string; Meta?: Record<string, string> };
type CatalogInstance = {
  Node?: string;
  Address?: string;
  ServiceID?: string;
  ServiceMeta?: Record<string, string>;
};
type ConsulAgentSelfResponse = {
  Config?: { NodeName?: string };
  Member?: { Addr?: string };
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

function isValidPort(raw: string): boolean {
  if (!/^\d+$/.test(raw.trim())) return false;
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed > 0 && parsed <= 65535;
}

function loadConfig(): Config {
  const ownerFromHost = process.env.HOSTNAME?.trim() ?? "";
  return {
    consulHTTPAddr: env("CONSUL_HTTP_ADDR", "http://127.0.0.1:8500"),
    consulOverlayHTTPPort: env("CONSUL_OVERLAY_HTTP_PORT", ""),
    consulToken: env("CONSUL_HTTP_TOKEN", ""),
    sourceMode: env("SOURCE_MODE", "docker") === "file" ? sourceModeFile : sourceModeDocker,
    dockerSocket: env("DOCKER_SOCKET", "/var/run/docker.sock"),
    fileSourcePath: env("FILE_SOURCE_PATH", "/etc/traefik-registrator/services.d"),
    requireTraefikEnable: parseBool(env("REQUIRE_TRAEFIK_ENABLE", "true")),
    ownerID: env("OWNER_ID", ownerFromHost),
    localNodeName: ownerFromHost,
    verboseCatalogCleanup: parseBool(env("LOG_VERBOSE_CATALOG_CLEANUP", "false")),
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

  if (cfg.consulOverlayHTTPPort && !isValidPort(cfg.consulOverlayHTTPPort)) {
    throw new Error("CONSUL_OVERLAY_HTTP_PORT must be a valid TCP port");
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

function dockerEventStreamPath(): string {
  const filters = encodeURIComponent(JSON.stringify({ type: ["container"] }));
  return `/events?filters=${filters}`;
}

function dockerEventAction(event: DockerEvent): string {
  return (event.Action ?? event.status ?? "").trim().toLowerCase();
}

function isDockerEventRelevant(event: DockerEvent): boolean {
  if ((event.Type ?? "").trim() !== "container") return false;
  const action = dockerEventAction(event);
  return ["start", "stop", "die", "kill", "destroy", "unpause", "pause"].includes(action);
}

async function watchDockerEvents(
  cfg: Config,
  onRelevantEvent: (event: DockerEvent) => void,
  onStreamConnected: () => void,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    try {
      await streamDockerEventsOnce(cfg, onRelevantEvent, onStreamConnected, signal);
    } catch (error) {
      if (signal.aborted) break;
      console.log(`docker event stream error: ${(error as Error).message}`);
      await sleep(1000);
      continue;
    }
    if (!signal.aborted) await sleep(250);
  }
}

function streamDockerEventsOnce(
  cfg: Config,
  onRelevantEvent: (event: DockerEvent) => void,
  onStreamConnected: () => void,
  signal: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (err?: Error): void => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", onAbort);
      if (err) reject(err);
      else resolve();
    };

    const onAbort = (): void => {
      req.destroy();
      finish();
    };

    const req = http.request(
      {
        socketPath: cfg.dockerSocket,
        path: dockerEventStreamPath(),
        method: "GET",
        headers: { Host: "docker" },
      },
      (res) => {
        const status = res.statusCode ?? 0;
        if (status < 200 || status >= 300) {
          finish(new Error(`docker API GET /events returned ${status}`));
          res.resume();
          return;
        }
        onStreamConnected();

        let buffer = "";
        res.setEncoding("utf-8");

        res.on("data", (chunk: string) => {
          if (signal.aborted) return;

          buffer += chunk;
          for (;;) {
            const idx = buffer.indexOf("\n");
            if (idx < 0) break;

            const line = buffer.slice(0, idx).trim();
            buffer = buffer.slice(idx + 1);
            if (!line) continue;

            try {
              const event = JSON.parse(line) as DockerEvent;
              if (isDockerEventRelevant(event)) {
                onRelevantEvent(event);
              }
            } catch {
              // Ignore malformed event payloads and keep the stream open.
            }
          }
        });

        res.on("end", () => finish());
        res.on("error", (error) => finish(error as Error));

        if (signal.aborted) {
          req.destroy();
          finish();
        }
      },
    );

    signal.addEventListener("abort", onAbort, { once: true });
    req.on("error", (error) => finish(error as Error));
    req.end();
  });
}

async function consulRequest(cfg: Config, method: string, requestPath: string, body?: unknown): Promise<Response> {
  return consulRequestAt(cfg.consulHTTPAddr, cfg.consulToken, method, requestPath, body);
}

async function consulRequestAt(
  baseAddr: string,
  consulToken: string,
  method: string,
  requestPath: string,
  body?: unknown,
  timeoutMs = 0,
): Promise<Response> {
  const url = `${baseAddr.replace(/\/$/, "")}${requestPath}`;
  const headers: Record<string, string> = body === undefined ? {} : { "Content-Type": "application/json" };
  if (consulToken) headers["X-Consul-Token"] = consulToken;

  const controller = timeoutMs > 0 ? new AbortController() : undefined;
  const timeoutHandle =
    timeoutMs > 0
      ? setTimeout(() => {
          controller?.abort();
        }, timeoutMs)
      : undefined;

  let response: Response;
  try {
    response = await fetch(url, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller?.signal,
    });
  } catch (error) {
    if (controller?.signal.aborted) {
      throw new Error(`consul API ${method} ${requestPath} timed out after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    if (timeoutHandle) clearTimeout(timeoutHandle);
  }

  if (!response.ok) {
    const text = await response.text();
    const error = new Error(`consul API ${method} ${requestPath} returned ${response.status}: ${text}`);
    (error as Error & { status?: number }).status = response.status;
    throw error;
  }
  return response;
}

function trimTrailingSlash(raw: string): string {
  return raw.replace(/\/$/, "");
}

function defaultPortForProtocol(protocol: string): string {
  return protocol === "https:" ? "443" : "80";
}

function defaultConsulAgentPortForProtocol(protocol: string): string {
  return protocol === "https:" ? "8501" : "8500";
}

function isDirectConsulHost(rawHost: string): boolean {
  const host = rawHost.trim().toLowerCase();
  if (!host) return false;
  if (host === "localhost" || host === "127.0.0.1" || host === "::1") return true;
  return isIP(host) !== 0;
}

function formatHostForURL(raw: string): string {
  if (raw.includes(":") && !raw.startsWith("[") && !raw.endsWith("]")) {
    return `[${raw}]`;
  }
  return raw;
}

function consulPeerBaseAddrsFromConfig(cfg: Config, nodeAddress: string): string[] {
  const parsed = new URL(cfg.consulHTTPAddr);
  const host = formatHostForURL(nodeAddress);
  const configuredPort = parsed.port || defaultPortForProtocol(parsed.protocol);
  const overlayPort = cfg.consulOverlayHTTPPort.trim();
  const nativeAgentPort = defaultConsulAgentPortForProtocol(parsed.protocol);
  const ports = [overlayPort, configuredPort, nativeAgentPort].filter((entry) => entry !== "");

  const uniquePorts: string[] = [];
  for (const port of ports) {
    if (!uniquePorts.includes(port)) {
      uniquePorts.push(port);
    }
  }

  return uniquePorts.map((port) => `${parsed.protocol}//${host}:${port}`);
}

async function consulAgentSelf(cfg: Config): Promise<ConsulAgentSelfResponse> {
  const response = await consulRequest(cfg, "GET", "/v1/agent/self");
  return (await response.json()) as ConsulAgentSelfResponse;
}

async function pinConsulAddrToSingleAgent(cfg: Config): Promise<void> {
  let parsed: URL;
  try {
    parsed = new URL(cfg.consulHTTPAddr);
  } catch {
    return;
  }

  if (parsed.protocol !== "http:") return;
  if (isDirectConsulHost(parsed.hostname)) return;
  if (parsed.pathname !== "" && parsed.pathname !== "/") return;

  const self = await consulAgentSelf(cfg);
  const memberAddr = (self.Member?.Addr ?? "").trim();
  if (!memberAddr) return;

  const current = trimTrailingSlash(cfg.consulHTTPAddr);
  const candidates = consulPeerBaseAddrsFromConfig(cfg, memberAddr).filter((baseAddr) => baseAddr !== current);

  if (candidates.length === 0) return;

  for (const candidate of candidates) {
    try {
      await consulRequestAt(candidate, cfg.consulToken, "GET", "/v1/status/leader", undefined, 2500);
      cfg.consulHTTPAddr = candidate;
      console.log(`consul endpoint pinned: ${cfg.consulHTTPAddr}`);
      return;
    } catch (error) {
      console.log(`consul endpoint pin skipped for ${candidate}: ${(error as Error).message}`);
    }
  }
}

async function consulLeader(cfg: Config): Promise<string> {
  const response = await consulRequest(cfg, "GET", "/v1/status/leader");
  const raw = (await response.text()).trim();
  return raw.replace(/^"/, "").replace(/"$/, "").trim();
}

async function consulLocalNodeName(cfg: Config): Promise<string> {
  const parsed = await consulAgentSelf(cfg);
  return (parsed.Config?.NodeName ?? "").trim();
}

async function waitForConsulReady(cfg: Config, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError = "consul leader not available";

  while (Date.now() < deadline) {
    try {
      const leader = await consulLeader(cfg);
      if (leader) return;
      lastError = "consul leader is empty";
    } catch (error) {
      lastError = (error as Error).message;
    }

    await sleep(1000);
  }

  throw new Error(`consul not ready after ${timeoutMs}ms: ${lastError}`);
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

function isRequestTimeoutError(raw: string): boolean {
  const message = raw.trim().toLowerCase();
  return message.includes("timed out");
}

async function deregisterNodeAgentService(
  cfg: Config,
  serviceName: string,
  nodeName: string,
  nodeAddress: string,
  serviceID: string,
): Promise<boolean> {
  const address = nodeAddress.trim();
  if (!address) return false;

  const baseAddrs = consulPeerBaseAddrsFromConfig(cfg, address);
  const reqPath = `/v1/agent/service/deregister/${encodeURIComponent(serviceID)}`;
  let lastError = "";

  for (const [index, baseAddr] of baseAddrs.entries()) {
    try {
      await consulRequestAt(baseAddr, cfg.consulToken, "PUT", reqPath, undefined, 2500);
      if (index > 0) {
        console.log(
          `node-agent cleanup fallback succeeded: service=${serviceName} service_id=${serviceID} node=${nodeName || "-"} node_addr=${address} endpoint=${baseAddr}`,
        );
      }
      return true;
    } catch (error) {
      lastError = (error as Error).message;
      if (isRequestTimeoutError(lastError)) {
        console.log(
          `node-agent unregister timeout: service=${serviceName} service_id=${serviceID} node=${nodeName || "-"} node_addr=${address} endpoint=${baseAddr} attempt=${index + 1}/${baseAddrs.length} error=${lastError}`,
        );
      }
    }
  }

  console.log(
    `node-agent cleanup skipped: service=${serviceName} service_id=${serviceID} node=${nodeName || "-"} node_addr=${address} endpoints=${baseAddrs.join(",")} error=${lastError}`,
  );
  return false;
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
  return Object.entries(labels)
    .filter(([key]) => key.startsWith("traefik."))
    .map(([key, value]) => `${key}=${value}`)
    .sort();
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
      source: sourceModeDocker,
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
      const id =
        (svc.id?.trim() ?? "") ||
        (normalizedKey ? `${cfg.serviceIDPrefix}${normalizedKey}` : deriveFileServiceID(cfg, filePath, key, svc));
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

function isOwnedManagedService(meta: Record<string, string> | undefined, ownerID: string): boolean {
  if (!meta) return false;
  if ((meta[managedByMetaKey] ?? "").trim() !== managedByMetaValue) return false;
  return (meta[ownerIDMetaKey] ?? "").trim() === ownerID;
}

type OwnedCatalogCleanupResult = {
  removedTotal: number;
  removedNotDesired: number;
  removedDuplicates: number;
  removedNodeAgentServices: number;
};

function prioritizeLocalNode(
  instances: CatalogInstance[],
  localNodeName: string,
): CatalogInstance[] {
  if (!localNodeName) return instances;
  return [...instances].sort((left, right) => {
    const leftLocal = (left.Node ?? "").trim() === localNodeName;
    const rightLocal = (right.Node ?? "").trim() === localNodeName;
    if (leftLocal === rightLocal) return 0;
    return leftLocal ? -1 : 1;
  });
}

async function cleanupOwnedCatalogServices(
  cfg: Config,
  desiredIDs?: Set<string>,
): Promise<OwnedCatalogCleanupResult> {
  let removedTotal = 0;
  let removedNotDesired = 0;
  let removedDuplicates = 0;
  let removedNodeAgentServices = 0;
  const keptDesired = new Set<string>();
  const catalogServices = await listCatalogServices(cfg);

  for (const serviceName of Object.keys(catalogServices).sort()) {
    const rawInstances = await listCatalogServiceInstances(cfg, serviceName);
    const instances = prioritizeLocalNode(rawInstances, cfg.localNodeName);

    for (const instance of instances) {
      if (!isOwnedManagedService(instance.ServiceMeta, cfg.ownerID)) continue;

      const node = (instance.Node ?? "").trim();
      const serviceID = (instance.ServiceID ?? "").trim();
      if (!node || !serviceID) continue;

      let reason: "not-desired" | "duplicate" | null = null;
      if (desiredIDs) {
        if (!desiredIDs.has(serviceID)) {
          reason = "not-desired";
        } else if (keptDesired.has(serviceID)) {
          reason = "duplicate";
        } else {
          keptDesired.add(serviceID);
        }
      } else {
        reason = "not-desired";
      }
      if (!reason) continue;

      const nodeAddress = (instance.Address ?? "").trim();
      const owner = (instance.ServiceMeta?.[ownerIDMetaKey] ?? "").trim();
      const source = (instance.ServiceMeta?.source ?? "").trim();
      if (cfg.verboseCatalogCleanup) {
        console.log(
          `catalog cleanup candidate: service=${serviceName} service_id=${serviceID} node=${node} node_addr=${nodeAddress || "-"} owner_id=${owner || "-"} source=${source || "-"} reason=${reason}`,
        );
      }

      try {
        await deregisterCatalogService(cfg, node, serviceID);
        removedTotal += 1;
        if (reason === "not-desired") removedNotDesired += 1;
        else removedDuplicates += 1;
        if (cfg.verboseCatalogCleanup) {
          console.log(
            `catalog cleanup deregistered catalog instance: service=${serviceName} service_id=${serviceID} node=${node} reason=${reason}`,
          );
        }
        if (await deregisterNodeAgentService(cfg, serviceName, node, nodeAddress, serviceID)) {
          removedNodeAgentServices += 1;
        } else {
          console.log(
            `catalog cleanup warning: service=${serviceName} service_id=${serviceID} node=${node} node_addr=${nodeAddress || "-"} removed_from_catalog=true removed_from_node_agent=false duplicate_may_reappear=true`,
          );
        }
      } catch (error) {
        console.log(`catalog cleanup deregister failed for ${node}/${serviceID}: ${(error as Error).message}`);
      }
    }
  }

  return { removedTotal, removedNotDesired, removedDuplicates, removedNodeAgentServices };
}

async function cleanupOwnedServicesOnStartup(cfg: Config): Promise<void> {
  let removedAgent = 0;
  let removedCatalog = 0;
  let removedNodeAgentServices = 0;

  try {
    const services = await listAgentServices(cfg);
    for (const [id, svc] of Object.entries(services)) {
      if (!isOwnedManagedService(svc.Meta, cfg.ownerID)) continue;
      try {
        await deregisterService(cfg, id);
        removedAgent += 1;
      } catch (error) {
        console.log(`startup cleanup agent deregister failed for ${id}: ${(error as Error).message}`);
      }
    }
  } catch (error) {
    console.log(`startup cleanup agent scan skipped: ${(error as Error).message}`);
  }

  try {
    const result = await cleanupOwnedCatalogServices(cfg);
    removedCatalog = result.removedTotal;
    removedNodeAgentServices = result.removedNodeAgentServices;
  } catch (error) {
    console.log(`startup cleanup catalog scan skipped: ${(error as Error).message}`);
  }

  console.log(
    `startup cleanup complete: owner_id=${cfg.ownerID} removed_agent=${removedAgent} removed_catalog=${removedCatalog} removed_node_agent=${removedNodeAgentServices}`,
  );
}

async function pruneUnexpectedOwnedAgentServices(
  cfg: Config,
  desired: Map<string, ConsulServiceRegistration>,
  managed: Set<string>,
): Promise<number> {
  let removed = 0;
  const services = await listAgentServices(cfg);

  for (const [id, svc] of Object.entries(services)) {
    if (desired.has(id)) continue;
    if (!isOwnedManagedService(svc.Meta, cfg.ownerID)) continue;

    try {
      await deregisterService(cfg, id);
      managed.delete(id);
      removed += 1;
    } catch (error) {
      console.log(`deregister stale ${id} failed: ${(error as Error).message}`);
    }
  }

  return removed;
}

async function applyDesiredState(
  cfg: Config,
  managed: Set<string>,
  desired: Map<string, ConsulServiceRegistration>,
  sourceSummary: string,
): Promise<void> {
  let upserted = 0;

  for (const reg of desired.values()) {
    try {
      await registerService(cfg, reg);
      managed.add(reg.ID);
      upserted += 1;
    } catch (error) {
      console.log(`register ${reg.ID} failed: ${(error as Error).message}`);
    }
  }

  let removedNoLongerDesired = 0;
  let agentServices: Record<string, AgentService> = {};
  try {
    agentServices = await listAgentServices(cfg);
  } catch (error) {
    console.log(`agent service snapshot for managed cleanup failed: ${(error as Error).message}`);
  }

  for (const id of [...managed]) {
    if (desired.has(id)) continue;
    if (!isOwnedManagedService(agentServices[id]?.Meta, cfg.ownerID)) {
      managed.delete(id);
      continue;
    }

    try {
      await deregisterService(cfg, id);
      managed.delete(id);
      removedNoLongerDesired += 1;
    } catch (error) {
      console.log(`deregister ${id} failed: ${(error as Error).message}`);
    }
  }

  let removedUnexpected = 0;
  try {
    removedUnexpected = await pruneUnexpectedOwnedAgentServices(cfg, desired, managed);
  } catch (error) {
    console.log(`owned-service prune skipped: ${(error as Error).message}`);
  }

  let removedCatalogNotDesired = 0;
  let removedCatalogDuplicates = 0;
  let removedCatalogNodeAgentServices = 0;
  try {
    const catalogCleanup = await cleanupOwnedCatalogServices(cfg, new Set(desired.keys()));
    removedCatalogNotDesired = catalogCleanup.removedNotDesired;
    removedCatalogDuplicates = catalogCleanup.removedDuplicates;
    removedCatalogNodeAgentServices = catalogCleanup.removedNodeAgentServices;
  } catch (error) {
    console.log(`catalog cleanup skipped: ${(error as Error).message}`);
  }

  console.log(
    `sync complete: ${sourceSummary} desired=${desired.size} upserted=${upserted} removed_not_desired=${removedNoLongerDesired} removed_unexpected_owned=${removedUnexpected} removed_catalog_not_desired=${removedCatalogNotDesired} removed_catalog_duplicates=${removedCatalogDuplicates} removed_node_agent=${removedCatalogNodeAgentServices}`,
  );
}

async function syncDockerModeOnce(cfg: Config, managed: Set<string>): Promise<void> {
  const containers = await listRunningContainers(cfg);
  const desired = new Map<string, ConsulServiceRegistration>();

  for (const container of containers) {
    const reg = buildDockerRegistration(cfg, container);
    if (reg) desired.set(reg.ID, reg);
  }

  await applyDesiredState(cfg, managed, desired, `source=docker running_containers=${containers.length}`);
}

async function syncFileModeOnce(cfg: Config, managed: Set<string>): Promise<void> {
  const loaded = await loadFileModeDesiredServices(cfg);
  await applyDesiredState(
    cfg,
    managed,
    loaded.desired,
    `source=file configured_services=${loaded.configured} source_path=${cfg.fileSourcePath}`,
  );
}

function isFileSourceEventRelevant(sourcePath: string, eventPath: string): boolean {
  const cleanSource = path.resolve(sourcePath);
  const cleanEvent = path.resolve(eventPath);
  if (cleanSource === cleanEvent) return true;

  if (isSupportedFileSourcePath(cleanSource)) {
    return cleanEvent === cleanSource;
  }

  return path.dirname(cleanEvent) === cleanSource && isSupportedFileSourcePath(cleanEvent);
}

function createSyncRunner(syncFn: (reason: string) => Promise<void>): {
  runNow: (reason: string) => Promise<void>;
  trigger: (reason: string) => void;
} {
  let running = false;
  let pending = false;
  let pendingReason = "";

  const runInternal = async (reason: string): Promise<void> => {
    if (running) {
      pending = true;
      pendingReason = pendingReason ? `${pendingReason},${reason}` : reason;
      return;
    }

    running = true;
    let currentReason = reason;

    for (;;) {
      try {
        await syncFn(currentReason);
      } catch (error) {
        console.log(`sync failed (${currentReason}): ${(error as Error).message}`);
      }

      if (!pending) break;
      pending = false;
      currentReason = pendingReason || "queued-trigger";
      pendingReason = "";
    }

    running = false;
  };

  return {
    runNow: runInternal,
    trigger: (reason: string) => {
      void runInternal(reason);
    },
  };
}

async function setupFileWatchers(cfg: Config, onRelevantChange: (eventPath: string) => void): Promise<() => void> {
  const targets = new Set<string>([path.dirname(cfg.fileSourcePath)]);

  try {
    const stat = await fs.stat(cfg.fileSourcePath);
    if (stat.isDirectory()) {
      targets.add(cfg.fileSourcePath);
    }
  } catch {
    // Rely on parent directory watch if source path is missing or inaccessible.
  }

  const watchers: ReturnType<typeof watch>[] = [];
  for (const target of [...targets]) {
    try {
      const watcher = watch(target, { persistent: true }, (_eventType, filename) => {
        const fullPath = filename ? path.join(target, filename.toString()) : target;
        if (isFileSourceEventRelevant(cfg.fileSourcePath, fullPath)) {
          onRelevantChange(fullPath);
        }
      });
      watchers.push(watcher);
    } catch (error) {
      console.log(`file watch skipped for ${target}: ${(error as Error).message}`);
    }
  }

  return () => {
    for (const watcher of watchers) {
      watcher.close();
    }
  };
}

function installSignalHandler(controller: AbortController): void {
  const stop = (): void => controller.abort();
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
}

async function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return;
  await new Promise<void>((resolve) => {
    signal.addEventListener("abort", () => resolve(), { once: true });
  });
}

function scheduleStartupReconciliations(
  signal: AbortSignal,
  trigger: (reason: string) => void,
  delaysMs: number[],
): void {
  const timers: ReturnType<typeof setTimeout>[] = [];

  for (const delay of delaysMs) {
    timers.push(
      setTimeout(() => {
        if (!signal.aborted) trigger(`startup-reconcile:${delay}ms`);
      }, delay),
    );
  }

  signal.addEventListener(
    "abort",
    () => {
      for (const timer of timers) clearTimeout(timer);
    },
    { once: true },
  );
}

async function run(): Promise<void> {
  const cfg = loadConfig();
  await validateConfig(cfg);

  console.log(
    `starting: source_mode=${cfg.sourceMode} consul=${cfg.consulHTTPAddr} require_traefik_enable=${cfg.requireTraefikEnable} owner_id=${cfg.ownerID}`,
  );

  if (cfg.sourceMode === sourceModeDocker) {
    console.log(`docker mode configured: docker_socket=${cfg.dockerSocket}`);
  } else {
    console.log(`file mode configured: file_source_path=${cfg.fileSourcePath}`);
  }
  if (cfg.verboseCatalogCleanup) {
    console.log("verbose catalog cleanup logging enabled");
  }

  console.log("waiting for consul leader availability");
  await waitForConsulReady(cfg, 90_000);
  console.log("consul is ready");

  try {
    await pinConsulAddrToSingleAgent(cfg);
  } catch (error) {
    console.log(`consul endpoint pin skipped: ${(error as Error).message}`);
  }

  try {
    const detectedNodeName = await consulLocalNodeName(cfg);
    if (detectedNodeName) {
      cfg.localNodeName = detectedNodeName;
      console.log(`local consul node detected: ${cfg.localNodeName}`);
    }
  } catch (error) {
    console.log(`local consul node detection skipped: ${(error as Error).message}`);
  }

  const managed = new Set<string>();
  await cleanupOwnedServicesOnStartup(cfg);

  const doSync = async (_reason: string): Promise<void> => {
    if (cfg.sourceMode === sourceModeDocker) {
      await syncDockerModeOnce(cfg, managed);
    } else {
      await syncFileModeOnce(cfg, managed);
    }
  };

  const syncRunner = createSyncRunner(doSync);
  await syncRunner.runNow("startup");

  const controller = new AbortController();
  installSignalHandler(controller);

  scheduleStartupReconciliations(controller.signal, syncRunner.trigger, [3000, 10000, 30000]);

  if (cfg.sourceMode === sourceModeDocker) {
    await watchDockerEvents(
      cfg,
      (event) => {
        const action = dockerEventAction(event);
        const id = shortID((event.id ?? "").trim());
        syncRunner.trigger(`docker-event:${action}:${id}`);
      },
      () => {
        syncRunner.trigger("docker-events-connected");
      },
      controller.signal,
    );
  } else {
    const closeWatchers = await setupFileWatchers(cfg, (eventPath) => {
      syncRunner.trigger(`file-change:${eventPath}`);
    });
    await syncRunner.runNow("file-watchers-attached");

    await waitForAbort(controller.signal);
    closeWatchers();
  }

  console.log("shutdown signal received; leaving current Consul registrations unchanged");
}

run().catch((error) => {
  console.error(`fatal: ${(error as Error).message}`);
  process.exit(1);
});
