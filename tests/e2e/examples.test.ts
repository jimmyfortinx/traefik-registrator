import { afterAll, afterEach, beforeAll, beforeEach, describe, it } from "bun:test";
import { existsSync } from "node:fs";
import path from "node:path";

const repoRoot = process.cwd();
const examplesDir = path.join(repoRoot, "examples");
const scenarioTimeoutMs = 8 * 60_000;

function run(cmd: string[], cwd = repoRoot): string {
  const proc = Bun.spawnSync({
    cmd,
    cwd,
    stdout: "pipe",
    stderr: "pipe",
    env: process.env,
  });

  const stdout = new TextDecoder().decode(proc.stdout).trim();
  const stderr = new TextDecoder().decode(proc.stderr).trim();
  if (proc.exitCode !== 0) {
    throw new Error(
      [
        `command failed: ${cmd.join(" ")}`,
        stdout ? `stdout:\n${stdout}` : "",
        stderr ? `stderr:\n${stderr}` : "",
      ]
        .filter(Boolean)
        .join("\n\n"),
    );
  }

  return stdout;
}

function compose(project: string, composeFile: string, args: string[]): string {
  return run(["docker", "compose", "-p", project, "-f", composeFile, ...args]);
}

function composeDown(project: string, composeFile: string): void {
  try {
    compose(project, composeFile, ["down", "-v", "--remove-orphans"]);
  } catch {
    // Cleanup is best-effort.
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitUntil(
  description: string,
  timeoutMs: number,
  check: () => Promise<boolean | string>,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let last = "";

  while (Date.now() < deadline) {
    try {
      const result = await check();
      if (result === true) {
        return;
      }
      last = result;
    } catch (err) {
      last = err instanceof Error ? err.message : String(err);
    }
    await sleep(1000);
  }

  throw new Error(`timeout waiting for ${description}. last=${last}`);
}

async function consulJson(project: string, composeFile: string, pathAndQuery: string): Promise<any> {
  const body = compose(project, composeFile, [
    "exec",
    "-T",
    "consul",
    "sh",
    "-ec",
    `wget -qO- http://127.0.0.1:8500${pathAndQuery}`,
  ]);
  return JSON.parse(body);
}

function managedServices(agentServices: Record<string, any>): any[] {
  return Object.values(agentServices).filter((svc: any) => {
    const meta = svc.Meta ?? {};
    return meta["managed-by"] === "traefik-registrator" && meta.kind !== "owner-heartbeat";
  });
}

async function ownerStates(project: string, composeFile: string): Promise<Record<string, string>> {
  const checks = (await consulJson(project, composeFile, "/v1/health/checks/traefik-registrator-owner")) as any[];
  const states: Record<string, string> = {};

  for (const check of checks) {
    const serviceID = String(check.ServiceID ?? "");
    if (!serviceID.startsWith("traefik-registrator-owner-")) continue;
    const owner = serviceID.slice("traefik-registrator-owner-".length);
    if (owner) states[owner] = String(check.Status ?? "");
  }

  return states;
}

async function whoamiOwners(project: string, composeFile: string): Promise<Record<string, string[]>> {
  const instances = (await consulJson(project, composeFile, "/v1/catalog/service/whoami")) as any[];
  const out: Record<string, string[]> = {};

  for (const instance of instances) {
    const meta = instance.ServiceMeta ?? {};
    if (meta["managed-by"] !== "traefik-registrator") continue;
    const owner = String(meta["owner-id"] ?? "").trim();
    if (!owner) continue;

    const current = out[owner] ?? [];
    current.push(String(instance.ServiceID ?? ""));
    out[owner] = current;
  }

  return out;
}

function containerState(project: string, composeFile: string, service: string): { running: boolean; paused: boolean } {
  const containerID = compose(project, composeFile, ["ps", "-q", service]).trim();
  const raw = run(["docker", "inspect", "--format", "{{json .State}}", containerID]);
  const state = JSON.parse(raw);
  return { running: Boolean(state.Running), paused: Boolean(state.Paused) };
}

function scenarioProject(scenario: string): string {
  return `trreg-${scenario}`.replace(/[^a-z0-9-]/g, "-").slice(0, 40);
}

function scenarioComposeFile(scenario: string): string {
  return path.join(examplesDir, scenario, "docker-compose.yml");
}

async function assertBasicRegistration(project: string, composeFile: string): Promise<void> {
  await waitUntil("basic service registration", 90_000, async () => {
    const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
    const managed = managedServices(services);
    const ownerHeartbeat = Object.values(services).some((svc: any) => {
      const meta = svc.Meta ?? {};
      return meta["managed-by"] === "traefik-registrator" && meta.kind === "owner-heartbeat";
    });

    const whoami = managed.find((svc: any) => svc.Service === "whoami");
    if (!whoami || !ownerHeartbeat) return "missing whoami service or owner heartbeat";

    const tags = new Set(whoami.Tags ?? []);
    const expected = [
      "traefik.enable=true",
      "traefik.http.routers.whoami.rule=Host(`whoami.local`)",
      "traefik.http.services.whoami.loadbalancer.server.port=80",
    ];

    const ok =
      whoami.Port === 80 &&
      Boolean(whoami.Address) &&
      whoami.Meta?.["owner-id"] === "example-basic" &&
      expected.every((tag) => tags.has(tag));

    return ok || "whoami registration is not fully reconciled";
  });
}

async function assertCustomLabelOverrides(project: string, composeFile: string): Promise<void> {
  await waitUntil("custom label override registration", 90_000, async () => {
    const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
    const managed = managedServices(services);
    const svc = managed.find((item: any) => item.Service === "payments-api");
    if (!svc) return "payments-api service not present yet";

    const ok = svc.Address === "10.9.8.7" && svc.Port === 9090 && svc.Meta?.["owner-id"] === "example-custom";
    return ok || "payments-api has unexpected address/port/owner";
  });
}

async function assertGcStaleServices(project: string, composeFile: string): Promise<void> {
  await waitUntil("seeded stale services", 30_000, async () => {
    const orphan = (await consulJson(project, composeFile, "/v1/catalog/service/orphan-app")) as any[];
    const dead = (await consulJson(project, composeFile, "/v1/catalog/service/dead-owner-app")) as any[];
    const hasOrphan = orphan.some((item) => item.ServiceID === "seed-orphan");
    const hasDead = dead.some((item) => item.ServiceID === "seed-dead-owner");
    return hasOrphan && hasDead ? true : "seeded stale instances not visible yet";
  });

  await waitUntil("stale services garbage collection", 90_000, async () => {
    const orphan = (await consulJson(project, composeFile, "/v1/catalog/service/orphan-app")) as any[];
    const dead = (await consulJson(project, composeFile, "/v1/catalog/service/dead-owner-app")) as any[];
    const hasOrphan = orphan.some((item) => item.ServiceID === "seed-orphan");
    const hasDead = dead.some((item) => item.ServiceID === "seed-dead-owner");
    return !hasOrphan && !hasDead ? true : "stale services still present";
  });
}

async function assertHttpsOnlyFile(project: string, composeFile: string): Promise<void> {
  await waitUntil("https file-mode registration", 90_000, async () => {
    const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
    const managed = managedServices(services);
    const svc = managed.find((item: any) => item.Service === "app-https");
    if (!svc) return "app-https service not present yet";

    const tags = new Set(svc.Tags ?? []);
    const expected = [
      "traefik.enable=true",
      "traefik.http.routers.app-https.rule=Host(`app.example.com`)",
      "traefik.http.routers.app-https.tls=true",
      "traefik.http.services.app-https.loadbalancer.server.scheme=https",
      "traefik.http.services.app-https.loadbalancer.server.port=443",
    ];

    const ok =
      svc.Address === "app.internal" &&
      svc.Port === 443 &&
      svc.Meta?.["owner-id"] === "example-https-file" &&
      svc.Meta?.source === "file" &&
      expected.every((tag) => tags.has(tag));

    return ok || "app-https registration not reconciled";
  });
}

async function assertHybridDockerFile(project: string, composeFile: string): Promise<void> {
  await waitUntil("hybrid initial reconciliation", 90_000, async () => {
    const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
    const managed = managedServices(services);

    const dockerReady = managed.some(
      (svc: any) => svc.Service === "webdocker" && svc.Meta?.["owner-id"] === "hybrid-docker-owner",
    );

    const fileSvc = services["file-webfile"];
    if (!fileSvc) return "file-webfile service not present yet";

    const tags = new Set(fileSvc.Tags ?? []);
    const fileReady =
      fileSvc.Service === "webfile" &&
      fileSvc.Address === "webfile" &&
      fileSvc.Port === 80 &&
      fileSvc.Meta?.["owner-id"] === "hybrid-file-owner" &&
      tags.has("traefik.http.routers.webfile.rule=Host(`webfile.local`)");

    return dockerReady && fileReady ? true : "initial docker/file registration incomplete";
  });

  compose(project, composeFile, [
    "exec",
    "-T",
    "seed-file-config",
    "sh",
    "-ec",
    "sed -i 's/webfile.local/webfile-updated.local/g' /fixtures/services.d/routes.yml",
  ]);

  await waitUntil("file change propagation", 45_000, async () => {
    const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
    const managed = managedServices(services);
    const dockerStillPresent = managed.some(
      (svc: any) => svc.Service === "webdocker" && svc.Meta?.["owner-id"] === "hybrid-docker-owner",
    );
    const fileSvc = services["file-webfile"];
    if (!fileSvc || !dockerStillPresent) return "docker service or file service missing";

    const tags = new Set(fileSvc.Tags ?? []);
    const updated =
      tags.has("traefik.http.routers.webfile.rule=Host(`webfile-updated.local`)") &&
      !tags.has("traefik.http.routers.webfile.rule=Host(`webfile.local`)");

    return updated || "updated route tag not observed yet";
  });
}

async function assertOwnerFailoverGc(project: string, composeFile: string): Promise<void> {
  await waitUntil("both owners to register", 90_000, async () => {
    const owners = await whoamiOwners(project, composeFile);
    const states = await ownerStates(project, composeFile);
    const ready =
      Boolean(owners["owner-a"]?.length) &&
      Boolean(owners["owner-b"]?.length) &&
      states["owner-a"] === "passing" &&
      states["owner-b"] === "passing";

    return ready || "owner services/heartbeats not fully ready";
  });

  compose(project, composeFile, ["pause", "registrator-owner-a"]);

  await waitUntil("owner-a paused", 30_000, async () => {
    const ownerA = containerState(project, composeFile, "registrator-owner-a");
    return ownerA.paused ? true : "owner-a container not paused yet";
  });

  await waitUntil("owner-a service GC after pause", 90_000, async () => {
    const owners = await whoamiOwners(project, composeFile);
    const states = await ownerStates(project, composeFile);
    const ownerA = containerState(project, composeFile, "registrator-owner-a");
    const ownerB = containerState(project, composeFile, "registrator-owner-b");

    const cleaned =
      !owners["owner-a"] &&
      Boolean(owners["owner-b"]?.length) &&
      states["owner-b"] === "passing" &&
      states["owner-a"] !== "passing" &&
      ownerB.running &&
      ownerA.paused;

    return cleaned || "owner-a services not cleaned up yet";
  });
}

async function assertRecoverOwnedStaleServices(project: string, composeFile: string): Promise<void> {
  const staleIDs = new Set(["stale-whoami-a", "stale-whoami-b"]);

  await waitUntil("stale service recovery", 90_000, async () => {
    const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
    const instances = (await consulJson(project, composeFile, "/v1/catalog/service/whoami")) as any[];
    const states = await ownerStates(project, composeFile);

    const serviceIDs = new Set(Object.keys(services));
    if ([...staleIDs].some((id) => serviceIDs.has(id))) {
      return "stale ids still present in /v1/agent/services";
    }

    const instanceIDs = new Set(instances.map((item) => String(item.ServiceID ?? "")));
    if ([...staleIDs].some((id) => instanceIDs.has(id))) {
      return "stale ids still present in /v1/catalog/service/whoami";
    }

    const liveIDs = Object.entries(services)
      .filter(([serviceID, svc]: any) => {
        const meta = svc.Meta ?? {};
        return (
          meta["managed-by"] === "traefik-registrator" &&
          meta.kind !== "owner-heartbeat" &&
          meta["owner-id"] === "live-owner" &&
          serviceID.startsWith("docker-")
        );
      })
      .map(([serviceID]) => serviceID);

    const ok = liveIDs.length > 0 && states["live-owner"] === "passing";
    return ok || "no live owner-managed docker-* service or heartbeat not passing";
  });
}

async function assertTraefikEnableFilter(project: string, composeFile: string): Promise<void> {
  await waitUntil("traefik.enable filter behavior", 90_000, async () => {
    const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
    const names = new Set(managedServices(services).map((svc: any) => String(svc.Service ?? "")));
    const ok = names.has("enabled") && !names.has("disabled");
    return ok || "enabled/disabled service filter state not correct yet";
  });
}

function registerScenario(
  scenario: string,
  assertion: (project: string, composeFile: string) => Promise<void>,
): void {
  describe(scenario, () => {
    const composeFile = scenarioComposeFile(scenario);
    const project = scenarioProject(scenario);

    beforeAll(() => {
      if (!existsSync(composeFile)) {
        throw new Error(`missing compose scenario file: ${composeFile}`);
      }
    });

    beforeEach(() => {
      compose(project, composeFile, ["up", "-d", "--build", "--renew-anon-volumes"]);
    });

    afterEach(() => {
      composeDown(project, composeFile);
    });

    afterAll(() => {
      composeDown(project, composeFile);
    });

    it(
      "passes",
      async () => {
        await assertion(project, composeFile);
      },
      scenarioTimeoutMs,
    );
  });
}

describe("compose examples", () => {
  beforeAll(() => {
    run(["docker", "compose", "version"]);
  });

  registerScenario("basic-registration", assertBasicRegistration);
  registerScenario("custom-label-overrides", assertCustomLabelOverrides);
  registerScenario("gc-stale-services", assertGcStaleServices);
  registerScenario("https-only-file", assertHttpsOnlyFile);
  registerScenario("hybrid-docker-file", assertHybridDockerFile);
  registerScenario("owner-failover-gc", assertOwnerFailoverGc);
  registerScenario("recover-owned-stale-services", assertRecoverOwnedStaleServices);
  registerScenario("traefik-enable-filter", assertTraefikEnableFilter);
});
