import path from "node:path";
import { afterAll, afterEach, beforeAll, beforeEach, describe, it } from "bun:test";
import { existsSync } from "node:fs";

const repoRoot = process.cwd();
const examplesDir = path.join(repoRoot, "examples");

export const scenarioTimeoutMs = 8 * 60_000;

export function run(cmd: string[], cwd = repoRoot): string {
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

export function compose(project: string, composeFile: string, args: string[]): string {
  return run(["docker", "compose", "-p", project, "-f", composeFile, ...args]);
}

export function composeDown(project: string, composeFile: string): void {
  try {
    compose(project, composeFile, ["down", "-v", "--remove-orphans"]);
  } catch {
    // Cleanup is best-effort.
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function waitUntil(
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

export async function consulJson(project: string, composeFile: string, pathAndQuery: string): Promise<any> {
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

export function managedServices(agentServices: Record<string, any>): any[] {
  return Object.values(agentServices).filter((svc: any) => {
    const meta = svc.Meta ?? {};
    return meta["managed-by"] === "traefik-registrator" && meta.kind !== "owner-heartbeat";
  });
}

export async function ownerStates(project: string, composeFile: string): Promise<Record<string, string>> {
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

export async function whoamiOwners(project: string, composeFile: string): Promise<Record<string, string[]>> {
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

export function containerState(
  project: string,
  composeFile: string,
  service: string,
): { running: boolean; paused: boolean } {
  const containerID = compose(project, composeFile, ["ps", "-q", service]).trim();
  const raw = run(["docker", "inspect", "--format", "{{json .State}}", containerID]);
  const state = JSON.parse(raw);
  return { running: Boolean(state.Running), paused: Boolean(state.Paused) };
}

export function scenarioProject(scenario: string): string {
  return `trreg-${scenario}`.replace(/[^a-z0-9-]/g, "-").slice(0, 40);
}

export function scenarioComposeFile(scenario: string): string {
  return path.join(examplesDir, scenario, "docker-compose.yml");
}

export function registerScenario(
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

export type SwarmRun = {
  managerName: string;
  worker1Name: string;
  worker2Name: string;
  netName: string;
  stackName: string;
};

export function readSwarmAgentServices(managerName: string): Record<string, any> {
  const body = run([
    "docker",
    "exec",
    managerName,
    "sh",
    "-ec",
    "wget -qO- http://127.0.0.1:18500/v1/agent/services",
  ]);
  return JSON.parse(body) as Record<string, any>;
}

export function swarmWhoamiInstances(services: Record<string, any>): any[] {
  return managedServices(services).filter((svc: any) => String(svc.Service ?? "").includes("whoami"));
}

export function readSwarmManagedServices(managerName: string): any[] {
  return managedServices(readSwarmAgentServices(managerName));
}

export async function waitForWhoamiRegistration(managerName: string, expectedCount = 1): Promise<any[]> {
  let latest: any[] = [];
  await waitUntil("whoami registration in consul", 180_000, async () => {
    const services = readSwarmAgentServices(managerName);
    const instances = swarmWhoamiInstances(services);
    latest = instances;
    if (instances.length !== expectedCount) return `expected ${expectedCount} whoami instance(s), got ${instances.length}`;

    const tags = new Set(instances[0]?.Tags ?? []);
    const ok =
      instances.every((svc: any) => svc.Meta?.["owner-id"] === "swarm-manager") &&
      tags.has("traefik.enable=true") &&
      tags.has("traefik.http.routers.whoami.rule=Host(`whoami.swarm.local`)");

    return ok || "whoami registration metadata/tags not reconciled yet";
  });
  return latest;
}

export async function waitForSwarmRegistratorReady(managerName: string, stackName: string): Promise<void> {
  await waitForSwarmServiceReplicas(managerName, `${stackName}_registrator`, "1/1");
}

async function waitForSwarmDocker(name: string): Promise<void> {
  await waitUntil(`dockerd in ${name}`, 120_000, async () => {
    try {
      run(["docker", "exec", name, "docker", "info"]);
      return true;
    } catch {
      return "docker daemon not ready";
    }
  });
}

async function waitForSwarmNodes(managerName: string): Promise<void> {
  await waitUntil("3 swarm nodes ready", 120_000, async () => {
    const nodes = run(["docker", "exec", managerName, "docker", "node", "ls", "--format", "{{.Status}}"]).split("\n");
    return nodes.filter((status) => status.trim() === "Ready").length >= 3 || "all nodes are not ready";
  });
}

function mirrorSwarmImage(managerName: string, workerName: string, image: string): void {
  run([
    "sh",
    "-ec",
    `docker exec ${managerName} docker save ${image} | docker exec -i ${workerName} docker load`,
  ]);
}

function cleanupSwarmRun(runCtx: SwarmRun): void {
  const { managerName, worker1Name, worker2Name, netName } = runCtx;
  const nodes = [managerName, worker1Name, worker2Name];
  try {
    run(["docker", "exec", managerName, "docker", "stack", "rm", runCtx.stackName]);
  } catch {
    // Best-effort cleanup.
  }
  try {
    run(["docker", "exec", managerName, "docker", "swarm", "leave", "--force"]);
  } catch {
    // Best-effort cleanup.
  }
  for (const node of [worker1Name, worker2Name]) {
    try {
      run(["docker", "exec", node, "docker", "swarm", "leave"]);
    } catch {
      // Best-effort cleanup.
    }
  }
  try {
    run(["docker", "rm", "-f", ...nodes]);
  } catch {
    // Best-effort cleanup.
  }
  try {
    run(["docker", "network", "rm", netName]);
  } catch {
    // Best-effort cleanup.
  }
}

export function swarmServiceName(stackName: string, service: string): string {
  return `${stackName}_${service}`;
}

export function deploySwarmStack(managerName: string, stackName: string, composeFiles: string[]): void {
  const cmd = [
    "docker",
    "exec",
    managerName,
    "docker",
    "stack",
    "deploy",
    ...composeFiles.flatMap((composeFile) => ["-c", composeFile]),
    stackName,
  ];
  run(cmd);
}

export function restartSwarmService(managerName: string, fullServiceName: string): void {
  run(["docker", "exec", managerName, "docker", "service", "update", "--force", fullServiceName]);
}

export async function waitForSwarmServiceReplicas(
  managerName: string,
  fullServiceName: string,
  expectedReplicas: string,
): Promise<void> {
  await waitUntil(`swarm service ${fullServiceName} replicas ${expectedReplicas}`, 180_000, async () => {
    const replicas = run([
      "docker",
      "exec",
      managerName,
      "docker",
      "service",
      "ls",
      "--filter",
      `name=${fullServiceName}`,
      "--format",
      "{{.Replicas}}",
    ]).trim();
    return replicas === expectedReplicas || `replicas=${replicas || "<empty>"}`;
  });
}

export async function waitForSwarmManagedServiceCount(
  managerName: string,
  description: string,
  expectedCount: number,
  match: (svc: any) => boolean,
): Promise<any[]> {
  let latest: any[] = [];
  await waitUntil(description, 180_000, async () => {
    const matched = readSwarmManagedServices(managerName).filter(match);
    latest = matched;
    return matched.length === expectedCount || `expected ${expectedCount}, got ${matched.length}`;
  });
  return latest;
}

export async function withSwarmCluster(assertion: (runCtx: SwarmRun) => Promise<void>): Promise<void> {
  const suffix = `${Date.now()}-${Math.floor(Math.random() * 10_000)}`;
  const runCtx: SwarmRun = {
    managerName: `trreg-mgr-${suffix}`,
    worker1Name: `trreg-w1-${suffix}`,
    worker2Name: `trreg-w2-${suffix}`,
    netName: `trreg-net-${suffix}`,
    stackName: `trregswarm-${suffix}`,
  };
  const nodes = [runCtx.managerName, runCtx.worker1Name, runCtx.worker2Name];

  run(["docker", "network", "create", runCtx.netName]);
  for (const node of nodes) {
    run([
      "docker",
      "run",
      "-d",
      "--privileged",
      "--name",
      node,
      "--hostname",
      node,
      "--network",
      runCtx.netName,
      "-e",
      "DOCKER_TLS_CERTDIR=",
      "-v",
      `${repoRoot}:/workspace`,
      "docker:27-dind",
    ]);
  }

  try {
    await Promise.all(nodes.map(waitForSwarmDocker));
    run(["docker", "exec", runCtx.managerName, "docker", "swarm", "init", "--advertise-addr", "eth0"]);

    const workerToken = run(["docker", "exec", runCtx.managerName, "docker", "swarm", "join-token", "-q", "worker"]);
    run([
      "docker",
      "exec",
      runCtx.worker1Name,
      "docker",
      "swarm",
      "join",
      "--token",
      workerToken,
      `${runCtx.managerName}:2377`,
    ]);
    run([
      "docker",
      "exec",
      runCtx.worker2Name,
      "docker",
      "swarm",
      "join",
      "--token",
      workerToken,
      `${runCtx.managerName}:2377`,
    ]);
    await waitForSwarmNodes(runCtx.managerName);

    run([
      "docker",
      "exec",
      runCtx.managerName,
      "docker",
      "node",
      "update",
      "--label-add",
      "trreg_worker1=true",
      runCtx.worker1Name,
    ]);
    run([
      "docker",
      "exec",
      runCtx.managerName,
      "docker",
      "node",
      "update",
      "--label-add",
      "trreg_worker2=true",
      runCtx.worker2Name,
    ]);

    run(["docker", "exec", runCtx.managerName, "docker", "build", "-t", "traefik-registrator:swarm-e2e", "/workspace"]);
    mirrorSwarmImage(runCtx.managerName, runCtx.worker1Name, "traefik-registrator:swarm-e2e");
    mirrorSwarmImage(runCtx.managerName, runCtx.worker2Name, "traefik-registrator:swarm-e2e");
    await assertion(runCtx);
  } finally {
    cleanupSwarmRun(runCtx);
  }
}

export async function withSwarmStack(assertion: (runCtx: SwarmRun) => Promise<void>): Promise<void> {
  await withSwarmCluster(async (runCtx) => {
    deploySwarmStack(runCtx.managerName, runCtx.stackName, ["/workspace/tests/e2e/swarm-stack.yml"]);
    await waitForSwarmRegistratorReady(runCtx.managerName, runCtx.stackName);
    await assertion(runCtx);
  });
}
