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
