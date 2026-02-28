import { describe, it } from "bun:test";
import {
  deploySwarmStack,
  scenarioTimeoutMs,
  swarmServiceName,
  swarmWhoamiInstances,
  waitForSwarmManagedServiceCount,
  waitForSwarmRegistratorReady,
  waitForSwarmServiceReplicas,
  waitForWhoamiRegistration,
  withSwarmCluster,
  withSwarmStack,
  readSwarmAgentServices,
  restartSwarmService,
} from "./test-utils";

const swarmTestEnabled = process.env.RUN_SWARM_E2E === "1";
const testTimeoutMs = scenarioTimeoutMs + 4 * 60_000;

const suite = swarmTestEnabled ? describe : describe.skip;

suite("swarm e2e", () => {
  it(
    "deploys in docker swarm and registers services",
    async () => {
      await withSwarmStack(async ({ managerName }) => {
        await waitForWhoamiRegistration(managerName);
      });
    },
    testTimeoutMs,
  );

  it(
    "keeps the same whoami instance count after registrator restart",
    async () => {
      await withSwarmStack(async ({ managerName, stackName }) => {
        const initial = await waitForWhoamiRegistration(managerName);
        const initialCount = initial.length;

        restartSwarmService(managerName, swarmServiceName(stackName, "registrator"));

        await waitForSwarmRegistratorReady(managerName, stackName);
        await waitForWhoamiRegistration(managerName, initialCount);

        const after = swarmWhoamiInstances(readSwarmAgentServices(managerName));
        if (after.length !== initialCount) {
          throw new Error(`unexpected whoami instance count after restart: expected=${initialCount} got=${after.length}`);
        }
      });
    },
    testTimeoutMs,
  );

  it(
    "keeps worker1 docker + worker2 file whoami counts stable across docker and file registrator restarts",
    async () => {
      await withSwarmCluster(async ({ managerName, stackName }) => {
        deploySwarmStack(managerName, stackName, [
          "/workspace/tests/e2e/swarm-dual-worker1.yml",
          "/workspace/tests/e2e/swarm-dual-worker2.yml",
          "/workspace/tests/e2e/swarm-dual-registrators.yml",
        ]);

        await waitForSwarmServiceReplicas(managerName, swarmServiceName(stackName, "registrator_docker"), "2/2");
        await waitForSwarmServiceReplicas(managerName, swarmServiceName(stackName, "registrator_file"), "1/1");

        await waitForSwarmManagedServiceCount(managerName, "worker1 whoami via docker registrators", 1, (svc: any) =>
          String(svc.Service ?? "").includes("whoami_worker1"),
        );
        await waitForSwarmManagedServiceCount(managerName, "worker2 whoami via file registrator", 1, (svc: any) =>
          String(svc.ID ?? "") === "file-whoami-worker2",
        );

        restartSwarmService(managerName, swarmServiceName(stackName, "registrator_docker"));
        await waitForSwarmServiceReplicas(managerName, swarmServiceName(stackName, "registrator_docker"), "2/2");

        await waitForSwarmManagedServiceCount(managerName, "worker1 whoami after docker registrator restart", 1, (svc: any) =>
          String(svc.Service ?? "").includes("whoami_worker1"),
        );
        await waitForSwarmManagedServiceCount(managerName, "worker2 whoami after docker registrator restart", 1, (svc: any) =>
          String(svc.ID ?? "") === "file-whoami-worker2",
        );

        restartSwarmService(managerName, swarmServiceName(stackName, "registrator_file"));
        await waitForSwarmServiceReplicas(managerName, swarmServiceName(stackName, "registrator_file"), "1/1");

        await waitForSwarmManagedServiceCount(managerName, "worker1 whoami after file registrator restart", 1, (svc: any) =>
          String(svc.Service ?? "").includes("whoami_worker1"),
        );
        await waitForSwarmManagedServiceCount(managerName, "worker2 whoami after file registrator restart", 1, (svc: any) =>
          String(svc.ID ?? "") === "file-whoami-worker2",
        );
      });
    },
    testTimeoutMs,
  );
});
