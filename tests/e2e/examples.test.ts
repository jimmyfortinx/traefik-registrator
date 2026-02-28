import { beforeAll, describe } from "bun:test";
import {
  consulJson,
  containerState,
  managedServices,
  ownerStates,
  registerScenario,
  run,
  waitUntil,
  whoamiOwners,
} from "./test-utils";

describe("compose examples", () => {
  beforeAll(() => {
    run(["docker", "compose", "version"]);
  });

  registerScenario("basic-registration", async (project, composeFile) => {
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
  });

  registerScenario("custom-label-overrides", async (project, composeFile) => {
    await waitUntil("custom label override registration", 90_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const managed = managedServices(services);
      const svc = managed.find((item: any) => item.Service === "payments-api");
      if (!svc) return "payments-api service not present yet";

      const ok = svc.Address === "10.9.8.7" && svc.Port === 9090 && svc.Meta?.["owner-id"] === "example-custom";
      return ok || "payments-api has unexpected address/port/owner";
    });
  });

  registerScenario("gc-stale-services", async (project, composeFile) => {
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
  });

  registerScenario("https-only-file", async (project, composeFile) => {
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
  });

  registerScenario("hybrid-docker-file", async (project, composeFile) => {
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
  });

  registerScenario("owner-failover-gc", async (project, composeFile) => {
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
  });

  registerScenario("recover-owned-stale-services", async (project, composeFile) => {
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
  });

  registerScenario("traefik-enable-filter", async (project, composeFile) => {
    await waitUntil("traefik.enable filter behavior", 90_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const names = new Set(managedServices(services).map((svc: any) => String(svc.Service ?? "")));
      const ok = names.has("enabled") && !names.has("disabled");
      return ok || "enabled/disabled service filter state not correct yet";
    });
  });
});
