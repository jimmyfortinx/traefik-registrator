import { beforeAll, describe } from "bun:test";
import {
  compose,
  consulJson,
  managedServices,
  registerScenario,
  run,
  waitUntil,
} from "./test-utils";

describe("compose examples", () => {
  beforeAll(() => {
    run(["docker", "compose", "version"]);
  });

  registerScenario("basic-registration", async (project, composeFile) => {
    await waitUntil("basic service registration", 90_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const managed = managedServices(services);
      const whoami = managed.find((svc: any) => svc.Service === "whoami");
      if (!whoami) return "missing whoami service";

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

    compose(project, composeFile, ["stop", "whoami"]);
    await waitUntil("whoami deregistration after stop event", 45_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const stillPresent = managedServices(services).some((svc: any) => svc.Service === "whoami");
      return stillPresent ? "whoami still registered after stop" : true;
    });

    compose(project, composeFile, ["start", "whoami"]);
    await waitUntil("whoami re-registration after start event", 45_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const present = managedServices(services).some(
        (svc: any) => svc.Service === "whoami" && svc.Meta?.["owner-id"] === "example-basic",
      );
      return present || "whoami not re-registered after start";
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
    await waitUntil("owner-scoped catalog cleanup", 90_000, async () => {
      const owned = (await consulJson(project, composeFile, "/v1/catalog/service/stale-live-owner-app")) as any[];
      const foreign = (await consulJson(project, composeFile, "/v1/catalog/service/foreign-owner-app")) as any[];
      const hasOwnedA = owned.some((item) => item.ServiceID === "seed-live-owner-a");
      const hasOwnedB = owned.some((item) => item.ServiceID === "seed-live-owner-b");
      const hasForeign = foreign.some((item) => item.ServiceID === "seed-foreign-owner");
      return !hasOwnedA && !hasOwnedB && hasForeign ? true : "owner-scoped cleanup did not converge";
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

    compose(project, composeFile, [
      "exec",
      "-T",
      "seed-file-config",
      "sh",
      "-ec",
      "rm -f /fixtures/services.d/routes.yml",
    ]);

    await waitUntil("file deletion propagation", 45_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const managed = managedServices(services);
      const dockerStillPresent = managed.some(
        (svc: any) => svc.Service === "webdocker" && svc.Meta?.["owner-id"] === "hybrid-docker-owner",
      );
      const filePresent = Boolean(services["file-webfile"]);
      return dockerStillPresent && !filePresent ? true : "file service still present after file deletion";
    });

    compose(project, composeFile, [
      "exec",
      "-T",
      "seed-file-config",
      "sh",
      "-ec",
      "cat > /fixtures/services.d/routes.yml <<'YAML'\nservices:\n  webfile:\n    address: webfile\n    port: 80\n    tags:\n      - traefik.enable=true\n      - traefik.http.routers.webfile.rule=Host(`webfile-recreated.local`)\nYAML",
    ]);

    await waitUntil("file creation propagation", 45_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const fileSvc = services["file-webfile"];
      if (!fileSvc) return "file service not re-created";
      const tags = new Set(fileSvc.Tags ?? []);
      const recreated = tags.has("traefik.http.routers.webfile.rule=Host(`webfile-recreated.local`)");
      return recreated || "re-created file service does not have expected rule";
    });
  });

  registerScenario("recover-owned-stale-services", async (project, composeFile) => {
    const staleIDs = new Set(["stale-whoami-a", "stale-whoami-b", "stale-catalog-live-owner"]);
    const foreignIDs = new Set(["stale-foreign-owner", "stale-catalog-foreign-owner"]);

    await waitUntil("stale service recovery", 90_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const instances = (await consulJson(project, composeFile, "/v1/catalog/service/whoami")) as any[];

      const serviceIDs = new Set(Object.keys(services));
      if ([...staleIDs].some((id) => serviceIDs.has(id))) {
        return "stale ids still present in /v1/agent/services";
      }

      const instanceIDs = new Set(instances.map((item) => String(item.ServiceID ?? "")));
      if ([...staleIDs].some((id) => instanceIDs.has(id))) {
        return "stale ids still present in /v1/catalog/service/whoami";
      }

      if ([...foreignIDs].some((id) => !serviceIDs.has(id) && !instanceIDs.has(id))) {
        return "foreign owner stale services were removed unexpectedly";
      }

      const liveIDs = Object.entries(services)
        .filter(([serviceID, svc]: any) => {
          const meta = svc.Meta ?? {};
          return (
            meta["managed-by"] === "traefik-registrator" &&
            meta["owner-id"] === "live-owner" &&
            serviceID.startsWith("docker-")
          );
        })
        .map(([serviceID]) => serviceID);

      const ok = liveIDs.length > 0;
      return ok || "no live owner-managed docker-* service";
    });
  });

  registerScenario("delayed-catalog-duplicate", async (project, composeFile) => {
    const owner = "example-late-reconcile";

    await waitUntil("initial whoami registration", 90_000, async () => {
      const services = (await consulJson(project, composeFile, "/v1/agent/services")) as Record<string, any>;
      const managed = managedServices(services);
      const instance = managed.find(
        (svc: any) => svc.Service === "whoami" && svc.Meta?.["managed-by"] === "traefik-registrator" && svc.Meta?.["owner-id"] === owner,
      );
      return Boolean(instance) || "missing initial whoami registration";
    });

    await waitUntil("delayed duplicate appears in catalog", 90_000, async () => {
      const instances = (await consulJson(project, composeFile, "/v1/catalog/service/whoami")) as any[];
      const owned = instances.filter((item: any) => {
        const meta = item.ServiceMeta ?? {};
        return meta["managed-by"] === "traefik-registrator" && meta["owner-id"] === owner;
      });
      const duplicatePresent = owned.some((item: any) => String(item.Node ?? "") === "seed-node-late");
      return duplicatePresent || "seeded delayed duplicate not visible yet";
    });

    await waitUntil("delayed duplicate is pruned by startup reconciliation", 90_000, async () => {
      const instances = (await consulJson(project, composeFile, "/v1/catalog/service/whoami")) as any[];
      const owned = instances.filter((item: any) => {
        const meta = item.ServiceMeta ?? {};
        return meta["managed-by"] === "traefik-registrator" && meta["owner-id"] === owner;
      });

      const seedPresent = owned.some((item: any) => String(item.Node ?? "") === "seed-node-late");
      const uniqueIDs = new Set(owned.map((item: any) => String(item.ServiceID ?? "")));

      return !seedPresent && owned.length === 1 && uniqueIDs.size === 1
        ? true
        : `owned whoami instances did not converge: count=${owned.length} seed_present=${seedPresent}`;
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
