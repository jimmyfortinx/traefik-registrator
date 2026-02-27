package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type rewriteTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.URL.Scheme = t.target.Scheme
	cloned.URL.Host = t.target.Host
	return t.base.RoundTrip(cloned)
}

func defaultTestConfig() config {
	return config{
		consulHTTPAddr:       "http://127.0.0.1:8500",
		requireTraefikEnable: true,
		ownerID:              "owner-a",
		ownerHeartbeatTTL:    30 * time.Second,
		ownerHeartbeatPass:   10 * time.Second,
		gcInterval:           time.Second,
		orphanGrace:          10 * time.Millisecond,
		ownerDownGrace:       10 * time.Millisecond,
		serviceIDPrefix:      "docker-",
		serviceNameLabel:     "com.docker.compose.service",
		servicePortLabel:     "consul.port",
		serviceAddressLabel:  "consul.address",
		defaultServiceName:   "container",
	}
}

func TestBuildRegistrationTraefikEnableFilter(t *testing.T) {
	cfg := defaultTestConfig()
	container := dockerContainer{
		ID:    "0123456789abcdef",
		Ports: []dockerPort{{PrivatePort: 8080}},
		NetworkSettings: dockerNetworks{Networks: map[string]dockerNetworkEndpoint{
			"default": {IPAddress: "10.10.0.2"},
		}},
	}

	_, ok := buildRegistration(cfg, container)
	if ok {
		t.Fatalf("expected container without traefik.enable to be skipped")
	}

	cfg.requireTraefikEnable = false
	reg, ok := buildRegistration(cfg, container)
	if !ok {
		t.Fatalf("expected container to be registered when REQUIRE_TRAEFIK_ENABLE=false")
	}
	if reg.Name != "container" {
		t.Fatalf("expected default service name, got %q", reg.Name)
	}
}

func TestBuildRegistrationSelectsExpectedFields(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.serviceNameLabel = "registrator.name"
	cfg.servicePortLabel = "registrator.port"
	cfg.serviceAddressLabel = "registrator.address"
	cfg.serviceIDPrefix = "svc-"

	container := dockerContainer{
		ID:    "1234567890abcdef1234",
		Names: []string{"/whoami"},
		Labels: map[string]string{
			"traefik.enable":                                        "true",
			"traefik.http.routers.whoami.rule":                      "Host(`whoami.local`)",
			"traefik.http.services.whoami.loadbalancer.server.port": "80",
			"registrator.name":                                      "edge-api",
			"registrator.port":                                      "9000",
			"registrator.address":                                   "10.1.2.3",
		},
		Ports: []dockerPort{{PrivatePort: 8080}},
		NetworkSettings: dockerNetworks{Networks: map[string]dockerNetworkEndpoint{
			"default": {IPAddress: "172.20.0.10"},
		}},
	}

	reg, ok := buildRegistration(cfg, container)
	if !ok {
		t.Fatalf("expected registration to be built")
	}

	if reg.ID != "svc-1234567890ab" {
		t.Fatalf("unexpected service id: %q", reg.ID)
	}
	if reg.Name != "edge-api" {
		t.Fatalf("unexpected service name: %q", reg.Name)
	}
	if reg.Address != "10.1.2.3" {
		t.Fatalf("unexpected service address: %q", reg.Address)
	}
	if reg.Port != 9000 {
		t.Fatalf("unexpected service port: %d", reg.Port)
	}
	if reg.Meta[managedByMetaKey] != managedByMetaValue {
		t.Fatalf("managed-by meta is missing")
	}
	if reg.Meta[ownerIDMetaKey] != "owner-a" {
		t.Fatalf("owner-id meta is missing")
	}

	expectedTags := []string{
		"traefik.enable=true",
		"traefik.http.routers.whoami.rule=Host(`whoami.local`)",
		"traefik.http.services.whoami.loadbalancer.server.port=80",
	}
	if len(reg.Tags) != len(expectedTags) {
		t.Fatalf("unexpected tags count: got=%d want=%d", len(reg.Tags), len(expectedTags))
	}
	for i, tag := range expectedTags {
		if reg.Tags[i] != tag {
			t.Fatalf("unexpected tag at %d: got=%q want=%q", i, reg.Tags[i], tag)
		}
	}
}

func TestServicePortPriorityAndFallbacks(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.servicePortLabel = "registrator.port"

	cases := []struct {
		name   string
		labels map[string]string
		ports  []dockerPort
		want   int
	}{
		{
			name: "custom label wins",
			labels: map[string]string{
				"registrator.port": "7000",
				"traefik.http.services.api.loadbalancer.server.port": "8000",
			},
			ports: []dockerPort{{PrivatePort: 9000}},
			want:  7000,
		},
		{
			name: "traefik label fallback",
			labels: map[string]string{
				"traefik.http.services.b.loadbalancer.server.port": "8100",
				"traefik.http.services.a.loadbalancer.server.port": "8200",
			},
			ports: []dockerPort{{PrivatePort: 9000}},
			want:  8200,
		},
		{
			name: "private port fallback",
			labels: map[string]string{
				"registrator.port": "abc",
			},
			ports: []dockerPort{{PrivatePort: 8080}},
			want:  8080,
		},
		{
			name:   "no port",
			labels: map[string]string{},
			ports:  nil,
			want:   0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := servicePort(cfg, tc.labels, tc.ports)
			if got != tc.want {
				t.Fatalf("unexpected port: got=%d want=%d", got, tc.want)
			}
		})
	}
}

func TestSyncOnceRegistersThenDeregisters(t *testing.T) {
	ctx := context.Background()
	cfg := defaultTestConfig()

	var (
		containersMu sync.Mutex
		containers   = []dockerContainer{
			{
				ID:    "0123456789abcdef",
				Names: []string{"/api"},
				Labels: map[string]string{
					"traefik.enable": "true",
					"traefik.http.services.api.loadbalancer.server.port": "8080",
				},
				NetworkSettings: dockerNetworks{Networks: map[string]dockerNetworkEndpoint{
					"app": {IPAddress: "172.18.0.2"},
				}},
			},
		}
	)

	dockerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/containers/json" {
			http.NotFound(w, r)
			return
		}
		containersMu.Lock()
		defer containersMu.Unlock()
		_ = json.NewEncoder(w).Encode(containers)
	}))
	defer dockerSrv.Close()

	var (
		consulMu        sync.Mutex
		registeredIDs   []string
		deregisteredIDs []string
	)
	consulSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/agent/service/register":
			var reg consulServiceRegistration
			if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			consulMu.Lock()
			registeredIDs = append(registeredIDs, reg.ID)
			consulMu.Unlock()
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/agent/service/deregister/"):
			id := strings.TrimPrefix(r.URL.Path, "/v1/agent/service/deregister/")
			id, _ = url.PathUnescape(id)
			consulMu.Lock()
			deregisteredIDs = append(deregisteredIDs, id)
			consulMu.Unlock()
		default:
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer consulSrv.Close()

	targetURL, err := url.Parse(dockerSrv.URL)
	if err != nil {
		t.Fatalf("parse docker test server url: %v", err)
	}
	dockerHTTP := &http.Client{Transport: rewriteTransport{target: targetURL, base: http.DefaultTransport}}
	consulHTTP := consulSrv.Client()
	cfg.consulHTTPAddr = consulSrv.URL

	managed := map[string]struct{}{}
	if err := syncDockerModeOnce(ctx, cfg, dockerHTTP, consulHTTP, managed); err != nil {
		t.Fatalf("first sync failed: %v", err)
	}
	if _, ok := managed["docker-0123456789ab"]; !ok {
		t.Fatalf("expected service to be tracked as managed")
	}

	containersMu.Lock()
	containers = nil
	containersMu.Unlock()

	if err := syncDockerModeOnce(ctx, cfg, dockerHTTP, consulHTTP, managed); err != nil {
		t.Fatalf("second sync failed: %v", err)
	}
	if len(managed) != 0 {
		t.Fatalf("expected managed map to be empty after deregistration")
	}

	consulMu.Lock()
	defer consulMu.Unlock()
	if len(registeredIDs) == 0 {
		t.Fatalf("expected at least one registration")
	}
	if len(deregisteredIDs) != 1 || deregisteredIDs[0] != "docker-0123456789ab" {
		t.Fatalf("unexpected deregistered IDs: %#v", deregisteredIDs)
	}
}

func TestApplyDesiredStateRecoversStaleServicesAfterRestart(t *testing.T) {
	ctx := context.Background()
	cfg := defaultTestConfig()

	desired := map[string]consulServiceRegistration{
		"docker-live": {
			ID:      "docker-live",
			Name:    "live",
			Address: "10.10.0.2",
			Port:    8080,
			Meta: map[string]string{
				managedByMetaKey: managedByMetaValue,
				ownerIDMetaKey:   "owner-a",
			},
		},
	}

	var (
		mu            sync.Mutex
		deregistered  []string
		agentServices = map[string]consulAgentService{
			"docker-old": {
				ID: "docker-old",
				Meta: map[string]string{
					managedByMetaKey: managedByMetaValue,
					ownerIDMetaKey:   "owner-a",
				},
			},
			"docker-other-owner": {
				ID: "docker-other-owner",
				Meta: map[string]string{
					managedByMetaKey: managedByMetaValue,
					ownerIDMetaKey:   "owner-b",
				},
			},
			"traefik-registrator-owner-owner-a": {
				ID: "traefik-registrator-owner-owner-a",
				Meta: map[string]string{
					managedByMetaKey: managedByMetaValue,
					ownerIDMetaKey:   "owner-a",
					"kind":           "owner-heartbeat",
				},
			},
		}
	)

	consulSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/agent/service/register":
			var reg consulServiceRegistration
			if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			agentServices[reg.ID] = consulAgentService{
				ID:   reg.ID,
				Meta: reg.Meta,
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodGet && r.URL.Path == "/v1/agent/services":
			mu.Lock()
			defer mu.Unlock()
			_ = json.NewEncoder(w).Encode(agentServices)
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/agent/service/deregister/"):
			id := strings.TrimPrefix(r.URL.Path, "/v1/agent/service/deregister/")
			id, _ = url.PathUnescape(id)
			mu.Lock()
			deregistered = append(deregistered, id)
			delete(agentServices, id)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))
	defer consulSrv.Close()

	cfg.consulHTTPAddr = consulSrv.URL
	managed := map[string]struct{}{}
	if err := applyDesiredState(ctx, cfg, consulSrv.Client(), managed, desired, "restart-reconcile"); err != nil {
		t.Fatalf("apply desired state failed: %v", err)
	}

	sort.Strings(deregistered)
	wantDeregistered := []string{"docker-old"}
	if strings.Join(deregistered, ",") != strings.Join(wantDeregistered, ",") {
		t.Fatalf("unexpected reconciled deregistration: got=%v want=%v", deregistered, wantDeregistered)
	}

	if _, ok := managed["docker-live"]; !ok {
		t.Fatalf("expected live service to be tracked as managed")
	}
	if _, ok := managed["docker-old"]; ok {
		t.Fatalf("stale service should not remain tracked as managed")
	}
}

func TestSweepStaleServicesRemovesDeadOwnersAndOrphans(t *testing.T) {
	ctx := context.Background()
	cfg := defaultTestConfig()
	cfg.orphanGrace = 5 * time.Millisecond
	cfg.ownerDownGrace = 5 * time.Millisecond

	type deregisterPayload struct {
		Node      string `json:"Node"`
		ServiceID string `json:"ServiceID"`
	}

	var (
		mu         sync.Mutex
		deregister []deregisterPayload
	)

	consulSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/health/checks/" + ownerHeartbeatService:
			_ = json.NewEncoder(w).Encode([]consulHealthCheck{{
				Status:    "passing",
				ServiceID: ownerHeartbeatIDPrefix + "live-owner",
			}})
			return
		case "/v1/catalog/services":
			_ = json.NewEncoder(w).Encode(map[string][]string{
				"web":    {},
				"orphan": {},
			})
			return
		case "/v1/catalog/service/web":
			_ = json.NewEncoder(w).Encode([]consulCatalogServiceInstance{
				{
					Node:        "node-live",
					ServiceID:   "web-live",
					ServiceName: "web",
					ServiceMeta: map[string]string{managedByMetaKey: managedByMetaValue, ownerIDMetaKey: "live-owner"},
				},
				{
					Node:        "node-dead",
					ServiceID:   "web-dead",
					ServiceName: "web",
					ServiceMeta: map[string]string{managedByMetaKey: managedByMetaValue, ownerIDMetaKey: "dead-owner"},
				},
				{
					Node:        "node-other",
					ServiceID:   "web-unmanaged",
					ServiceName: "web",
					ServiceMeta: map[string]string{},
				},
			})
			return
		case "/v1/catalog/service/orphan":
			_ = json.NewEncoder(w).Encode([]consulCatalogServiceInstance{
				{
					Node:        "node-orphan",
					ServiceID:   "orphan-1",
					ServiceName: "orphan",
					ServiceMeta: map[string]string{managedByMetaKey: managedByMetaValue},
				},
			})
			return
		case "/v1/catalog/deregister":
			var payload deregisterPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			deregister = append(deregister, payload)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))
	defer consulSrv.Close()

	cfg.consulHTTPAddr = consulSrv.URL
	seen := map[string]time.Time{}

	if err := sweepStaleServices(ctx, cfg, consulSrv.Client(), seen); err != nil {
		t.Fatalf("first sweep failed: %v", err)
	}
	mu.Lock()
	if len(deregister) != 0 {
		mu.Unlock()
		t.Fatalf("expected no immediate deregistration, got %#v", deregister)
	}
	mu.Unlock()

	time.Sleep(15 * time.Millisecond)
	if err := sweepStaleServices(ctx, cfg, consulSrv.Client(), seen); err != nil {
		t.Fatalf("second sweep failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(deregister) != 2 {
		t.Fatalf("expected 2 deregistrations, got %#v", deregister)
	}

	got := []string{fmt.Sprintf("%s/%s", deregister[0].Node, deregister[0].ServiceID), fmt.Sprintf("%s/%s", deregister[1].Node, deregister[1].ServiceID)}
	sort.Strings(got)
	want := []string{"node-dead/web-dead", "node-orphan/orphan-1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected deregistration payloads: got=%v want=%v", got, want)
	}
}

func TestTraefikTagsSorted(t *testing.T) {
	labels := map[string]string{
		"traefik.http.routers.b.rule": "Host(`b.local`)",
		"traefik.http.routers.a.rule": "Host(`a.local`)",
		"not-traefik":                 "ignored",
	}
	got := traefikTags(labels)
	want := []string{
		"traefik.http.routers.a.rule=Host(`a.local`)",
		"traefik.http.routers.b.rule=Host(`b.local`)",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected tags: got=%v want=%v", got, want)
	}
}
