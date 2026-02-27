package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	consulHTTPAddr       string
	consulToken          string
	dockerSocket         string
	resyncInterval       time.Duration
	requireTraefikEnable bool
	ownerID              string
	ownerHeartbeatTTL    time.Duration
	ownerHeartbeatPass   time.Duration
	gcInterval           time.Duration
	orphanGrace          time.Duration
	ownerDownGrace       time.Duration
	serviceIDPrefix      string
	serviceNameLabel     string
	servicePortLabel     string
	serviceAddressLabel  string
	defaultServiceName   string
}

type dockerContainer struct {
	ID              string            `json:"Id"`
	Names           []string          `json:"Names"`
	Labels          map[string]string `json:"Labels"`
	Ports           []dockerPort      `json:"Ports"`
	NetworkSettings dockerNetworks    `json:"NetworkSettings"`
}

type dockerPort struct {
	PrivatePort int `json:"PrivatePort"`
}

type dockerNetworks struct {
	Networks map[string]dockerNetworkEndpoint `json:"Networks"`
}

type dockerNetworkEndpoint struct {
	IPAddress string `json:"IPAddress"`
}

type consulServiceRegistration struct {
	ID      string              `json:"ID"`
	Name    string              `json:"Name"`
	Address string              `json:"Address,omitempty"`
	Port    int                 `json:"Port,omitempty"`
	Tags    []string            `json:"Tags,omitempty"`
	Meta    map[string]string   `json:"Meta,omitempty"`
	Check   *consulServiceCheck `json:"Check,omitempty"`
}

type dockerEvent struct {
	Type   string `json:"Type"`
	Action string `json:"Action"`
}

type consulServiceCheck struct {
	Name                           string `json:"Name,omitempty"`
	TTL                            string `json:"TTL,omitempty"`
	DeregisterCriticalServiceAfter string `json:"DeregisterCriticalServiceAfter,omitempty"`
}

type consulHealthCheck struct {
	Node        string `json:"Node"`
	CheckID     string `json:"CheckID"`
	Status      string `json:"Status"`
	ServiceID   string `json:"ServiceID"`
	ServiceName string `json:"ServiceName"`
}

type consulCatalogServiceInstance struct {
	Node        string            `json:"Node"`
	ServiceID   string            `json:"ServiceID"`
	ServiceName string            `json:"ServiceName"`
	ServiceMeta map[string]string `json:"ServiceMeta"`
}

const (
	managedByMetaKey       = "managed-by"
	managedByMetaValue     = "traefik-registrator"
	ownerIDMetaKey         = "owner-id"
	ownerHeartbeatService  = "traefik-registrator-owner"
	ownerHeartbeatIDPrefix = "traefik-registrator-owner-"
)

func main() {
	cfg := loadConfig()
	validateConfig(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	dockerHTTP := newDockerHTTPClient(cfg.dockerSocket)
	dockerEventsHTTP := newDockerEventsHTTPClient(cfg.dockerSocket)
	consulHTTP := &http.Client{Timeout: 10 * time.Second}

	log.Printf(
		"starting event loop: consul=%s docker_socket=%s resync_interval=%s gc_interval=%s require_traefik_enable=%t owner_id=%s",
		cfg.consulHTTPAddr,
		cfg.dockerSocket,
		cfg.resyncInterval,
		cfg.gcInterval,
		cfg.requireTraefikEnable,
		cfg.ownerID,
	)

	managed := map[string]struct{}{}
	if err := syncOnce(ctx, cfg, dockerHTTP, consulHTTP, managed); err != nil {
		log.Printf("initial sync failed: %v", err)
	}

	ownerServiceID := ownerHeartbeatIDPrefix + cfg.ownerID
	if err := registerOwnerHeartbeatService(ctx, cfg, consulHTTP, ownerServiceID); err != nil {
		log.Printf("owner heartbeat registration failed: %v", err)
	}
	if err := passOwnerHeartbeat(ctx, cfg, consulHTTP, ownerServiceID); err != nil {
		log.Printf("initial owner heartbeat pass failed: %v", err)
	}

	ticker := time.NewTicker(cfg.resyncInterval)
	defer ticker.Stop()
	ownerTicker := time.NewTicker(cfg.ownerHeartbeatPass)
	defer ownerTicker.Stop()
	gcTicker := time.NewTicker(cfg.gcInterval)
	defer gcTicker.Stop()
	events := make(chan dockerEvent, 64)
	gcSeen := map[string]time.Time{}
	go streamDockerEvents(ctx, dockerEventsHTTP, events)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown signal received; leaving current Consul registrations unchanged")
			return
		case ev := <-events:
			log.Printf("docker event: type=%s action=%s -> sync", ev.Type, ev.Action)
			if err := syncOnce(ctx, cfg, dockerHTTP, consulHTTP, managed); err != nil {
				log.Printf("sync after event failed: %v", err)
			}
		case <-ticker.C:
			if err := syncOnce(ctx, cfg, dockerHTTP, consulHTTP, managed); err != nil {
				log.Printf("periodic resync failed: %v", err)
			}
		case <-ownerTicker.C:
			if err := passOwnerHeartbeat(ctx, cfg, consulHTTP, ownerServiceID); err != nil {
				log.Printf("owner heartbeat pass failed: %v", err)
			}
		case <-gcTicker.C:
			if err := sweepStaleServices(ctx, cfg, consulHTTP, gcSeen); err != nil {
				log.Printf("gc sweep failed: %v", err)
			}
		}
	}
}

func validateConfig(cfg config) {
	if cfg.ownerID == "" {
		log.Fatal("OWNER_ID resolved to empty value")
	}
	if cfg.ownerHeartbeatPass >= cfg.ownerHeartbeatTTL {
		log.Fatalf(
			"OWNER_HEARTBEAT_PASS_INTERVAL (%s) must be smaller than OWNER_HEARTBEAT_TTL (%s)",
			cfg.ownerHeartbeatPass,
			cfg.ownerHeartbeatTTL,
		)
	}
}

func streamDockerEvents(ctx context.Context, dockerHTTP *http.Client, out chan<- dockerEvent) {
	backoff := time.Second
	for {
		err := readDockerEvents(ctx, dockerHTTP, out)
		if err == nil || ctx.Err() != nil {
			return
		}
		log.Printf("docker events stream disconnected: %v (retry in %s)", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 15*time.Second {
			backoff *= 2
			if backoff > 15*time.Second {
				backoff = 15 * time.Second
			}
		}
	}
}

func readDockerEvents(ctx context.Context, dockerHTTP *http.Client, out chan<- dockerEvent) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/events", nil)
	if err != nil {
		return err
	}

	filters := map[string][]string{
		"type":  {"container"},
		"event": {"start", "die", "stop", "destroy"},
	}
	rawFilters, err := json.Marshal(filters)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Set("filters", string(rawFilters))
	req.URL.RawQuery = q.Encode()

	resp, err := dockerHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker events API returned %s", resp.Status)
	}

	dec := json.NewDecoder(resp.Body)
	for {
		var ev dockerEvent
		if err := dec.Decode(&ev); err != nil {
			if errorsIsEOF(err) {
				return io.EOF
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case out <- ev:
		}
	}
}

func syncOnce(
	ctx context.Context,
	cfg config,
	dockerHTTP *http.Client,
	consulHTTP *http.Client,
	managed map[string]struct{},
) error {
	containers, err := listRunningContainers(ctx, dockerHTTP)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}

	desired := map[string]consulServiceRegistration{}
	for _, c := range containers {
		reg, ok := buildRegistration(cfg, c)
		if !ok {
			continue
		}
		desired[reg.ID] = reg
	}

	for _, reg := range desired {
		if err := registerService(ctx, cfg, consulHTTP, reg); err != nil {
			log.Printf("register %s failed: %v", reg.ID, err)
			continue
		}
		managed[reg.ID] = struct{}{}
	}

	for id := range managed {
		if _, ok := desired[id]; ok {
			continue
		}
		if err := deregisterService(ctx, cfg, consulHTTP, id); err != nil {
			log.Printf("deregister %s failed: %v", id, err)
			continue
		}
		delete(managed, id)
	}

	log.Printf("sync complete: running_containers=%d registered_services=%d", len(containers), len(desired))
	return nil
}

func listRunningContainers(ctx context.Context, dockerHTTP *http.Client) ([]dockerContainer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/json", nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("all", "0")
	req.URL.RawQuery = q.Encode()

	resp, err := dockerHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("docker API returned %s", resp.Status)
	}

	var containers []dockerContainer
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return nil, err
	}
	return containers, nil
}

func buildRegistration(cfg config, c dockerContainer) (consulServiceRegistration, bool) {
	if c.ID == "" {
		return consulServiceRegistration{}, false
	}

	labels := c.Labels
	if labels == nil {
		labels = map[string]string{}
	}

	if cfg.requireTraefikEnable && !isTruthy(labels["traefik.enable"]) {
		return consulServiceRegistration{}, false
	}

	address := serviceAddress(cfg, labels, c.NetworkSettings.Networks)
	if address == "" {
		log.Printf("skip %s: no container IP found", shortID(c.ID))
		return consulServiceRegistration{}, false
	}

	port := servicePort(cfg, labels, c.Ports)
	if port <= 0 {
		log.Printf("skip %s: no service port found", shortID(c.ID))
		return consulServiceRegistration{}, false
	}

	name := serviceName(cfg, labels, c.Names, c.ID)
	id := cfg.serviceIDPrefix + shortID(c.ID)
	tags := traefikTags(labels)
	meta := map[string]string{
		managedByMetaKey: managedByMetaValue,
	}
	if cfg.ownerID != "" {
		meta[ownerIDMetaKey] = cfg.ownerID
	}

	return consulServiceRegistration{
		ID:      id,
		Name:    name,
		Address: address,
		Port:    port,
		Tags:    tags,
		Meta:    meta,
	}, true
}

func registerService(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
	reg consulServiceRegistration,
) error {
	body, err := json.Marshal(reg)
	if err != nil {
		return err
	}

	u := strings.TrimRight(cfg.consulHTTPAddr, "/") + "/v1/agent/service/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.consulToken != "" {
		req.Header.Set("X-Consul-Token", cfg.consulToken)
	}

	resp, err := consulHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("consul API returned %s", resp.Status)
	}
	return nil
}

func deregisterService(ctx context.Context, cfg config, consulHTTP *http.Client, id string) error {
	u := strings.TrimRight(cfg.consulHTTPAddr, "/") + "/v1/agent/service/deregister/" + url.PathEscape(id)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, nil)
	if err != nil {
		return err
	}
	if cfg.consulToken != "" {
		req.Header.Set("X-Consul-Token", cfg.consulToken)
	}

	resp, err := consulHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("consul API returned %s", resp.Status)
	}
	return nil
}

func registerOwnerHeartbeatService(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
	serviceID string,
) error {
	reg := consulServiceRegistration{
		ID:   serviceID,
		Name: ownerHeartbeatService,
		Meta: map[string]string{
			managedByMetaKey: managedByMetaValue,
			ownerIDMetaKey:   cfg.ownerID,
			"kind":           "owner-heartbeat",
		},
		Check: &consulServiceCheck{
			Name:                           "owner heartbeat",
			TTL:                            cfg.ownerHeartbeatTTL.String(),
			DeregisterCriticalServiceAfter: cfg.ownerDownGrace.String(),
		},
	}
	return registerService(ctx, cfg, consulHTTP, reg)
}

func passOwnerHeartbeat(ctx context.Context, cfg config, consulHTTP *http.Client, serviceID string) error {
	checkID := "service:" + serviceID
	u := strings.TrimRight(cfg.consulHTTPAddr, "/") + "/v1/agent/check/pass/" + url.PathEscape(checkID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, nil)
	if err != nil {
		return err
	}
	if cfg.consulToken != "" {
		req.Header.Set("X-Consul-Token", cfg.consulToken)
	}

	resp, err := consulHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("consul API returned %s", resp.Status)
	}
	return nil
}

func sweepStaleServices(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
	seen map[string]time.Time,
) error {
	aliveOwners, err := listAliveOwners(ctx, cfg, consulHTTP)
	if err != nil {
		return err
	}

	serviceNames, err := listCatalogServiceNames(ctx, cfg, consulHTTP)
	if err != nil {
		return err
	}

	now := time.Now()
	activeCandidates := map[string]struct{}{}
	removed := 0

	for _, serviceName := range serviceNames {
		instances, err := listCatalogServiceInstances(ctx, cfg, consulHTTP, serviceName)
		if err != nil {
			log.Printf("gc skip service=%s: list instances failed: %v", serviceName, err)
			continue
		}

		for _, inst := range instances {
			if strings.TrimSpace(inst.ServiceID) == "" || strings.TrimSpace(inst.Node) == "" {
				continue
			}
			if inst.ServiceName == ownerHeartbeatService {
				continue
			}

			meta := inst.ServiceMeta
			if meta == nil || meta[managedByMetaKey] != managedByMetaValue {
				continue
			}
			if strings.TrimSpace(meta["kind"]) == "owner-heartbeat" {
				continue
			}

			owner := strings.TrimSpace(meta[ownerIDMetaKey])
			reason := ""
			grace := cfg.ownerDownGrace
			if owner == "" {
				reason = "missing owner-id"
				grace = cfg.orphanGrace
			} else if _, ok := aliveOwners[owner]; !ok {
				reason = "owner heartbeat missing"
			} else {
				continue
			}

			candidateKey := inst.Node + ":" + inst.ServiceID
			activeCandidates[candidateKey] = struct{}{}
			firstSeen, ok := seen[candidateKey]
			if !ok {
				seen[candidateKey] = now
				continue
			}
			if now.Sub(firstSeen) < grace {
				continue
			}

			if err := catalogDeregisterService(ctx, cfg, consulHTTP, inst.Node, inst.ServiceID); err != nil {
				log.Printf("gc deregister failed service_id=%s node=%s reason=%s err=%v", inst.ServiceID, inst.Node, reason, err)
				continue
			}
			delete(seen, candidateKey)
			removed++
			log.Printf("gc deregistered stale service service_id=%s node=%s reason=%s", inst.ServiceID, inst.Node, reason)
		}
	}

	for key := range seen {
		if _, ok := activeCandidates[key]; !ok {
			delete(seen, key)
		}
	}

	log.Printf("gc sweep complete: alive_owners=%d removed=%d", len(aliveOwners), removed)
	return nil
}

func listAliveOwners(ctx context.Context, cfg config, consulHTTP *http.Client) (map[string]struct{}, error) {
	u := strings.TrimRight(cfg.consulHTTPAddr, "/") + "/v1/health/checks/" + url.PathEscape(ownerHeartbeatService)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if cfg.consulToken != "" {
		req.Header.Set("X-Consul-Token", cfg.consulToken)
	}

	resp, err := consulHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return map[string]struct{}{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("consul API returned %s", resp.Status)
	}

	var checks []consulHealthCheck
	if err := json.NewDecoder(resp.Body).Decode(&checks); err != nil {
		return nil, err
	}

	alive := map[string]struct{}{}
	for _, check := range checks {
		if check.Status != "passing" {
			continue
		}
		serviceID := strings.TrimSpace(check.ServiceID)
		if !strings.HasPrefix(serviceID, ownerHeartbeatIDPrefix) {
			continue
		}
		owner := strings.TrimPrefix(serviceID, ownerHeartbeatIDPrefix)
		owner = strings.TrimSpace(owner)
		if owner == "" {
			continue
		}
		alive[owner] = struct{}{}
	}
	return alive, nil
}

func listCatalogServiceNames(ctx context.Context, cfg config, consulHTTP *http.Client) ([]string, error) {
	u := strings.TrimRight(cfg.consulHTTPAddr, "/") + "/v1/catalog/services"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if cfg.consulToken != "" {
		req.Header.Set("X-Consul-Token", cfg.consulToken)
	}

	resp, err := consulHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("consul API returned %s", resp.Status)
	}

	var raw map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func listCatalogServiceInstances(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
	serviceName string,
) ([]consulCatalogServiceInstance, error) {
	u := strings.TrimRight(cfg.consulHTTPAddr, "/") + "/v1/catalog/service/" + url.PathEscape(serviceName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if cfg.consulToken != "" {
		req.Header.Set("X-Consul-Token", cfg.consulToken)
	}

	resp, err := consulHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("consul API returned %s", resp.Status)
	}

	var instances []consulCatalogServiceInstance
	if err := json.NewDecoder(resp.Body).Decode(&instances); err != nil {
		return nil, err
	}
	return instances, nil
}

func catalogDeregisterService(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
	node string,
	serviceID string,
) error {
	payload := map[string]string{
		"Node":      node,
		"ServiceID": serviceID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	u := strings.TrimRight(cfg.consulHTTPAddr, "/") + "/v1/catalog/deregister"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.consulToken != "" {
		req.Header.Set("X-Consul-Token", cfg.consulToken)
	}

	resp, err := consulHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("consul API returned %s", resp.Status)
	}
	return nil
}

func serviceName(cfg config, labels map[string]string, names []string, containerID string) string {
	candidates := []string{
		labels[cfg.serviceNameLabel],
		labels["com.docker.compose.service"],
		firstContainerName(names),
		cfg.defaultServiceName,
		shortID(containerID),
	}
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c != "" {
			return c
		}
	}
	return "service"
}

func serviceAddress(
	cfg config,
	labels map[string]string,
	networks map[string]dockerNetworkEndpoint,
) string {
	if v := strings.TrimSpace(labels[cfg.serviceAddressLabel]); v != "" {
		return v
	}

	if len(networks) == 0 {
		return ""
	}

	networkNames := make([]string, 0, len(networks))
	for name := range networks {
		networkNames = append(networkNames, name)
	}
	sort.Strings(networkNames)

	for _, networkName := range networkNames {
		ip := strings.TrimSpace(networks[networkName].IPAddress)
		if ip != "" {
			return ip
		}
	}
	return ""
}

func servicePort(cfg config, labels map[string]string, ports []dockerPort) int {
	if v, ok := parsePositiveInt(strings.TrimSpace(labels[cfg.servicePortLabel])); ok {
		return v
	}

	traefikKeys := make([]string, 0, len(labels))
	for k := range labels {
		if strings.HasPrefix(k, "traefik.http.services.") && strings.HasSuffix(k, ".loadbalancer.server.port") {
			traefikKeys = append(traefikKeys, k)
		}
	}
	sort.Strings(traefikKeys)
	for _, key := range traefikKeys {
		if v, ok := parsePositiveInt(strings.TrimSpace(labels[key])); ok {
			return v
		}
	}

	for _, p := range ports {
		if p.PrivatePort > 0 {
			return p.PrivatePort
		}
	}
	return 0
}

func traefikTags(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		if strings.HasPrefix(key, "traefik.") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	tags := make([]string, 0, len(keys))
	for _, key := range keys {
		tags = append(tags, key+"="+labels[key])
	}
	return tags
}

func loadConfig() config {
	return config{
		consulHTTPAddr:       getEnv("CONSUL_HTTP_ADDR", "http://127.0.0.1:8500"),
		consulToken:          os.Getenv("CONSUL_HTTP_TOKEN"),
		dockerSocket:         getEnv("DOCKER_SOCKET", "/var/run/docker.sock"),
		resyncInterval:       mustParseDurationEnv("POLL_INTERVAL", "5m"),
		requireTraefikEnable: isTruthy(getEnv("REQUIRE_TRAEFIK_ENABLE", "true")),
		ownerID:              defaultOwnerID(),
		ownerHeartbeatTTL:    mustParseDurationEnv("OWNER_HEARTBEAT_TTL", "30s"),
		ownerHeartbeatPass:   mustParseDurationEnv("OWNER_HEARTBEAT_PASS_INTERVAL", "10s"),
		gcInterval:           mustParseDurationEnv("GC_INTERVAL", "1m"),
		orphanGrace:          mustParseDurationEnv("ORPHAN_GRACE_PERIOD", "10m"),
		ownerDownGrace:       mustParseDurationEnv("OWNER_DOWN_GRACE_PERIOD", "10m"),
		serviceIDPrefix:      getEnv("SERVICE_ID_PREFIX", "docker-"),
		serviceNameLabel:     getEnv("SERVICE_NAME_LABEL", "com.docker.compose.service"),
		servicePortLabel:     getEnv("SERVICE_PORT_LABEL", "consul.port"),
		serviceAddressLabel:  getEnv("SERVICE_ADDRESS_LABEL", "consul.address"),
		defaultServiceName:   getEnv("DEFAULT_SERVICE_NAME", "container"),
	}
}

func defaultOwnerID() string {
	if value := strings.TrimSpace(os.Getenv("OWNER_ID")); value != "" {
		return value
	}

	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(hostname)
}

func newDockerHTTPClient(socketPath string) *http.Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, "unix", socketPath)
		},
	}
	return &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}
}

func newDockerEventsHTTPClient(socketPath string) *http.Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, "unix", socketPath)
		},
	}
	return &http.Client{
		Transport: transport,
	}
}

func shortID(containerID string) string {
	containerID = strings.TrimSpace(containerID)
	if len(containerID) > 12 {
		return containerID[:12]
	}
	return containerID
}

func firstContainerName(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return strings.TrimPrefix(strings.TrimSpace(names[0]), "/")
}

func mustParseDurationEnv(key, fallback string) time.Duration {
	raw := getEnv(key, fallback)
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		log.Fatalf("invalid %s: %q", key, raw)
	}
	return d
}

func parsePositiveInt(raw string) (int, bool) {
	if raw == "" {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func isTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func errorsIsEOF(err error) bool {
	return err == io.EOF || strings.Contains(err.Error(), "EOF")
}
