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
	ID      string   `json:"ID"`
	Name    string   `json:"Name"`
	Address string   `json:"Address"`
	Port    int      `json:"Port"`
	Tags    []string `json:"Tags,omitempty"`
}

type dockerEvent struct {
	Type   string `json:"Type"`
	Action string `json:"Action"`
}

func main() {
	cfg := loadConfig()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	dockerHTTP := newDockerHTTPClient(cfg.dockerSocket)
	dockerEventsHTTP := newDockerEventsHTTPClient(cfg.dockerSocket)
	consulHTTP := &http.Client{Timeout: 10 * time.Second}

	log.Printf(
		"starting event loop: consul=%s docker_socket=%s resync_interval=%s require_traefik_enable=%t",
		cfg.consulHTTPAddr,
		cfg.dockerSocket,
		cfg.resyncInterval,
		cfg.requireTraefikEnable,
	)

	managed := map[string]struct{}{}
	if err := syncOnce(ctx, cfg, dockerHTTP, consulHTTP, managed); err != nil {
		log.Printf("initial sync failed: %v", err)
	}

	ticker := time.NewTicker(cfg.resyncInterval)
	defer ticker.Stop()
	events := make(chan dockerEvent, 64)
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
		}
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

	return consulServiceRegistration{
		ID:      id,
		Name:    name,
		Address: address,
		Port:    port,
		Tags:    tags,
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
		resyncInterval:       mustParseDuration(getEnv("POLL_INTERVAL", "5m")),
		requireTraefikEnable: isTruthy(getEnv("REQUIRE_TRAEFIK_ENABLE", "true")),
		serviceIDPrefix:      getEnv("SERVICE_ID_PREFIX", "docker-"),
		serviceNameLabel:     getEnv("SERVICE_NAME_LABEL", "com.docker.compose.service"),
		servicePortLabel:     getEnv("SERVICE_PORT_LABEL", "consul.port"),
		serviceAddressLabel:  getEnv("SERVICE_ADDRESS_LABEL", "consul.address"),
		defaultServiceName:   getEnv("DEFAULT_SERVICE_NAME", "container"),
	}
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

func mustParseDuration(raw string) time.Duration {
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		log.Fatalf("invalid POLL_INTERVAL: %q", raw)
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
