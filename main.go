package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

type config struct {
	consulHTTPAddr       string
	consulToken          string
	sourceMode           string
	dockerSocket         string
	fileSourcePath       string
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

type consulAgentService struct {
	ID   string            `json:"ID"`
	Meta map[string]string `json:"Meta"`
}

type fileSourceDocument struct {
	Services map[string]fileServiceDefinition `json:"services" yaml:"services"`
}

type fileServiceDefinition struct {
	ID      string            `json:"id" yaml:"id"`
	Name    string            `json:"name" yaml:"name"`
	Address string            `json:"address" yaml:"address"`
	Port    int               `json:"port" yaml:"port"`
	Tags    []string          `json:"tags" yaml:"tags"`
	Meta    map[string]string `json:"meta" yaml:"meta"`
	Key     string            `json:"-" yaml:"-"`
}

const (
	sourceModeDocker       = "docker"
	sourceModeFile         = "file"
	fileExtYAML            = ".yaml"
	fileExtYML             = ".yml"
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

	consulHTTP := &http.Client{Timeout: 10 * time.Second}

	log.Printf(
		"starting event loop: source_mode=%s consul=%s resync_interval=%s gc_interval=%s require_traefik_enable=%t owner_id=%s",
		cfg.sourceMode,
		cfg.consulHTTPAddr,
		cfg.resyncInterval,
		cfg.gcInterval,
		cfg.requireTraefikEnable,
		cfg.ownerID,
	)
	if cfg.sourceMode == sourceModeDocker {
		log.Printf("docker mode configured: docker_socket=%s", cfg.dockerSocket)
	} else {
		log.Printf(
			"file mode configured: file_source_path=%s",
			cfg.fileSourcePath,
		)
	}

	managed := map[string]struct{}{}
	gcSeen := map[string]time.Time{}

	var syncSource func(context.Context) error
	var dockerEvents <-chan dockerEvent
	var fileEvents <-chan struct{}

	switch cfg.sourceMode {
	case sourceModeDocker:
		dockerHTTP := newDockerHTTPClient(cfg.dockerSocket)
		dockerEventsHTTP := newDockerEventsHTTPClient(cfg.dockerSocket)
		syncSource = func(ctx context.Context) error {
			return syncDockerModeOnce(ctx, cfg, dockerHTTP, consulHTTP, managed)
		}
		events := make(chan dockerEvent, 64)
		dockerEvents = events
		go streamDockerEvents(ctx, dockerEventsHTTP, events)
	case sourceModeFile:
		syncSource = func(ctx context.Context) error {
			return syncFileModeOnce(ctx, cfg, consulHTTP, managed)
		}
		events := make(chan struct{}, 1)
		fileEvents = events
		go streamFileSourceChanges(ctx, cfg, events)
	}

	if err := syncSource(ctx); err != nil {
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

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown signal received; leaving current Consul registrations unchanged")
			return
		case ev := <-dockerEvents:
			log.Printf("docker event: type=%s action=%s -> sync", ev.Type, ev.Action)
			if err := syncSource(ctx); err != nil {
				log.Printf("sync after event failed: %v", err)
			}
		case <-fileEvents:
			log.Printf("file source change detected -> sync")
			if err := syncSource(ctx); err != nil {
				log.Printf("sync after file change failed: %v", err)
			}
		case <-ticker.C:
			if err := syncSource(ctx); err != nil {
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
	switch cfg.sourceMode {
	case sourceModeDocker:
		if strings.TrimSpace(cfg.dockerSocket) == "" {
			log.Fatal("DOCKER_SOCKET cannot be empty when SOURCE_MODE=docker")
		}
	case sourceModeFile:
		if strings.TrimSpace(cfg.fileSourcePath) == "" {
			log.Fatal("FILE_SOURCE_PATH cannot be empty when SOURCE_MODE=file")
		}
		if info, err := os.Stat(cfg.fileSourcePath); err == nil && !info.IsDir() && !isSupportedFileSourcePath(cfg.fileSourcePath) {
			log.Fatal("FILE_SOURCE_PATH must point to a .yaml/.yml file or directory containing .yaml/.yml files")
		}
	default:
		log.Fatalf("invalid SOURCE_MODE: %q (expected %q or %q)", cfg.sourceMode, sourceModeDocker, sourceModeFile)
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

func syncDockerModeOnce(
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

	return applyDesiredState(ctx, cfg, consulHTTP, managed, desired, fmt.Sprintf("running_containers=%d", len(containers)))
}

func syncFileModeOnce(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
	managed map[string]struct{},
) error {
	desired, configured, err := loadFileModeDesiredServices(cfg)
	if err != nil {
		return fmt.Errorf("load file source: %w", err)
	}
	return applyDesiredState(
		ctx,
		cfg,
		consulHTTP,
		managed,
		desired,
		fmt.Sprintf("configured_services=%d source_path=%s", configured, cfg.fileSourcePath),
	)
}

func applyDesiredState(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
	managed map[string]struct{},
	desired map[string]consulServiceRegistration,
	sourceSummary string,
) error {
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

	recovered := 0
	agentServices, err := listAgentServices(ctx, cfg, consulHTTP)
	if err != nil {
		log.Printf("owner reconciliation skipped: list agent services failed: %v", err)
	} else {
		for id, svc := range agentServices {
			if _, ok := desired[id]; ok {
				continue
			}

			meta := svc.Meta
			if meta == nil || meta[managedByMetaKey] != managedByMetaValue {
				continue
			}
			if strings.TrimSpace(meta["kind"]) == "owner-heartbeat" {
				continue
			}
			if strings.TrimSpace(meta[ownerIDMetaKey]) != cfg.ownerID {
				continue
			}

			if err := deregisterService(ctx, cfg, consulHTTP, id); err != nil {
				log.Printf("reconcile stale %s failed: %v", id, err)
				continue
			}
			delete(managed, id)
			recovered++
		}
	}

	log.Printf(
		"sync complete: %s registered_services=%d recovered_stale_services=%d",
		sourceSummary,
		len(desired),
		recovered,
	)
	return nil
}

func streamFileSourceChanges(ctx context.Context, cfg config, out chan<- struct{}) {
	backoff := time.Second
	for {
		err := watchFileSourceChanges(ctx, cfg, out)
		if err == nil || ctx.Err() != nil {
			return
		}

		log.Printf("file events stream disconnected: %v (retry in %s)", err, backoff)
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

func watchFileSourceChanges(ctx context.Context, cfg config, out chan<- struct{}) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	targets, err := collectFileWatchTargets(cfg.fileSourcePath)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if err := watcher.Add(target); err != nil {
			return fmt.Errorf("watch %s: %w", target, err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case err, ok := <-watcher.Errors:
			if !ok {
				return io.EOF
			}
			if err != nil {
				return err
			}
		case ev, ok := <-watcher.Events:
			if !ok {
				return io.EOF
			}
			if !isFileSourceEventRelevant(cfg.fileSourcePath, ev.Name) {
				continue
			}
			log.Printf("file source event: op=%s path=%s", fileWatchOpString(ev.Op), ev.Name)
			select {
			case out <- struct{}{}:
			default:
			}
		}
	}
}

func loadFileModeDesiredServices(cfg config) (map[string]consulServiceRegistration, int, error) {
	files, err := collectFileSourcePaths(cfg.fileSourcePath)
	if err != nil {
		return nil, 0, err
	}

	desired := map[string]consulServiceRegistration{}
	configured := 0

	for _, filePath := range files {
		services, err := parseFileServiceDefinitions(filePath)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: %w", filePath, err)
		}
		for idx, svc := range services {
			reg, err := buildFileRegistration(cfg, svc, filePath)
			if err != nil {
				return nil, 0, fmt.Errorf("%s service[%d]: %w", filePath, idx, err)
			}
			if _, exists := desired[reg.ID]; exists {
				return nil, 0, fmt.Errorf("%s service[%d]: duplicate service id %q", filePath, idx, reg.ID)
			}
			desired[reg.ID] = reg
			configured++
		}
	}

	return desired, configured, nil
}

func collectFileWatchTargets(sourcePath string) ([]string, error) {
	sourcePath = filepath.Clean(sourcePath)
	info, err := os.Stat(sourcePath)
	if err == nil && info.IsDir() {
		parent := filepath.Dir(sourcePath)
		if parent == sourcePath {
			return []string{sourcePath}, nil
		}
		return []string{sourcePath, parent}, nil
	}
	if err == nil {
		return []string{filepath.Dir(sourcePath)}, nil
	}
	if os.IsNotExist(err) {
		return []string{filepath.Dir(sourcePath)}, nil
	}
	return nil, err
}

func isFileSourceEventRelevant(sourcePath, eventPath string) bool {
	sourcePath = filepath.Clean(sourcePath)
	eventPath = filepath.Clean(eventPath)
	if sourcePath == eventPath {
		return true
	}

	info, err := os.Stat(sourcePath)
	if err == nil && info.IsDir() {
		return filepath.Dir(eventPath) == sourcePath && isSupportedFileSourcePath(eventPath)
	}
	if err == nil {
		return eventPath == sourcePath
	}
	if os.IsNotExist(err) {
		return eventPath == sourcePath ||
			(filepath.Dir(eventPath) == sourcePath && isSupportedFileSourcePath(eventPath))
	}
	return false
}

func fileWatchOpString(op fsnotify.Op) string {
	parts := make([]string, 0, 5)
	if op&fsnotify.Create != 0 {
		parts = append(parts, "CREATE")
	}
	if op&fsnotify.Write != 0 {
		parts = append(parts, "WRITE")
	}
	if op&fsnotify.Remove != 0 {
		parts = append(parts, "REMOVE")
	}
	if op&fsnotify.Rename != 0 {
		parts = append(parts, "RENAME")
	}
	if op&fsnotify.Chmod != 0 {
		parts = append(parts, "CHMOD")
	}
	if len(parts) == 0 {
		return "UNKNOWN"
	}
	return strings.Join(parts, "|")
}

func collectFileSourcePaths(sourcePath string) ([]string, error) {
	info, err := os.Stat(sourcePath)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		if !isSupportedFileSourcePath(sourcePath) {
			return nil, fmt.Errorf("unsupported FILE_SOURCE_PATH extension for %q: only .yaml/.yml are supported", sourcePath)
		}
		return []string{sourcePath}, nil
	}

	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(sourcePath, entry.Name())
		if !isSupportedFileSourcePath(path) {
			continue
		}
		files = append(files, path)
	}
	sort.Strings(files)
	return files, nil
}

func parseFileServiceDefinitions(filePath string) ([]fileServiceDefinition, error) {
	if !isSupportedFileSourcePath(filePath) {
		return nil, fmt.Errorf("unsupported file extension: only .yaml/.yml are supported")
	}

	raw, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(string(raw)) == "" {
		return nil, nil
	}

	var doc fileSourceDocument
	if err := yaml.Unmarshal(raw, &doc); err == nil && doc.Services != nil {
		keys := make([]string, 0, len(doc.Services))
		for key := range doc.Services {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		services := make([]fileServiceDefinition, 0, len(keys))
		for _, key := range keys {
			svc := doc.Services[key]
			svc.Key = strings.TrimSpace(key)
			if svc.Key != "" && strings.TrimSpace(svc.Name) == "" {
				svc.Name = svc.Key
			}
			services = append(services, svc)
		}
		return services, nil
	}

	return nil, fmt.Errorf("expected YAML object with \"services\" keyed map")
}

func isSupportedFileSourcePath(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == fileExtYAML || ext == fileExtYML
}

func buildFileRegistration(
	cfg config,
	svc fileServiceDefinition,
	sourceFile string,
) (consulServiceRegistration, error) {
	address := strings.TrimSpace(svc.Address)
	if address == "" {
		return consulServiceRegistration{}, fmt.Errorf("address is required")
	}
	if svc.Port <= 0 {
		return consulServiceRegistration{}, fmt.Errorf("port must be > 0")
	}

	id := strings.TrimSpace(svc.ID)
	if id == "" {
		keyID := normalizeFileServiceKey(svc.Key)
		if keyID != "" {
			id = cfg.serviceIDPrefix + keyID
		} else {
			id = deriveFileServiceID(cfg, sourceFile, svc)
		}
	}

	name := strings.TrimSpace(svc.Name)
	if name == "" {
		name = cfg.defaultServiceName
	}

	meta := map[string]string{}
	for key, value := range svc.Meta {
		k := strings.TrimSpace(key)
		if k == "" {
			continue
		}
		meta[k] = value
	}
	meta[managedByMetaKey] = managedByMetaValue
	if cfg.ownerID != "" {
		meta[ownerIDMetaKey] = cfg.ownerID
	}
	meta["source"] = sourceModeFile
	meta["source-file"] = filepath.Base(sourceFile)

	tags := make([]string, 0, len(svc.Tags))
	for _, tag := range svc.Tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		tags = append(tags, tag)
	}
	sort.Strings(tags)

	return consulServiceRegistration{
		ID:      id,
		Name:    name,
		Address: address,
		Port:    svc.Port,
		Tags:    tags,
		Meta:    meta,
	}, nil
}

func normalizeFileServiceKey(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}

	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		isAllowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
		if isAllowed {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func deriveFileServiceID(cfg config, sourceFile string, svc fileServiceDefinition) string {
	tags := append([]string(nil), svc.Tags...)
	sort.Strings(tags)
	seed := strings.Join(
		[]string{
			sourceFile,
			strings.TrimSpace(svc.Name),
			strings.TrimSpace(svc.Address),
			strconv.Itoa(svc.Port),
			strings.Join(tags, ","),
		},
		"|",
	)
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	return cfg.serviceIDPrefix + "file-" + strconv.FormatUint(h.Sum64(), 16)
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

func listAgentServices(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
) (map[string]consulAgentService, error) {
	u := strings.TrimRight(cfg.consulHTTPAddr, "/") + "/v1/agent/services"
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

	var services map[string]consulAgentService
	if err := json.NewDecoder(resp.Body).Decode(&services); err != nil {
		return nil, err
	}
	if services == nil {
		return map[string]consulAgentService{}, nil
	}
	return services, nil
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
	aliveOwnersByNode, err := listAliveOwnersByNode(ctx, cfg, consulHTTP)
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
			} else if nodes, ok := aliveOwnersByNode[owner]; !ok {
				reason = "owner heartbeat missing"
			} else if !ownerPassingOnNode(nodes, inst.Node) {
				reason = "owner heartbeat missing on node"
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

	log.Printf("gc sweep complete: alive_owners=%d removed=%d", len(aliveOwnersByNode), removed)
	return nil
}

func listAliveOwnersByNode(
	ctx context.Context,
	cfg config,
	consulHTTP *http.Client,
) (map[string]map[string]struct{}, error) {
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
		return map[string]map[string]struct{}{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("consul API returned %s", resp.Status)
	}

	var checks []consulHealthCheck
	if err := json.NewDecoder(resp.Body).Decode(&checks); err != nil {
		return nil, err
	}

	alive := map[string]map[string]struct{}{}
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
		nodes := alive[owner]
		if nodes == nil {
			nodes = map[string]struct{}{}
			alive[owner] = nodes
		}
		node := strings.TrimSpace(check.Node)
		// Be conservative if Consul omits node from a passing check.
		// Empty node acts as wildcard to avoid false-positive cleanup.
		nodes[node] = struct{}{}
	}
	return alive, nil
}

func ownerPassingOnNode(nodes map[string]struct{}, node string) bool {
	if len(nodes) == 0 {
		return false
	}
	if _, ok := nodes[""]; ok {
		return true
	}
	_, ok := nodes[strings.TrimSpace(node)]
	return ok
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
		sourceMode:           strings.ToLower(getEnv("SOURCE_MODE", sourceModeDocker)),
		dockerSocket:         getEnv("DOCKER_SOCKET", "/var/run/docker.sock"),
		fileSourcePath:       getEnv("FILE_SOURCE_PATH", "/etc/traefik-registrator/services.d"),
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
