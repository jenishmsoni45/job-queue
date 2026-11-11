// Package compose checks the Compose file and the lockfiles against NFR-15 and
// NFR-16: pinned images, loopback-only ports, healthchecks and a durable
// Postgres (P1-5), only the chaos harness mounting the Docker socket, and only
// under the chaos profile (P3-8, D-025), and only the PLAN stack in go.mod and
// console/ (P1-6).
//
// The Go tests run in the tools container, which has no Docker CLI (D-021), so
// `make test` first writes `docker compose config --format json` on the host
// (D-034): .cache/compose-config.json for the normal stack,
// .cache/compose-config-chaos.json for the chaos stack, which is the separate
// jobqueue-chaos project with docker-compose.chaos.yml on top (D-042), and
// .cache/compose-config-bench.json for the bench stack, the jobqueue-bench
// project with docker-compose.bench.yml on top (D-053). These tests read those
// files.
package compose

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/jenishmsoni45/job-queue/internal/pgtest"
)

const (
	repoRoot        = "../.."
	configFile      = ".cache/compose-config.json"
	chaosConfigFile = ".cache/compose-config-chaos.json"
	benchConfigFile = ".cache/compose-config-bench.json"
	chaosService    = "chaos"
	chaosProfile    = "chaos"
	chaosProject    = "jobqueue-chaos"
	benchProfile    = "bench"
	benchProject    = "jobqueue-bench"
)

// The default-profile services phase 1 defines.
var defaultServices = []string{
	"postgres", "toxiproxy", "prometheus", "migrate",
	"api", "dispatcher", "worker-1", "worker-2", "mockshop",
}

// oneShots run a command and exit, so they have no healthcheck (D-033).
var oneShots = map[string]bool{"migrate": true, "tools": true, chaosService: true, "loadgen": true, "bench": true}

// queuedRoles must start only after migrate has exited 0 (D-033).
var queuedRoles = []string{"api", "dispatcher", "worker-1", "worker-2"}

type config struct {
	Name     string             `json:"name"`
	Services map[string]service `json:"services"`
}

type service struct {
	Image       string               `json:"image"`
	Build       *build               `json:"build"`
	Profiles    []string             `json:"profiles"`
	Command     []string             `json:"command"`
	Entrypoint  []string             `json:"entrypoint"`
	Environment map[string]*string   `json:"environment"`
	Ports       []port               `json:"ports"`
	Volumes     []volume             `json:"volumes"`
	Healthcheck *healthcheck         `json:"healthcheck"`
	DependsOn   map[string]dependsOn `json:"depends_on"`
	Restart     string               `json:"restart"`
	NetworkMode string               `json:"network_mode"`
	Privileged  bool                 `json:"privileged"`
}

type build struct {
	Context    string `json:"context"`
	Dockerfile string `json:"dockerfile"`
}

type port struct {
	HostIP    string `json:"host_ip"`
	Target    int    `json:"target"`
	Published string `json:"published"`
}

type volume struct {
	Type   string `json:"type"`
	Source string `json:"source"`
	Target string `json:"target"`
}

type healthcheck struct {
	Test    []string `json:"test"`
	Disable bool     `json:"disable"`
}

type dependsOn struct {
	Condition string `json:"condition"`
}

func loadConfig(t *testing.T) config { return loadConfigFile(t, configFile) }

func loadConfigFile(t *testing.T, file string) config {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot, file)) //nolint:gosec // one of the two config files this package names
	if err != nil {
		t.Fatalf("read %s: %v. Run the tests with `make test`, which writes it with `docker compose config --format json`.", file, err)
	}
	var c config
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	if len(c.Services) == 0 {
		t.Fatalf("%s lists no services", file)
	}
	return c
}

// bothStacks runs check on the normal stack, the chaos stack and the bench stack.
func bothStacks(t *testing.T, check func(t *testing.T, c config)) {
	for _, file := range []string{configFile, chaosConfigFile, benchConfigFile} {
		t.Run(filepath.Base(file), func(t *testing.T) { check(t, loadConfigFile(t, file)) })
	}
}

func isDefaultProfile(s service) bool { return len(s.Profiles) == 0 }

func TestDefaultServices(t *testing.T) {
	c := loadConfig(t)
	var got []string
	for name, s := range c.Services {
		if isDefaultProfile(s) {
			got = append(got, name)
		}
	}
	slices.Sort(got)
	want := slices.Clone(defaultServices)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("default-profile services = %v, want %v", got, want)
	}
	if tools, ok := c.Services["tools"]; !ok || !slices.Equal(tools.Profiles, []string{"tools"}) {
		t.Errorf("the tools service must exist under the tools profile only; got %+v", tools.Profiles)
	}
	if chaos, ok := c.Services[chaosService]; !ok || !slices.Equal(chaos.Profiles, []string{chaosProfile}) {
		t.Errorf("the chaos service must exist under the chaos profile only; got %+v", chaos.Profiles)
	}
	for _, name := range []string{"loadgen", "bench"} {
		if s, ok := c.Services[name]; !ok || !slices.Equal(s.Profiles, []string{benchProfile}) {
			t.Errorf("the %s service must exist under the bench profile only; got %+v", name, s.Profiles)
		}
	}
}

// pinnedRef is name:tag@sha256:digest, where the tag starts with an exact
// version of at least major.minor (16.15-trixie, v3.15.1-busybox, 2.13.0).
var pinnedRef = regexp.MustCompile(`^[a-z0-9]+(?:[._/:-][a-z0-9]+)*:v?[0-9]+(?:\.[0-9]+)+(?:[-_.][A-Za-z0-9._-]+)?@sha256:[0-9a-f]{64}$`)

func checkPinned(ref string) error {
	if !pinnedRef.MatchString(ref) {
		return fmt.Errorf("image %q is not pinned to an exact version and a sha256 digest", ref)
	}
	return nil
}

// dockerfileBases returns the external images a Dockerfile builds from,
// leaving out `scratch` and references to its own earlier stages.
func dockerfileBases(path string) ([]string, error) {
	f, err := os.Open(path) //nolint:gosec // path comes from the repo's own Compose file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	stages := map[string]bool{}
	var bases []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || !strings.EqualFold(fields[0], "FROM") {
			continue
		}
		args := slices.DeleteFunc(fields[1:], func(s string) bool { return strings.HasPrefix(s, "--") })
		if len(args) == 0 {
			return nil, fmt.Errorf("%s: FROM without an image", path)
		}
		image := args[0]
		if len(args) >= 3 && strings.EqualFold(args[1], "AS") {
			stages[strings.ToLower(args[2])] = true
		}
		if image == "scratch" || stages[strings.ToLower(image)] {
			continue
		}
		bases = append(bases, image)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return bases, nil
}

// P1-5: every image is pinned to an exact version and digest. A service built
// here is checked through the FROM lines of its Dockerfile.
func TestImagesArePinned(t *testing.T) { bothStacks(t, checkImagesPinned) }

func checkImagesPinned(t *testing.T, c config) {
	for name, s := range c.Services {
		if s.Build == nil {
			if err := checkPinned(s.Image); err != nil {
				t.Errorf("service %s: %v", name, err)
			}
			continue
		}
		dockerfile := filepath.Join(repoRoot, s.Build.Context, s.Build.Dockerfile)
		bases, err := dockerfileBases(dockerfile)
		if err != nil {
			t.Errorf("service %s: %v", name, err)
			continue
		}
		if len(bases) == 0 {
			t.Errorf("service %s: %s has no base image", name, dockerfile)
		}
		for _, b := range bases {
			if err := checkPinned(b); err != nil {
				t.Errorf("service %s (%s): %v", name, dockerfile, err)
			}
		}
	}
}

func TestPinCheckRejectsLooseReferences(t *testing.T) {
	const digest = "@sha256:f9a7b9ebf6f56201f949db745e47bc423c0f0f8294bafe991cc10bf74b029657"
	good := []string{
		"postgres:16.15-trixie" + digest,
		"prom/prometheus:v3.15.1-busybox" + digest,
		"ghcr.io/shopify/toxiproxy:2.13.0" + digest,
		"golang:1.27.3-trixie" + digest,
	}
	bad := []string{
		"postgres",
		"postgres:16",
		"postgres:16.15",
		"postgres:latest" + digest,
		"postgres:16" + digest,
		"postgres" + digest,
		"postgres:16.15@sha256:abc",
		"postgres:16.15-trixie@sha256:" + strings.Repeat("g", 64),
		"",
	}
	for _, ref := range good {
		if err := checkPinned(ref); err != nil {
			t.Errorf("checkPinned(%q) = %v, want nil", ref, err)
		}
	}
	for _, ref := range bad {
		if checkPinned(ref) == nil {
			t.Errorf("checkPinned(%q) = nil, want an error", ref)
		}
	}
}

func TestDockerfileBases(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Dockerfile")
	content := strings.Join([]string{
		"# comment FROM ignored",
		"FROM --platform=linux/amd64 golang:1.27.3-trixie@sha256:abc AS build",
		"RUN echo from here is not a FROM line",
		"from build AS test",
		"FROM scratch",
		"COPY --from=build /out /",
		"FROM alpine:latest",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := dockerfileBases(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"golang:1.27.3-trixie@sha256:abc", "alpine:latest"}
	if !slices.Equal(got, want) {
		t.Fatalf("dockerfileBases = %v, want %v", got, want)
	}
}

func checkPorts(s service) error {
	var errs []error
	if s.NetworkMode == "host" {
		errs = append(errs, errors.New("uses network_mode host, which publishes every port on every interface"))
	}
	for _, p := range s.Ports {
		if p.HostIP != "127.0.0.1" {
			errs = append(errs, fmt.Errorf("publishes port %d on host IP %q, want 127.0.0.1", p.Target, p.HostIP))
		}
	}
	return errors.Join(errs...)
}

// P1-5: every published port is bound to 127.0.0.1 (NFR-16). The chaos stack
// publishes none (TestChaosStack).
func TestPortsBoundToLoopback(t *testing.T) {
	c := loadConfig(t)
	published := 0
	for name, s := range c.Services {
		if err := checkPorts(s); err != nil {
			t.Errorf("service %s: %v", name, err)
		}
		published += len(s.Ports)
	}
	if published == 0 {
		t.Error("no service publishes a port; the check would pass vacuously")
	}
}

func TestPortCheckRejectsExposedPorts(t *testing.T) {
	bad := []service{
		{Ports: []port{{HostIP: "", Target: 8080}}},
		{Ports: []port{{HostIP: "0.0.0.0", Target: 8080}}},
		{Ports: []port{{HostIP: "127.0.0.1", Target: 8080}, {HostIP: "::", Target: 9090}}},
		{NetworkMode: "host"},
	}
	for i, s := range bad {
		if checkPorts(s) == nil {
			t.Errorf("case %d: checkPorts = nil, want an error", i)
		}
	}
	if err := checkPorts(service{Ports: []port{{HostIP: "127.0.0.1", Target: 8080}}}); err != nil {
		t.Errorf("loopback port rejected: %v", err)
	}
}

func mountsDockerSocket(s service) bool {
	for _, v := range s.Volumes {
		if strings.Contains(v.Source, "docker.sock") || strings.Contains(v.Target, "docker.sock") {
			return true
		}
	}
	return false
}

// checkSocketPolicy enforces D-025: only the chaos service may mount the Docker
// socket, and only under the chaos profile alone. Nothing runs privileged.
func checkSocketPolicy(name string, s service) error {
	var errs []error
	if s.Privileged {
		errs = append(errs, errors.New("runs privileged"))
	}
	if mountsDockerSocket(s) {
		switch {
		case name != chaosService:
			errs = append(errs, errors.New("mounts the Docker socket, which only the chaos harness may (D-025)"))
		case !slices.Equal(s.Profiles, []string{chaosProfile}):
			errs = append(errs, fmt.Errorf("mounts the Docker socket under profiles %v; only the chaos profile may (D-025)", s.Profiles))
		}
	}
	if name == chaosService && len(s.Ports) > 0 {
		errs = append(errs, errors.New("the chaos harness publishes a port"))
	}
	return errors.Join(errs...)
}

// P1-5 and P3-8: only the chaos harness mounts the Docker socket, and only
// under the chaos profile, in the normal stack and in the chaos stack (NFR-16,
// D-025). The harness does mount it, so the check is not vacuous.
func TestOnlyTheChaosHarnessMountsTheDockerSocket(t *testing.T) {
	bothStacks(t, func(t *testing.T, c config) {
		for name, s := range c.Services {
			if err := checkSocketPolicy(name, s); err != nil {
				t.Errorf("service %s: %v", name, err)
			}
		}
		if !mountsDockerSocket(c.Services[chaosService]) {
			t.Error("the chaos harness doesn't mount the Docker socket, so it can't control the containers (D-025)")
		}
	})
}

func TestSocketCheckRejectsMounts(t *testing.T) {
	sock := []volume{{Type: "bind", Source: "/var/run/docker.sock", Target: "/var/run/docker.sock"}}
	bad := []struct {
		name string
		s    service
	}{
		{"api", service{Volumes: sock}},
		{"worker-1", service{Volumes: []volume{{Type: "bind", Source: "/run/user/501/docker.sock", Target: "/sock"}}}},
		{"api", service{Privileged: true}},
		{chaosService, service{Volumes: sock}},                                            // in the default profile
		{chaosService, service{Volumes: sock, Profiles: []string{chaosProfile, "tools"}}}, // under another profile too
		{chaosService, service{Volumes: sock, Profiles: []string{chaosProfile}, Privileged: true}},
		{chaosService, service{Volumes: sock, Profiles: []string{chaosProfile}, Ports: []port{{HostIP: "127.0.0.1", Target: 80}}}},
		{"chaos-helper", service{Volumes: sock, Profiles: []string{chaosProfile}}},
	}
	for i, tt := range bad {
		if checkSocketPolicy(tt.name, tt.s) == nil {
			t.Errorf("case %d (%s): checkSocketPolicy = nil, want an error", i, tt.name)
		}
	}
	if err := checkSocketPolicy(chaosService, service{Volumes: sock, Profiles: []string{chaosProfile}}); err != nil {
		t.Errorf("the chaos harness under the chaos profile was rejected: %v", err)
	}
}

// D-042: the chaos stack is its own project, publishes nothing on the host,
// routes the workers and the dispatcher to Postgres through their own Toxiproxy
// links, and keeps Postgres durable.
func TestChaosStack(t *testing.T) {
	c := loadConfigFile(t, chaosConfigFile)
	if c.Name != chaosProject {
		t.Errorf("chaos project name = %q, want %q", c.Name, chaosProject)
	}
	for name, s := range c.Services {
		if len(s.Ports) > 0 {
			t.Errorf("service %s publishes %d ports in the chaos stack", name, len(s.Ports))
		}
	}
	ports := map[string]bool{}
	for _, name := range []string{"dispatcher", "worker-1", "worker-2"} {
		env := c.Services[name].Environment
		host, port := env["PGHOST"], env["PGPORT"]
		if host == nil || *host != "toxiproxy" || port == nil || ports[*port] {
			t.Errorf("service %s reaches Postgres at %v:%v, want its own toxiproxy link", name, deref(host), deref(port))
			continue
		}
		ports[*port] = true
	}
	for _, name := range []string{"api", chaosService} {
		if host := c.Services[name].Environment["PGHOST"]; host == nil || *host != "postgres" {
			t.Errorf("service %s reaches Postgres at %v, want it directly", name, deref(host))
		}
	}
	if err := checkDurable(c.Services["postgres"]); err != nil {
		t.Errorf("postgres: %v", err)
	}
}

// D-053: the bench stack is its own project, publishes nothing on the host, and
// every process of a run reads the one bench config file, so the FIFO and DRR
// runs differ only in the dispatcher's QUEUED_SCHEDULER (AC-16.1). Its Postgres
// stays durable, like the others.
func TestBenchStack(t *testing.T) {
	c := loadConfigFile(t, benchConfigFile)
	if c.Name != benchProject {
		t.Errorf("bench project name = %q, want %q", c.Name, benchProject)
	}
	for name, s := range c.Services {
		if len(s.Ports) > 0 {
			t.Errorf("service %s publishes %d ports in the bench stack", name, len(s.Ports))
		}
	}
	for _, name := range []string{"api", "dispatcher", "worker-1", "worker-2", "mockshop", "loadgen"} {
		if src := mountedAt(c.Services[name], "/etc/queued/shops.json"); src != "./bench/blackfriday.json" {
			t.Errorf("service %s reads %q as its config, want ./bench/blackfriday.json", name, src)
		}
	}
	if v := c.Services["dispatcher"].Environment["QUEUED_SCHEDULER"]; v == nil {
		t.Error("the dispatcher's QUEUED_SCHEDULER is not set from the environment, so a run can't choose the scheduler")
	}
	if err := checkDurable(c.Services["postgres"]); err != nil {
		t.Errorf("postgres: %v", err)
	}
}

// mountedAt returns the source mounted at target, or "" if nothing is.
func mountedAt(s service, target string) string {
	for _, v := range s.Volumes {
		if v.Target == target {
			return v.Source
		}
	}
	return ""
}

func deref(s *string) string {
	if s == nil {
		return "<unset>"
	}
	return *s
}

// D-033: every long-running service has a healthcheck; the one-shots don't
// need one, and the queued roles start only after migrate exits 0.
func TestHealthchecksAndOneShots(t *testing.T) { bothStacks(t, checkHealthchecks) }

func checkHealthchecks(t *testing.T, c config) {
	for name, s := range c.Services {
		if oneShots[name] {
			continue
		}
		hc := s.Healthcheck
		if hc == nil || hc.Disable || len(hc.Test) == 0 || hc.Test[0] == "NONE" {
			t.Errorf("service %s has no healthcheck", name)
		}
	}

	migrate, ok := c.Services["migrate"]
	if !ok {
		t.Fatal("no migrate service")
	}
	if migrate.Restart != "" && migrate.Restart != "no" {
		t.Errorf("migrate has restart %q; a one-shot must not restart", migrate.Restart)
	}
	if got := migrate.DependsOn["postgres"].Condition; got != "service_healthy" {
		t.Errorf("migrate waits for postgres with condition %q, want service_healthy", got)
	}
	for _, role := range queuedRoles {
		if got := c.Services[role].DependsOn["migrate"].Condition; got != "service_completed_successfully" {
			t.Errorf("service %s waits for migrate with condition %q, want service_completed_successfully", role, got)
		}
	}
}

// fsync and synchronous_commit must stay on: Q1 relies on a committed job
// surviving a crash (design §13).
var durabilityOff = regexp.MustCompile(`(?i)\b(fsync|synchronous_commit)\s*=\s*'?(off|false|no|0)\b`)

func checkDurable(s service) error {
	args := slices.Concat(s.Entrypoint, s.Command)
	for _, v := range s.Environment {
		if v != nil {
			args = append(args, *v)
		}
	}
	if m := durabilityOff.FindString(strings.Join(args, " ")); m != "" {
		return fmt.Errorf("turns off durability: %q", m)
	}
	return nil
}

// P1-5: Postgres doesn't turn off fsync or synchronous_commit in the Compose file.
func TestPostgresKeepsDurabilityInCompose(t *testing.T) {
	c := loadConfig(t)
	pg, ok := c.Services["postgres"]
	if !ok {
		t.Fatal("no postgres service")
	}
	if !strings.HasPrefix(pg.Image, "postgres:16.") {
		t.Errorf("postgres image %q is not the official postgres:16.x", pg.Image)
	}
	if err := checkDurable(pg); err != nil {
		t.Errorf("postgres: %v", err)
	}
}

func TestDurabilityCheckRejectsOverrides(t *testing.T) {
	off := "-c fsync=off"
	bad := []service{
		{Command: []string{"postgres", "-c", "fsync=off"}},
		{Command: []string{"postgres", "-c", "synchronous_commit=off"}},
		{Command: []string{"postgres", "-c", "synchronous_commit = 'off'"}},
		{Entrypoint: []string{"docker-entrypoint.sh", "postgres", "-c", "FSYNC=false"}},
		{Environment: map[string]*string{"PGOPTIONS": &off}},
	}
	for i, s := range bad {
		if checkDurable(s) == nil {
			t.Errorf("case %d: checkDurable = nil, want an error", i)
		}
	}
	if err := checkDurable(service{Command: []string{"postgres", "-c", "synchronous_commit=on", "-c", "fsync_off_note=1"}}); err != nil {
		t.Errorf("durable settings rejected: %v", err)
	}
}

// P1-5, checked on the running server too: whatever the config files say, the
// Postgres the stack runs has fsync and synchronous_commit on.
func TestPostgresKeepsDurabilityLive(t *testing.T) {
	conn := pgtest.ConnectAdmin(t)
	for _, setting := range []string{"fsync", "synchronous_commit"} {
		var value string
		if err := conn.QueryRow(t.Context(), "SELECT current_setting($1)", setting).Scan(&value); err != nil {
			t.Fatalf("read %s: %v", setting, err)
		}
		if value != "on" {
			t.Errorf("%s = %q, want on", setting, value)
		}
	}
}
