package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"
)

// DockerAPIVersion is the Docker Engine API version the harness speaks.
// Docker Engine 25 and later support it.
const DockerAPIVersion = "v1.44"

// The Compose labels that tie a container to its project and service.
const (
	labelProject = "com.docker.compose.project"
	labelService = "com.docker.compose.service"
)

// Docker is a minimal Docker Engine API client (D-025). It speaks HTTP over the
// Unix socket with net/http, with no Docker SDK (D-006). It acts only on the
// containers of one Compose project, and only on the services it was given
// (D-042), so the harness can never touch any other container on the machine.
type Docker struct {
	client  *http.Client
	base    string
	project string
	allowed []string
}

// NewDocker returns a client that dials the Docker socket at socket and acts
// only on services of project.
func NewDocker(socket, project string, services ...string) *Docker {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}
	return newDocker(&http.Client{Transport: tr}, "http://docker", project, services)
}

func newDocker(client *http.Client, baseURL, project string, services []string) *Docker {
	return &Docker{client: client, base: baseURL, project: project, allowed: slices.Clone(services)}
}

// Container is what the harness knows about one container.
type Container struct {
	ID       string
	Service  string
	Hostname string
	Status   string // created, running, paused, restarting, removing, exited or dead
}

// Inspect finds the one container of service in the project.
func (d *Docker) Inspect(ctx context.Context, service string) (Container, error) {
	if !slices.Contains(d.allowed, service) {
		return Container{}, fmt.Errorf("chaos: the harness may not touch service %q", service)
	}
	filters, err := json.Marshal(map[string][]string{
		"label": {labelProject + "=" + d.project, labelService + "=" + service},
	})
	if err != nil {
		return Container{}, err
	}
	var list []struct {
		ID string `json:"Id"`
	}
	if err := d.do(ctx, http.MethodGet, "/containers/json?all=1&filters="+url.QueryEscape(string(filters)), &list, http.StatusOK); err != nil {
		return Container{}, err
	}
	if len(list) != 1 {
		return Container{}, fmt.Errorf("chaos: found %d containers for service %s of project %s, want 1", len(list), service, d.project)
	}
	var info struct {
		ID     string `json:"Id"`
		Config struct {
			Hostname string
			Labels   map[string]string
		}
		State struct {
			Status string
		}
	}
	if err := d.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(list[0].ID)+"/json", &info, http.StatusOK); err != nil {
		return Container{}, err
	}
	// The filter matched these labels already. Check them again, so that no bug
	// can ever point the harness at another container.
	if info.Config.Labels[labelProject] != d.project || info.Config.Labels[labelService] != service {
		return Container{}, fmt.Errorf("chaos: container %s is not service %s of project %s", info.ID, service, d.project)
	}
	return Container{ID: info.ID, Service: service, Hostname: info.Config.Hostname, Status: info.State.Status}, nil
}

// Kill sends SIGKILL to the service's container, like kill -9.
func (d *Docker) Kill(ctx context.Context, service string) error {
	return d.act(ctx, service, "kill?signal=SIGKILL", http.StatusNoContent)
}

// Start starts the service's container. Starting a running container is not an error.
func (d *Docker) Start(ctx context.Context, service string) error {
	return d.act(ctx, service, "start", http.StatusNoContent, http.StatusNotModified)
}

// Restart stops the service's container with its stop signal, waiting up to
// stopTimeout before SIGKILL, then starts it again.
func (d *Docker) Restart(ctx context.Context, service string, stopTimeout time.Duration) error {
	return d.act(ctx, service, "restart?t="+strconv.Itoa(int(stopTimeout.Seconds())), http.StatusNoContent)
}

// Pause freezes every process in the service's container.
func (d *Docker) Pause(ctx context.Context, service string) error {
	return d.act(ctx, service, "pause", http.StatusNoContent)
}

// Unpause thaws the service's container.
func (d *Docker) Unpause(ctx context.Context, service string) error {
	return d.act(ctx, service, "unpause", http.StatusNoContent)
}

func (d *Docker) act(ctx context.Context, service, action string, ok ...int) error {
	c, err := d.Inspect(ctx, service)
	if err != nil {
		return err
	}
	return d.do(ctx, http.MethodPost, "/containers/"+url.PathEscape(c.ID)+"/"+action, nil, ok...)
}

func (d *Docker) do(ctx context.Context, method, path string, out any, ok ...int) error {
	req, err := http.NewRequestWithContext(ctx, method, d.base+"/"+DockerAPIVersion+path, nil)
	if err != nil {
		return err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("chaos: docker %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if !slices.Contains(ok, resp.StatusCode) {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("chaos: docker %s %s: %s: %s", method, path, resp.Status, msg)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("chaos: docker %s %s: decode: %w", method, path, err)
	}
	return nil
}
