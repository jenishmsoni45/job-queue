package chaos

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/queuetest"
)

// fakeDocker serves the slice of the Docker Engine API the harness uses, for
// containers keyed by ID.
type fakeDocker struct {
	mu         sync.Mutex
	containers map[string]map[string]string // ID → labels
	actions    []string                     // "POST <path>?<query>"
	status     int                          // answer to actions; 0 means 204
}

func (f *fakeDocker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path, ok := strings.CutPrefix(r.URL.Path, "/"+DockerAPIVersion)
	if !ok {
		http.Error(w, "unversioned path", http.StatusBadRequest)
		return
	}
	switch {
	case r.Method == http.MethodGet && path == "/containers/json":
		var filters map[string][]string
		if err := json.Unmarshal([]byte(r.URL.Query().Get("filters")), &filters); err != nil || r.URL.Query().Get("all") != "1" {
			http.Error(w, "bad filters", http.StatusBadRequest)
			return
		}
		var out []map[string]string
		for id, labels := range f.containers {
			match := true
			for _, l := range filters["label"] {
				k, v, _ := strings.Cut(l, "=")
				match = match && labels[k] == v
			}
			if match {
				out = append(out, map[string]string{"Id": id})
			}
		}
		_ = json.NewEncoder(w).Encode(out)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/json"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/json")
		labels, ok := f.containers[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Id":     id,
			"Config": map[string]any{"Hostname": "host-" + id, "Labels": labels},
			"State":  map[string]any{"Status": "running"},
		})
	case r.Method == http.MethodPost:
		f.actions = append(f.actions, "POST "+path+"?"+r.URL.RawQuery)
		status := f.status
		if status == 0 {
			status = http.StatusNoContent
		}
		w.WriteHeader(status)
	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

func labels(project, service string) map[string]string {
	return map[string]string{labelProject: project, labelService: service}
}

func TestDockerActsOnlyOnItsProjectAndServices(t *testing.T) {
	fake := &fakeDocker{containers: map[string]map[string]string{
		"aaa": labels(ComposeProject, Worker1),
		"bbb": labels("jobqueue", Worker1), // the `make up` stack's worker
		"ccc": labels(ComposeProject, "api"),
		"ddd": labels(ComposeProject, Postgres),
	}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	d := newDocker(srv.Client(), srv.URL, ComposeProject, Services)

	c, err := d.Inspect(t.Context(), Worker1)
	if err != nil || c.ID != "aaa" || c.Hostname != "host-aaa" || c.Status != "running" {
		t.Fatalf("Inspect(worker-1) = %+v, %v; want container aaa of the chaos project", c, err)
	}
	if err := d.Kill(t.Context(), Worker1); err != nil {
		t.Fatal(err)
	}
	if err := d.Restart(t.Context(), Postgres, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, act := range []func() error{
		func() error { return d.Pause(t.Context(), Worker1) },
		func() error { return d.Unpause(t.Context(), Worker1) },
		func() error { return d.Start(t.Context(), Worker1) },
	} {
		if err := act(); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{
		"POST /containers/aaa/kill?signal=SIGKILL",
		"POST /containers/ddd/restart?t=10",
		"POST /containers/aaa/pause?",
		"POST /containers/aaa/unpause?",
		"POST /containers/aaa/start?",
	}
	if !slices.Equal(fake.actions, want) {
		t.Errorf("actions = %q, want %q", fake.actions, want)
	}

	// D-042: the api is in the chaos project, but not a service the harness may touch.
	if err := d.Kill(t.Context(), "api"); err == nil {
		t.Error("the harness killed the api, which is not in its allowlist")
	}
	if len(fake.actions) != len(want) {
		t.Errorf("a refused action still reached Docker: %q", fake.actions)
	}
}

func TestDockerRefusesAmbiguousOrMislabelledContainers(t *testing.T) {
	fake := &fakeDocker{containers: map[string]map[string]string{
		"aaa": labels(ComposeProject, Worker2),
		"bbb": labels(ComposeProject, Worker2),
	}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	d := newDocker(srv.Client(), srv.URL, ComposeProject, Services)
	if _, err := d.Inspect(t.Context(), Worker2); err == nil {
		t.Error("two containers for one service were accepted")
	}
	if _, err := d.Inspect(t.Context(), Worker1); err == nil {
		t.Error("a missing service was accepted")
	}

	// A container that the list returns but whose own labels disagree is refused.
	liar := &fakeDocker{containers: map[string]map[string]string{"eee": labels(ComposeProject, Worker1)}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /"+DockerAPIVersion+"/containers/eee/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"Id": "eee", "Config": map[string]any{"Labels": labels("jobqueue", Worker1)}})
	})
	mux.Handle("/", liar)
	srv2 := httptest.NewServer(mux)
	defer srv2.Close()
	if _, err := newDocker(srv2.Client(), srv2.URL, ComposeProject, Services).Inspect(t.Context(), Worker1); err == nil {
		t.Error("a container labelled with another project was accepted")
	}
}

func TestDockerErrors(t *testing.T) {
	fake := &fakeDocker{containers: map[string]map[string]string{"aaa": labels(ComposeProject, Worker1)}, status: http.StatusInternalServerError}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	d := newDocker(srv.Client(), srv.URL, ComposeProject, Services)
	if err := d.Kill(t.Context(), Worker1); err == nil {
		t.Error("a 500 from Docker was not an error")
	}
	fake.status = http.StatusNotModified
	if err := d.Start(t.Context(), Worker1); err != nil {
		t.Errorf("starting a running container: %v, want no error", err)
	}
	if err := d.Pause(t.Context(), Worker1); err == nil {
		t.Error("304 to a pause was not an error")
	}
}

func TestToxiproxyClient(t *testing.T) {
	var mu sync.Mutex
	var got []string
	var toxic Toxic
	mux := http.NewServeMux()
	mux.HandleFunc("GET /proxies", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{ProxyW1: map[string]any{}, ProxyDisp: map[string]any{}})
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		got = append(got, "reset")
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /proxies/{proxy}/toxics", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, "add "+r.PathValue("proxy"))
		if err := json.NewDecoder(r.Body).Decode(&toxic); err != nil || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "bad toxic", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(toxic)
	})
	mux.HandleFunc("DELETE /proxies/{proxy}/toxics/{name}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, "remove "+r.PathValue("proxy")+" "+r.PathValue("name"))
		mu.Unlock()
		if r.PathValue("name") == "gone" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	tp := NewToxiproxy(srv.Client(), srv.URL)

	names, err := tp.Proxies(t.Context())
	if err != nil || !slices.Equal(names, []string{ProxyDisp, ProxyW1}) {
		t.Errorf("Proxies = %v, %v", names, err)
	}
	if err := tp.Reset(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := Toxic{Name: "fault-0-latency", Type: "latency", Stream: "downstream", Toxicity: 1,
		Attributes: map[string]int64{"latency": 700, "jitter": 50}}
	if err := tp.AddToxic(t.Context(), ProxyW1, want); err != nil {
		t.Fatal(err)
	}
	if !reflectEqualToxic(toxic, want) {
		t.Errorf("toxiproxy received %+v, want %+v", toxic, want)
	}
	if err := tp.RemoveToxic(t.Context(), ProxyW1, "fault-0-latency"); err != nil {
		t.Fatal(err)
	}
	if err := tp.RemoveToxic(t.Context(), ProxyW1, "gone"); err != nil {
		t.Errorf("removing a toxic that is already gone: %v", err)
	}
	wantCalls := []string{"reset", "add " + ProxyW1, "remove " + ProxyW1 + " fault-0-latency", "remove " + ProxyW1 + " gone"}
	if !slices.Equal(got, wantCalls) {
		t.Errorf("calls = %q, want %q", got, wantCalls)
	}
}

func reflectEqualToxic(a, b Toxic) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

// D-046: CHAOS_SEED alone replays one run; otherwise CHAOS_RUNS seeds count up
// from CHAOS_BASE_SEED, or from a fresh random base.
func TestSeeds(t *testing.T) {
	env := func(vars map[string]string) func(string) string { return func(k string) string { return vars[k] } }

	seeds, base, err := Seeds(env(map[string]string{"CHAOS_SEED": "77", "CHAOS_RUNS": "5"}), 100)
	if err != nil || !slices.Equal(seeds, []uint64{77}) || base != nil {
		t.Errorf("CHAOS_SEED: %v, %v, %v", seeds, base, err)
	}
	seeds, base, err = Seeds(env(map[string]string{"CHAOS_BASE_SEED": "1000", "CHAOS_RUNS": "3"}), 100)
	if err != nil || !slices.Equal(seeds, []uint64{1000, 1001, 1002}) || base == nil || *base != 1000 {
		t.Errorf("CHAOS_BASE_SEED: %v, %v, %v", seeds, base, err)
	}
	a, baseA, err := Seeds(env(nil), 100)
	if err != nil || len(a) != 100 || baseA == nil || a[0] != *baseA || a[99] != *baseA+99 {
		t.Fatalf("defaults: %d seeds from %v, %v", len(a), baseA, err)
	}
	_, baseB, errB := Seeds(env(nil), 1)
	_, baseC, errC := Seeds(env(nil), 1)
	if errB != nil || errC != nil || *baseA == *baseB && *baseB == *baseC {
		t.Errorf("three random base seeds were all %d (%v, %v)", *baseA, errB, errC)
	}
	for _, bad := range []map[string]string{{"CHAOS_SEED": "x"}, {"CHAOS_RUNS": "0"}, {"CHAOS_BASE_SEED": "-1"}} {
		if _, _, err := Seeds(env(bad), 100); err == nil {
			t.Errorf("%v was accepted", bad)
		}
	}
}

// The Q3 check catches what it claims to: overlapping leases, a gap in
// claim_seq, a first claim that isn't 1, and a lease that never ended.
func TestLeaseLogCheckCatchesOverlaps(t *testing.T) {
	at := func(s int) time.Time { return time.Unix(1_700_000_000+int64(s), 0) }
	ended := func(s int) *time.Time { e := at(s); return &e }
	good := []queuetest.LeaseRecord{
		{JobID: 1, Seq: 1, Owner: "a", Started: at(0), Ended: ended(2)},
		{JobID: 1, Seq: 2, Owner: "b", Started: at(2), Ended: ended(3)},
		{JobID: 2, Seq: 1, Owner: "a", Started: at(1), Ended: ended(5)},
	}
	if errs := queuetest.LeaseLogErrors(good, true); len(errs) != 0 {
		t.Errorf("clean leases: %v", errs)
	}
	for name, recs := range map[string][]queuetest.LeaseRecord{
		"overlap":     {{JobID: 1, Seq: 1, Started: at(0), Ended: ended(3)}, {JobID: 1, Seq: 2, Started: at(2), Ended: ended(4)}},
		"gap":         {{JobID: 1, Seq: 1, Started: at(0), Ended: ended(1)}, {JobID: 1, Seq: 3, Started: at(2), Ended: ended(4)}},
		"first not 1": {{JobID: 1, Seq: 2, Started: at(0), Ended: ended(1)}},
		"never ended": {{JobID: 1, Seq: 1, Started: at(0)}, {JobID: 1, Seq: 2, Started: at(2), Ended: ended(4)}},
		"last open":   {{JobID: 1, Seq: 1, Started: at(0)}},
	} {
		if errs := queuetest.LeaseLogErrors(recs, true); len(errs) == 0 {
			t.Errorf("%s: no error", name)
		}
	}
	if errs := queuetest.LeaseLogErrors([]queuetest.LeaseRecord{{JobID: 1, Seq: 1, Started: at(0)}}, false); len(errs) != 0 {
		t.Errorf("an open last lease of a running job is fine: %v", errs)
	}
}

// The proxy name in a toxic path is escaped, so it can't reach another route.
func TestToxiproxyEscapesNames(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.EscapedPath()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	_ = NewToxiproxy(srv.Client(), srv.URL).RemoveToxic(t.Context(), "a/b", "c d")
	if path != "/proxies/"+url.PathEscape("a/b")+"/toxics/"+url.PathEscape("c d") {
		t.Errorf("path = %q", path)
	}
}
