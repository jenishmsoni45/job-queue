package chaos

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// slowStartDocker is a fakeDocker on which starting a container takes a while,
// and which refuses to kill a container that isn't running, as Docker does.
type slowStartDocker struct {
	*fakeDocker
	delay   time.Duration
	mu      sync.Mutex
	stopped map[string]bool // container ID → killed and not yet started again
}

func (s *slowStartDocker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/"+DockerAPIVersion+"/containers/")
	id, action, _ := strings.Cut(path, "/")
	if r.Method == http.MethodPost {
		switch action {
		case "start":
			time.Sleep(s.delay)
			s.mu.Lock()
			delete(s.stopped, id)
			s.mu.Unlock()
		case "kill":
			s.mu.Lock()
			stopped := s.stopped[id]
			s.stopped[id] = true
			s.mu.Unlock()
			if stopped {
				http.Error(w, `{"message":"cannot kill container: `+id+` is not running"}`, http.StatusConflict)
				return
			}
		}
	}
	s.fakeDocker.ServeHTTP(w, r)
}

// A fault waits until the one before it on the same target has been undone,
// so a slow Docker start can't leave the next kill a worker that isn't running
// yet (seed 78833456). Faults on other targets don't wait.
func TestAFaultWaitsForTheFaultBeforeItOnItsTarget(t *testing.T) {
	fake := &slowStartDocker{
		fakeDocker: &fakeDocker{containers: map[string]map[string]string{
			"aaa": labels(ComposeProject, Worker1),
			"bbb": labels(ComposeProject, Worker2),
		}},
		delay:   300 * time.Millisecond,
		stopped: map[string]bool{},
	}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	e := &Env{Docker: newDocker(srv.Client(), srv.URL, ComposeProject, Services)}

	// Planned apart, but the first kill's start ends at about 400 ms, after the
	// second kill is due at 150 ms.
	faults := []Fault{
		{Kind: KillWorker, Target: Worker1, AtMS: 0, DurationMS: 100},
		{Kind: PauseWorker, Target: Worker2, AtMS: 150, DurationMS: 100},
		{Kind: KillWorker, Target: Worker1, AtMS: 150, DurationMS: 100},
	}
	results := e.injectAll(t.Context(), time.Now(), faults)()

	for _, r := range results {
		if r.Error != "" {
			t.Errorf("%s on %s at %d ms: %s", r.Kind, r.Target, r.AtMS, r.Error)
		}
	}
	first, pause, second := results[0], results[1], results[2]
	if second.AppliedMS < first.RevertedMS {
		t.Errorf("the second kill of worker-1 was applied at %d ms, before the first was undone at %d ms", second.AppliedMS, first.RevertedMS)
	}
	if pause.AppliedMS >= first.RevertedMS {
		t.Errorf("the pause of worker-2 waited for worker-1: applied at %d ms, worker-1's first kill undone at %d ms", pause.AppliedMS, first.RevertedMS)
	}

	fake.fakeDocker.mu.Lock()
	var worker1 []string
	for _, a := range fake.actions {
		if strings.Contains(a, "/aaa/") {
			worker1 = append(worker1, a)
		}
	}
	fake.fakeDocker.mu.Unlock()
	want := []string{
		"POST /containers/aaa/kill?signal=SIGKILL",
		"POST /containers/aaa/start?",
		"POST /containers/aaa/kill?signal=SIGKILL",
		"POST /containers/aaa/start?",
	}
	if !slices.Equal(worker1, want) {
		t.Errorf("worker-1 actions = %q, want %q", worker1, want)
	}
}
