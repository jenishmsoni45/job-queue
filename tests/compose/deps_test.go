package compose

import (
	"bufio"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The only direct Go modules allowed (D-001, D-006). rapid arrives with the
// property tests in phase 2.
var allowedGoModules = map[string]bool{
	"github.com/jackc/pgx/v5": true,
	"pgregory.net/rapid":      true,
}

// The only direct npm packages allowed in console/ (D-001: React, Vite and
// TypeScript, with Vite's React plugin and React's type definitions).
var allowedNPMPackages = map[string]bool{
	"react":                true,
	"react-dom":            true,
	"vite":                 true,
	"@vitejs/plugin-react": true,
	"typescript":           true,
	"@types/react":         true,
	"@types/react-dom":     true,
}

// directGoRequires returns the module paths go.mod requires directly, that is,
// without an `// indirect` comment.
func directGoRequires(t *testing.T, goMod string) []string {
	t.Helper()
	f, err := os.Open(goMod) //nolint:gosec // a fixed path inside the repo
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	var direct []string
	inBlock := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "require (":
			inBlock = true
			continue
		case inBlock && line == ")":
			inBlock = false
			continue
		case strings.HasPrefix(line, "require "):
			line = strings.TrimSpace(strings.TrimPrefix(line, "require "))
		case !inBlock:
			continue
		}
		if line == "" || strings.HasPrefix(line, "//") || strings.Contains(line, "// indirect") {
			continue
		}
		direct = append(direct, strings.Fields(line)[0])
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return direct
}

// NFR-15: Go uses only the PLAN stack, pinned in go.sum. Phase 1 needs pgx only.
func TestGoModulesAreThePlanStack(t *testing.T) {
	direct := directGoRequires(t, filepath.Join(repoRoot, "go.mod"))
	if !slices.Contains(direct, "github.com/jackc/pgx/v5") {
		t.Errorf("go.mod does not require pgx; direct requires: %v", direct)
	}
	for _, m := range direct {
		if !allowedGoModules[m] {
			t.Errorf("go.mod requires %s, which is outside the PLAN stack (D-001, D-006)", m)
		}
	}
	if info, err := os.Stat(filepath.Join(repoRoot, "go.sum")); err != nil || info.Size() == 0 {
		t.Errorf("go.sum is missing or empty: %v", err)
	}
}

var exactVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

// NFR-15 and P1-6: the console uses only the PLAN stack, every package is pinned
// to an exact version, the lockfile matches package.json, and install scripts
// are off.
func TestConsoleDependencies(t *testing.T) {
	console := filepath.Join(repoRoot, "console")

	var pkg struct {
		Dependencies    map[string]string `json:"dependencies"`
		DevDependencies map[string]string `json:"devDependencies"`
	}
	readJSON(t, filepath.Join(console, "package.json"), &pkg)
	all := map[string]string{}
	maps.Copy(all, pkg.Dependencies)
	maps.Copy(all, pkg.DevDependencies)
	if len(all) == 0 {
		t.Fatal("console/package.json lists no packages")
	}
	for name, version := range all {
		if !allowedNPMPackages[name] {
			t.Errorf("console depends on %s, which is outside the PLAN stack (D-001)", name)
		}
		if !exactVersion.MatchString(version) {
			t.Errorf("console pins %s to %q, want an exact version such as 1.2.3", name, version)
		}
	}

	var lock struct {
		LockfileVersion int `json:"lockfileVersion"`
		Packages        map[string]struct {
			Dependencies    map[string]string `json:"dependencies"`
			DevDependencies map[string]string `json:"devDependencies"`
		} `json:"packages"`
	}
	readJSON(t, filepath.Join(console, "package-lock.json"), &lock)
	if lock.LockfileVersion < 3 {
		t.Errorf("package-lock.json lockfileVersion = %d, want 3 or later", lock.LockfileVersion)
	}
	root := lock.Packages[""]
	if !maps.Equal(root.Dependencies, pkg.Dependencies) || !maps.Equal(root.DevDependencies, pkg.DevDependencies) {
		t.Error("package-lock.json is out of date with package.json; run npm install in console/")
	}

	if !npmrcIgnoresScripts(t, filepath.Join(console, ".npmrc")) {
		t.Error("console/.npmrc must set ignore-scripts=true")
	}
}

// npmrcIgnoresScripts reports whether the last ignore-scripts setting in the
// file is true.
func npmrcIgnoresScripts(t *testing.T, path string) bool {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // a fixed path inside the repo
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	ignore := false
	for line := range strings.Lines(string(data)) {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok && strings.TrimSpace(key) == "ignore-scripts" {
			ignore = strings.TrimSpace(value) == "true"
		}
	}
	return ignore
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // a fixed path inside the repo
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
}

func TestNpmrcParsing(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]bool{
		"ignore-scripts=true\n":                       true,
		"save-exact=true\nignore-scripts = true\n":    true,
		"# ignore-scripts=true\n":                     false,
		"ignore-scripts=true\nignore-scripts=false\n": false,
		"":                     false,
		"ignore-scripts=yes\n": false,
		"; comment\nsave-exact=true\nignore-scripts=1\n": false,
	}
	i := 0
	for content, want := range cases {
		path := filepath.Join(dir, strings.Repeat("x", i+1))
		i++
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := npmrcIgnoresScripts(t, path); got != want {
			t.Errorf("npmrc %q: ignores scripts = %v, want %v", content, got, want)
		}
	}
}

func TestDirectGoRequiresParsing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go.mod")
	content := `module example.com/m

go 1.27.3

require example.com/single v1.0.0

require (
	// a comment
	example.com/a v1.2.3
	example.com/b v0.1.0 // indirect
)
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	got := directGoRequires(t, path)
	want := []string{"example.com/single", "example.com/a"}
	if !slices.Equal(got, want) {
		t.Fatalf("directGoRequires = %v, want %v", got, want)
	}
}
