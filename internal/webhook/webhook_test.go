package webhook_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

// Sign is base64(HMAC-SHA256(secret, body)), computed independently here.
func TestSignMatchesHMACSHA256(t *testing.T) {
	secret, body := []byte("local-dummy-webhook-secret"), []byte(`{"id":1001}`)
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	want := base64.StdEncoding.EncodeToString(m.Sum(nil))
	if got := webhook.Sign(secret, body); got != want {
		t.Fatalf("Sign = %q, want %q", got, want)
	}
}

// AC-8.1: only the signature of these exact bytes under this secret verifies.
func TestVerify(t *testing.T) {
	secret, body := []byte("secret"), []byte(`{"id":1001}`)
	good := webhook.Sign(secret, body)
	cases := map[string]struct {
		secret, body []byte
		header       string
		want         bool
	}{
		"valid":            {secret, body, good, true},
		"missing header":   {secret, body, "", false},
		"wrong signature":  {secret, body, webhook.Sign(secret, []byte("other")), false},
		"tampered body":    {secret, []byte(`{"id":1002}`), good, false},
		"one byte added":   {secret, append(append([]byte{}, body...), ' '), good, false},
		"wrong secret":     {[]byte("other-secret"), body, good, false},
		"empty secret":     {nil, body, webhook.Sign(nil, body), false},
		"not base64":       {secret, body, "%%%not-base64%%%", false},
		"truncated MAC":    {secret, body, good[:len(good)-4], false},
		"hex, not base64":  {secret, body, "6162636465666768", false},
		"padding stripped": {secret, body, base64.RawStdEncoding.EncodeToString(mustDecode(t, good)), false},
	}
	for name, c := range cases {
		if got := webhook.Verify(c.secret, c.body, c.header); got != c.want {
			t.Errorf("%s: Verify = %v, want %v", name, got, c.want)
		}
	}
}

func mustDecode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// P5-3: Verify compares MACs with hmac.Equal, which takes the same time
// whatever the bytes are, and never with bytes.Equal, ==, or a string compare,
// which stop at the first differing byte and leak how much of a guess was right.
// The receiver calls Verify on the raw body before parsing it (internal/api).
func TestVerifyComparesWithHMACEqual(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "webhook.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var verify *ast.FuncDecl
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == "Verify" {
			verify = fn
		}
	}
	if verify == nil {
		t.Fatal("no Verify function")
	}
	usesHMACEqual := false
	ast.Inspect(verify.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.CallExpr:
			if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok {
					switch pkg.Name + "." + sel.Sel.Name {
					case "hmac.Equal":
						usesHMACEqual = true
					case "bytes.Equal", "bytes.Compare", "strings.Compare", "strings.EqualFold", "reflect.DeepEqual":
						t.Errorf("%s: Verify compares with %s.%s", fset.Position(n.Pos()), pkg.Name, sel.Sel.Name)
					}
				}
			}
		case *ast.BinaryExpr:
			if n.Op == token.EQL || n.Op == token.NEQ {
				// Only length and nil checks are allowed; a MAC or header is never compared with ==.
				if !isLenOrNilOrEmpty(n.X) && !isLenOrNilOrEmpty(n.Y) {
					t.Errorf("%s: Verify compares with %s", fset.Position(n.Pos()), n.Op)
				}
			}
		}
		return true
	})
	if !usesHMACEqual {
		t.Error("Verify does not call hmac.Equal")
	}
}

func isLenOrNilOrEmpty(e ast.Expr) bool {
	switch e := e.(type) {
	case *ast.Ident:
		return e.Name == "nil"
	case *ast.BasicLit:
		return e.Value == `""` || e.Value == "0"
	case *ast.CallExpr:
		id, ok := e.Fun.(*ast.Ident)
		return ok && id.Name == "len"
	}
	return false
}
