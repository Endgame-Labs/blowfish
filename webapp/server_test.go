package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAPI is an in-memory stand-in for blowfish's admin API.
type fakeAPI struct {
	mu       sync.Mutex
	projects map[string]Project
	keys     map[string]Key // by key id
	nsCount  map[string]int // namespaces per project
	calls    []string
	auths    map[string]bool
	bodies   []map[string]any
	nextKey  int
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{
		projects: map[string]Project{"default": {ID: "default", Name: "Default"}},
		keys: map[string]Key{"00000000000000000000000000000001": {
			ID: "00000000000000000000000000000001", Name: "webapp", Kind: "superadmin", CreatedBy: "super", CreatedAt: time.Now()}},
		nsCount: map[string]int{}, auths: map[string]bool{},
	}
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auths[r.Header.Get("Authorization")] = true
	f.calls = append(f.calls, r.Method+" "+r.URL.RequestURI())
	reply := func(code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	fail := func(code int, msg string) { reply(code, map[string]string{"status": "error", "error": msg}) }
	var body map[string]any
	if r.Body != nil {
		raw, _ := io.ReadAll(r.Body)
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
			f.bodies = append(f.bodies, body)
		}
	}
	if r.Header.Get("Authorization") != "Bearer test-super" {
		fail(401, "missing or invalid API key")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/_blowfish/v1/"), "/"), "/")
	mint := func(project, kind string) {
		f.nextKey++
		id := strings.Repeat("a", 31) + string(rune('0'+f.nextKey))
		by, _ := body["created_by"].(string)
		k := Key{ID: id, Name: body["name"].(string), Kind: kind, Project: project, CreatedBy: by, CreatedAt: time.Now()}
		f.keys[id] = k
		reply(201, map[string]any{"key": k, "token": "bf_" + id + "_" + strings.Repeat("e", 64)})
	}
	listKeys := func(match func(Key) bool) {
		out := []Key{}
		for _, k := range f.keys {
			if match(k) {
				out = append(out, k)
			}
		}
		reply(200, map[string]any{"keys": out})
	}
	switch {
	case r.Method == "GET" && parts[0] == "whoami":
		reply(200, map[string]any{"kind": "superadmin", "key_id": "00000000000000000000000000000001"})
	case parts[0] == "projects" && len(parts) == 1 && r.Method == "GET":
		out := []Project{f.projects["default"]}
		for id, p := range f.projects {
			if id != "default" {
				out = append(out, p)
			}
		}
		reply(200, map[string]any{"projects": out})
	case parts[0] == "projects" && len(parts) == 1 && r.Method == "POST":
		id := body["id"].(string)
		if _, ok := f.projects[id]; ok {
			fail(409, "project already exists")
			return
		}
		p := Project{ID: id, Name: body["name"].(string), CreatedBy: body["created_by"].(string), CreatedAt: time.Now()}
		f.projects[id] = p
		reply(201, p)
	case parts[0] == "projects" && len(parts) == 2:
		p, ok := f.projects[parts[1]]
		if !ok {
			fail(404, "project not found")
			return
		}
		switch r.Method {
		case "GET":
			reply(200, p)
		case "PATCH":
			p.Name = body["name"].(string)
			f.projects[p.ID] = p
			reply(200, p)
		case "DELETE":
			if f.nsCount[p.ID] > 0 && r.URL.Query().Get("force") != "true" {
				fail(409, "project has namespaces")
				return
			}
			delete(f.projects, p.ID)
			reply(200, map[string]string{"status": "OK"})
		}
	case parts[0] == "projects" && len(parts) == 3 && parts[2] == "keys" && r.Method == "GET":
		listKeys(func(k Key) bool { return k.Project == parts[1] })
	case parts[0] == "projects" && len(parts) == 3 && parts[2] == "keys" && r.Method == "POST":
		mint(parts[1], body["kind"].(string))
	case parts[0] == "projects" && len(parts) == 4 && r.Method == "DELETE":
		if k, ok := f.keys[parts[3]]; !ok || k.Project != parts[1] {
			fail(404, "key not found")
			return
		}
		delete(f.keys, parts[3])
		reply(200, map[string]string{"status": "OK"})
	case parts[0] == "org" && len(parts) == 2 && r.Method == "GET":
		listKeys(func(k Key) bool { return k.Kind == "superadmin" })
	case parts[0] == "org" && len(parts) == 2 && r.Method == "POST":
		mint("", "superadmin")
	case parts[0] == "org" && len(parts) == 3 && r.Method == "DELETE":
		delete(f.keys, parts[2])
		reply(200, map[string]string{"status": "OK"})
	default:
		fail(404, "unknown route")
	}
}

type mailbox struct {
	mu   sync.Mutex
	sent []string
	to   []string
}

func (m *mailbox) Send(_ context.Context, to, _, body string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.to = append(m.to, to)
	m.sent = append(m.sent, body)
	return nil
}

func (m *mailbox) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sent)
}

// waitMail waits for the background send.
func (m *mailbox) wait(t *testing.T, n int) string {
	t.Helper()
	for i := 0; i < 200; i++ {
		if m.count() >= n {
			m.mu.Lock()
			defer m.mu.Unlock()
			return m.sent[n-1]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %d emails, got %d", n, m.count())
	return ""
}

type testEnv struct {
	t    *testing.T
	api  *fakeAPI
	mail *mailbox
	srv  *Server
	ts   *httptest.Server
	now  time.Time
	mu   sync.Mutex
}

func newEnv(t *testing.T, baseSuffix string) *testEnv {
	e := &testEnv{t: t, api: newFakeAPI(), mail: &mailbox{}, now: time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)}
	fake := httptest.NewServer(e.api)
	t.Cleanup(fake.Close)
	cfg := &Config{
		BaseURL: "http://console.test" + baseSuffix, APIURL: fake.URL, APIKey: "test-super",
		Admins: []string{"alice@example.com"}, SessionSecret: []byte(strings.Repeat("s", 40)), Mailer: "log",
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	e.srv = NewServer(cfg, newAPI(fake.URL, "test-super"), e.mail)
	e.srv.now = func() time.Time { e.mu.Lock(); defer e.mu.Unlock(); return e.now }
	e.ts = httptest.NewServer(e.srv.Handler())
	t.Cleanup(e.ts.Close)
	return e
}

func (e *testEnv) advance(d time.Duration) { e.mu.Lock(); e.now = e.now.Add(d); e.mu.Unlock() }

type browser struct {
	e      *testEnv
	c      *http.Client
	prefix string
}

func (e *testEnv) browser() *browser {
	jar, _ := cookiejar.New(nil)
	return &browser{e: e, prefix: e.srv.prefix, c: &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (b *browser) do(method, path string, form url.Values) (int, string, http.Header) {
	b.e.t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, _ := http.NewRequest(method, b.e.ts.URL+path, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := b.c.Do(req)
	if err != nil {
		b.e.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data), resp.Header
}

var (
	reForm  = regexp.MustCompile(`name="form" value="([^"]+)"`)
	reCSRF  = regexp.MustCompile(`name="csrf" value="([^"]+)"`)
	reToken = regexp.MustCompile(`token=([A-Za-z0-9_-]+)`)
	reKey   = regexp.MustCompile(`id="token">([^<]+)<`)
)

func (b *browser) requestLink(email string) (int, string) {
	_, page, _ := b.do("GET", b.prefix+"/login", nil)
	m := reForm.FindStringSubmatch(page)
	if m == nil {
		b.e.t.Fatalf("no login form token in %s", page)
	}
	code, body, _ := b.do("POST", b.prefix+"/login", url.Values{"email": {email}, "form": {m[1]}})
	return code, body
}

func (b *browser) login() {
	b.e.t.Helper()
	n := b.e.mail.count()
	if code, _ := b.requestLink("alice@example.com"); code != 200 {
		b.e.t.Fatalf("login request: %d", code)
	}
	mail := b.e.mail.wait(b.e.t, n+1)
	token := reToken.FindStringSubmatch(mail)[1]
	b.verify(token, 303)
}

func (b *browser) verify(token string, want int) string {
	b.e.t.Helper()
	code, page, _ := b.do("GET", b.prefix+"/login/verify?token="+token, nil)
	if want == 303 && code != 200 {
		b.e.t.Fatalf("verify page: %d %s", code, page)
	}
	code, body, _ := b.do("POST", b.prefix+"/login/verify", url.Values{"token": {token}})
	if code != want {
		b.e.t.Fatalf("verify post: got %d want %d: %s", code, want, body)
	}
	return body
}

func (b *browser) csrf(path string) string {
	b.e.t.Helper()
	code, page, _ := b.do("GET", path, nil)
	m := reCSRF.FindStringSubmatch(page)
	if code != 200 || m == nil {
		b.e.t.Fatalf("no csrf at %s: %d", path, code)
	}
	return m[1]
}

func TestMagicLinkLoginAndAllowlist(t *testing.T) {
	e := newEnv(t, "")
	b := e.browser()
	// Signed out: everything redirects to the sign-in page.
	for _, p := range []string{"/", "/projects", "/projects/x", "/org/keys"} {
		code, _, h := b.do("GET", p, nil)
		if code != 303 || !strings.HasSuffix(h.Get("Location"), "/login") && p != "/" {
			t.Fatalf("%s: %d %v", p, code, h)
		}
	}
	// The response is identical for an allowed and a non-allowed address, and only the
	// allowed one gets mail.
	codeA, bodyA := b.requestLink("alice@example.com")
	codeB, bodyB := b.requestLink("mallory@example.com")
	_, bodyC := b.requestLink("not an email")
	nonce := regexp.MustCompile(`nonce="[^"]+"`)
	norm := func(s string) string { return nonce.ReplaceAllString(s, "") }
	if codeA != 200 || codeA != codeB || norm(bodyA) != norm(bodyB) || norm(bodyA) != norm(bodyC) {
		t.Fatalf("responses differ: %d %d", codeA, codeB)
	}
	mail := e.mail.wait(t, 1)
	time.Sleep(50 * time.Millisecond)
	if e.mail.count() != 1 || e.mail.to[0] != "alice@example.com" {
		t.Fatalf("mail sent to %v", e.mail.to)
	}
	token := reToken.FindStringSubmatch(mail)[1]
	if !strings.Contains(mail, "http://console.test/login/verify?token=") {
		t.Fatal(mail)
	}
	// A bad token, a forged login form, and mixed-case addresses.
	b.verify("nope", 400)
	if code, _, _ := b.do("POST", "/login", url.Values{"email": {"alice@example.com"}, "form": {"1.x"}}); code != 400 {
		t.Fatalf("forged login form: %d", code)
	}
	// GET alone doesn't use the link: scanners can fetch it safely.
	if code, _, _ := b.do("GET", "/login/verify?token="+token, nil); code != 200 {
		t.Fatal(code)
	}
	if code, _, _ := b.do("GET", "/projects", nil); code != 303 {
		t.Fatalf("signed in by a GET: %d", code)
	}
	b.verify(token, 303)
	if code, _, _ := b.do("GET", "/projects", nil); code != 200 {
		t.Fatalf("not signed in: %d", code)
	}
	// Single use.
	b2 := e.browser()
	b2.verify(token, 400)
	// Uppercase input still matches the allowlist.
	b3 := e.browser()
	n := e.mail.count()
	b3.requestLink("  ALICE@Example.com ")
	e.mail.wait(t, n+1)
}

func TestLinkExpiresAfterFifteenMinutes(t *testing.T) {
	e := newEnv(t, "")
	b := e.browser()
	b.requestLink("alice@example.com")
	token := reToken.FindStringSubmatch(e.mail.wait(t, 1))[1]
	e.advance(16 * time.Minute)
	b.verify(token, 400)
	// A link used within its window works.
	b.requestLink("alice@example.com")
	token = reToken.FindStringSubmatch(e.mail.wait(t, 2))[1]
	e.advance(14 * time.Minute)
	b.verify(token, 303)
}

func TestSignInRateLimits(t *testing.T) {
	e := newEnv(t, "")
	b := e.browser()
	// Per email: the sixth request in the window is refused, allowed address or not.
	for i := 0; i < 5; i++ {
		if code, _ := b.requestLink("alice@example.com"); code != 200 {
			t.Fatalf("request %d: %d", i, code)
		}
	}
	if code, _ := b.requestLink("alice@example.com"); code != 429 {
		t.Fatalf("per-email limit: %d", code)
	}
	if code, _ := b.requestLink("zed@example.com"); code != 200 {
		t.Fatalf("other email blocked: %d", code)
	}
	for i := 0; i < 5; i++ {
		b.requestLink("zed" + string(rune('a'+i)) + "@example.com")
	}
	// Per IP: ten attempts so far from this address, so the next is refused for any email.
	if code, _ := b.requestLink("fresh@example.com"); code != 429 {
		t.Fatalf("per-IP limit: %d", code)
	}
	// The window slides.
	e.advance(16 * time.Minute)
	if code, _ := b.requestLink("alice@example.com"); code != 200 {
		t.Fatalf("after the window: %d", code)
	}
}

func TestSessionsCSRFAndExpiry(t *testing.T) {
	e := newEnv(t, "")
	b := e.browser()
	b.login()
	// A POST without the CSRF token, or with another session's, is refused.
	if code, _, _ := b.do("POST", "/projects", url.Values{"id": {"acme"}, "name": {"Acme"}}); code != 403 {
		t.Fatalf("no csrf: %d", code)
	}
	if code, _, _ := b.do("POST", "/projects", url.Values{"id": {"acme"}, "name": {"Acme"}, "csrf": {"forged"}}); code != 403 {
		t.Fatalf("bad csrf: %d", code)
	}
	other := e.browser()
	other.login()
	foreign := other.csrf("/projects")
	calls := len(e.api.calls)
	if code, _, _ := b.do("POST", "/projects", url.Values{"id": {"acme"}, "name": {"Acme"}, "csrf": {foreign}}); code != 403 {
		t.Fatalf("another session's csrf: %d", code)
	}
	if len(e.api.calls) != calls {
		t.Fatalf("API reached despite CSRF failures: %v", e.api.calls)
	}
	// A tampered cookie is rejected.
	u, _ := url.Parse(e.ts.URL)
	cookies := b.c.Jar.Cookies(u)
	forged := e.browser()
	forged.c.Jar.SetCookies(u, []*http.Cookie{{Name: sessionCookie, Value: "e30." + strings.Repeat("A", 43), Path: "/"}})
	if code, _, _ := forged.do("GET", "/projects", nil); code != 303 {
		t.Fatalf("forged cookie: %d", code)
	}
	if len(cookies) == 0 {
		t.Fatal("no cookie")
	}
	// Sessions last 12 hours.
	e.advance(11 * time.Hour)
	if code, _, _ := b.do("GET", "/projects", nil); code != 200 {
		t.Fatalf("session ended early: %d", code)
	}
	e.advance(2 * time.Hour)
	if code, _, _ := b.do("GET", "/projects", nil); code != 303 {
		t.Fatalf("session outlived 12h: %d", code)
	}
	// A session for an address removed from the allowlist stops working.
	c := e.browser()
	c.login()
	delete(e.srv.admins, "alice@example.com")
	if code, _, _ := c.do("GET", "/projects", nil); code != 303 {
		t.Fatalf("removed admin: %d", code)
	}
}

func TestCookieFlagsAndHeaders(t *testing.T) {
	e := newEnv(t, "")
	e.srv.cfg.BaseURL = "https://console.example.com/console"
	b := e.browser()
	b.requestLink("alice@example.com")
	// Secure cookies for an https base URL: log in directly against the handler.
	token := e.srv.links.issue("alice@example.com", e.now)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/login/verify", strings.NewReader(url.Values{"token": {token}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	e.srv.Handler().ServeHTTP(rec, req)
	set := rec.Header().Get("Set-Cookie")
	for _, want := range []string{sessionCookie + "=", "HttpOnly", "Secure", "SameSite=Lax", "Max-Age=43200"} {
		if !strings.Contains(set, want) {
			t.Fatalf("cookie %q lacks %q", set, want)
		}
	}
	h := rec.Header()
	if !strings.Contains(h.Get("Content-Security-Policy"), "frame-ancestors 'none'") || h.Get("Referrer-Policy") != "no-referrer" || h.Get("Cache-Control") != "no-store" {
		t.Fatalf("headers: %v", h)
	}
}

func TestProjectsAndKeysThroughTheUI(t *testing.T) {
	e := newEnv(t, "")
	b := e.browser()
	b.login()
	csrf := b.csrf("/projects")
	// Create a project.
	code, _, h := b.do("POST", "/projects", url.Values{"csrf": {csrf}, "id": {"acme"}, "name": {"Acme Search"}})
	if code != 303 || h.Get("Location") != "/projects/acme" {
		t.Fatalf("create: %d %v", code, h)
	}
	if got := e.api.projects["acme"].CreatedBy; got != "webapp:alice@example.com" {
		t.Fatalf("created_by = %q", got)
	}
	_, page, _ := b.do("GET", "/projects", nil)
	if !strings.Contains(page, "Acme Search") || !strings.Contains(page, "Default") {
		t.Fatalf("list missing projects: %s", page)
	}
	// A bad ID never reaches blowfish, and a duplicate shows blowfish's error.
	calls := len(e.api.calls)
	b.do("POST", "/projects", url.Values{"csrf": {csrf}, "id": {"Bad ID"}, "name": {"x"}})
	if len(e.api.calls) != calls {
		t.Fatal("invalid ID reached the API")
	}
	b.do("POST", "/projects", url.Values{"csrf": {csrf}, "id": {"acme"}, "name": {"Again"}})
	_, page, _ = b.do("GET", "/projects", nil)
	if !strings.Contains(page, "Project already exists") {
		t.Fatalf("duplicate error not shown: %s", page)
	}
	// Create a key: the token appears once, on the response to the POST.
	code, page, _ = b.do("POST", "/projects/acme/keys", url.Values{"csrf": {csrf}, "name": {"prod"}, "kind": {"read_write"}, "expires": {"30"}})
	m := reKey.FindStringSubmatch(page)
	if code != 200 || m == nil || !strings.HasPrefix(m[1], "bf_") || !strings.Contains(page, "id=\"copy\"") {
		t.Fatalf("key page: %d %s", code, page)
	}
	last := e.api.bodies[len(e.api.bodies)-1]
	if last["created_by"] != "webapp:alice@example.com" || last["kind"] != "read_write" || last["expires_at"] == nil {
		t.Fatalf("create body: %v", last)
	}
	token := m[1]
	_, page, _ = b.do("GET", "/projects/acme", nil)
	if strings.Contains(page, token) || !strings.Contains(page, "prod") || !strings.Contains(page, "read_write") {
		t.Fatalf("project page: %s", page)
	}
	// Invalid kinds are refused before the API.
	calls = len(e.api.calls)
	b.do("POST", "/projects/acme/keys", url.Values{"csrf": {csrf}, "name": {"x"}, "kind": {"superadmin"}})
	b.do("POST", "/projects/acme/keys", url.Values{"csrf": {csrf}, "name": {""}, "kind": {"read"}})
	if len(e.api.calls) != calls {
		t.Fatal("invalid key request reached the API")
	}
	// Revoke, with a confirmation step.
	var keyID string
	for id, k := range e.api.keys {
		if k.Project == "acme" {
			keyID = id
		}
	}
	code, page, _ = b.do("GET", "/projects/acme/keys/"+keyID+"/revoke", nil)
	if code != 200 || !strings.Contains(page, "Revoke this key?") {
		t.Fatalf("confirm page: %d", code)
	}
	if _, ok := e.api.keys[keyID]; !ok {
		t.Fatal("GET revoked the key")
	}
	if code, _, _ = b.do("POST", "/projects/acme/keys/"+keyID+"/revoke", url.Values{"csrf": {csrf}}); code != 303 {
		t.Fatalf("revoke: %d", code)
	}
	if _, ok := e.api.keys[keyID]; ok {
		t.Fatal("key not revoked")
	}
	if code, _, _ = b.do("GET", "/projects/acme/keys/not-a-key/revoke", nil); code != 404 {
		t.Fatalf("bad key id: %d", code)
	}
	// Rename.
	b.do("POST", "/projects/acme/rename", url.Values{"csrf": {csrf}, "name": {"Acme, Inc."}})
	if e.api.projects["acme"].Name != "Acme, Inc." {
		t.Fatal("not renamed")
	}
	// Delete needs the typed ID, and namespaces need the force box.
	e.api.nsCount["acme"] = 2
	b.do("POST", "/projects/acme/delete", url.Values{"csrf": {csrf}, "confirm": {"nope"}})
	if _, ok := e.api.projects["acme"]; !ok {
		t.Fatal("deleted without confirmation")
	}
	b.do("POST", "/projects/acme/delete", url.Values{"csrf": {csrf}, "confirm": {"acme"}})
	if _, ok := e.api.projects["acme"]; !ok {
		t.Fatal("deleted despite namespaces")
	}
	_, page, _ = b.do("GET", "/projects/acme/delete", nil)
	if !strings.Contains(page, "still has namespaces") {
		t.Fatalf("no namespaces warning: %s", page)
	}
	b.do("POST", "/projects/acme/delete", url.Values{"csrf": {csrf}, "confirm": {"acme"}, "force": {"1"}})
	if _, ok := e.api.projects["acme"]; ok {
		t.Fatal("not deleted")
	}
	// The default project has no delete or rename.
	_, page, _ = b.do("GET", "/projects/default", nil)
	if strings.Contains(page, "Delete project") || strings.Contains(page, "Rename") {
		t.Fatal("default project shows destructive actions")
	}
	if code, _, _ = b.do("GET", "/projects/no-such", nil); code != 404 {
		t.Fatalf("missing project: %d", code)
	}
	if code, _, _ = b.do("GET", "/projects/..%2fx", nil); code != 404 {
		t.Fatalf("path injection: %d", code)
	}
	// Every API call used the webapp's key, never a browser-supplied one.
	for auth := range e.api.auths {
		if auth != "Bearer test-super" {
			t.Fatalf("unexpected auth %q", auth)
		}
	}
}

func TestOrgKeysPage(t *testing.T) {
	e := newEnv(t, "")
	b := e.browser()
	b.login()
	csrf := b.csrf("/org/keys")
	_, page, _ := b.do("GET", "/org/keys", nil)
	if !strings.Contains(page, "this webapp's key") {
		t.Fatalf("own key not marked: %s", page)
	}
	code, page, _ := b.do("POST", "/org/keys", url.Values{"csrf": {csrf}, "name": {"ops"}, "expires": {"90"}})
	if code != 200 || reKey.FindStringSubmatch(page) == nil {
		t.Fatalf("create: %d %s", code, page)
	}
	var id string
	for k, v := range e.api.keys {
		if v.Name == "ops" {
			id = k
		}
	}
	if code, _, _ = b.do("POST", "/org/keys/"+id+"/revoke", url.Values{"csrf": {csrf}}); code != 303 {
		t.Fatalf("revoke: %d", code)
	}
	if _, ok := e.api.keys[id]; ok {
		t.Fatal("not revoked")
	}
	// The webapp refuses to revoke the key it runs on.
	own := "00000000000000000000000000000001"
	if code, _, _ = b.do("POST", "/org/keys/"+own+"/revoke", url.Values{"csrf": {csrf}}); code != 400 {
		t.Fatalf("own key revoke: %d", code)
	}
	if _, ok := e.api.keys[own]; !ok {
		t.Fatal("own key was revoked")
	}
}

func TestMountedUnderPrefixAndAPIErrors(t *testing.T) {
	e := newEnv(t, "/console")
	b := e.browser()
	if code, _, h := b.do("GET", "/console/projects", nil); code != 303 || h.Get("Location") != "/console/login" {
		t.Fatalf("redirect: %d %v", code, h)
	}
	if code, _, _ := b.do("GET", "/projects", nil); code != 404 {
		t.Fatalf("served outside the prefix: %d", code)
	}
	b.login()
	_, page, _ := b.do("GET", "/console/projects", nil)
	if !strings.Contains(page, `href="/console/projects/default"`) {
		t.Fatalf("links ignore the prefix: %s", page)
	}
	u, _ := url.Parse(e.ts.URL + "/console/")
	if cs := b.c.Jar.Cookies(u); len(cs) == 0 {
		t.Fatal("cookie not scoped to the prefix")
	}
	// A rejected webapp key is explained, not leaked.
	e.srv.api = newAPI(e.srv.api.base, "bf_stale_key")
	code, page, _ := b.do("GET", "/console/projects", nil)
	if code != 502 || !strings.Contains(page, "BLOWFISH_WEBAPP_API_KEY") || strings.Contains(page, "bf_stale_key") {
		t.Fatalf("rejected key: %d %s", code, page)
	}
}

func TestConfigValidation(t *testing.T) {
	good := Config{BaseURL: "http://x", APIKey: "k", Admins: []string{"a@b.c"}, SessionSecret: []byte(strings.Repeat("x", 32)), Mailer: "log"}
	if err := good.validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"no key":       func(c *Config) { c.APIKey = "" },
		"short secret": func(c *Config) { c.SessionSecret = []byte("short") },
		"no admins":    func(c *Config) { c.Admins = nil },
		"bad mailer":   func(c *Config) { c.Mailer = "pigeon" },
		"smtp unset":   func(c *Config) { c.Mailer = "smtp" },
		"bad base url": func(c *Config) { c.BaseURL = "console" },
	} {
		c := good
		mutate(&c)
		if c.validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestExedevMailerPostsJSON(t *testing.T) {
	var got map[string]string
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request: %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
	}))
	defer gw.Close()
	m := &exedevMailer{url: gw.URL, client: gw.Client()}
	if err := m.Send(context.Background(), "a@b.c", "subj", "body"); err != nil {
		t.Fatal(err)
	}
	if got["to"] != "a@b.c" || got["subject"] != "subj" || got["body"] != "body" {
		t.Fatalf("payload %v", got)
	}
}
