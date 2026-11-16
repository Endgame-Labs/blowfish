package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"time"
)

//go:embed templates/*.html
var templateFS embed.FS

const (
	sessionCookie = "blowfish_webapp_session"
	flashCookie   = "blowfish_webapp_flash"
)

var (
	projectIDRe = regexp.MustCompile(`^[a-z0-9-]{1,40}$`)
	keyIDRe     = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// Server is the webapp.
type Server struct {
	cfg    *Config
	api    *API
	mailer Mailer
	sign   signer
	links  *linkStore
	byMail *limiter // sign-in link requests per email address
	byIP   *limiter // sign-in link requests per client address
	verify *limiter // link redemptions per client address
	admins map[string]bool
	prefix string
	now    func() time.Time
	pages  map[string]*template.Template
}

// NewServer wires the webapp together.
func NewServer(cfg *Config, api *API, mailer Mailer) *Server {
	s := &Server{
		cfg: cfg, api: api, mailer: mailer,
		sign:   signer{secret: cfg.SessionSecret},
		links:  newLinkStore(),
		byMail: newLimiter(5, 15*time.Minute),
		byIP:   newLimiter(10, 15*time.Minute),
		verify: newLimiter(30, 15*time.Minute),
		admins: map[string]bool{},
		prefix: cfg.PathPrefix(),
		now:    time.Now,
		pages:  map[string]*template.Template{},
	}
	for _, a := range cfg.Admins {
		s.admins[a] = true
	}
	funcs := template.FuncMap{"date": func(t any) string {
		switch v := t.(type) {
		case time.Time:
			return v.UTC().Format("2006-01-02 15:04 UTC")
		case *time.Time:
			if v != nil {
				return v.UTC().Format("2006-01-02 15:04 UTC")
			}
		}
		return ""
	}}
	for _, name := range []string{"login", "message", "login_confirm", "projects", "project", "key_created", "confirm", "org_keys"} {
		s.pages[name] = template.Must(template.New(name).Funcs(funcs).ParseFS(templateFS,
			"templates/layout.html", "templates/keytable.html", "templates/"+name+".html"))
	}
	return s
}

// Handler returns the HTTP handler, mounted under the base URL's path.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	p := s.prefix
	route := func(pattern string, h http.HandlerFunc) {
		method, path, _ := strings.Cut(pattern, " ")
		mux.HandleFunc(method+" "+p+path, h)
	}
	authed := func(h func(http.ResponseWriter, *http.Request, session)) http.HandlerFunc {
		return s.requireSession(h)
	}
	route("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	route("GET /{$}", s.index)
	if p != "" {
		mux.HandleFunc("GET "+p, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, p+"/", http.StatusSeeOther) })
	}
	route("GET /login", s.loginPage)
	route("POST /login", s.loginSubmit)
	route("GET /login/verify", s.verifyPage)
	route("POST /login/verify", s.verifySubmit)
	route("POST /logout", authed(s.logout))
	route("GET /projects", authed(s.projects))
	route("POST /projects", authed(s.createProject))
	route("GET /projects/{id}", authed(s.project))
	route("POST /projects/{id}/rename", authed(s.renameProject))
	route("GET /projects/{id}/delete", authed(s.deleteProjectPage))
	route("POST /projects/{id}/delete", authed(s.deleteProject))
	route("POST /projects/{id}/keys", authed(s.createProjectKey))
	route("GET /projects/{id}/keys/{key}/revoke", authed(s.revokeProjectKeyPage))
	route("POST /projects/{id}/keys/{key}/revoke", authed(s.revokeProjectKey))
	route("GET /org/keys", authed(s.orgKeys))
	route("POST /org/keys", authed(s.createOrgKey))
	route("GET /org/keys/{key}/revoke", authed(s.revokeOrgKeyPage))
	route("POST /org/keys/{key}/revoke", authed(s.revokeOrgKey))
	return s.secure(mux)
}

type ctxKey struct{}

// secure adds security headers and a per-request script and style nonce.
func (s *Server) secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nonce := randomToken(16)
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; style-src 'nonce-"+nonce+"'; script-src 'nonce-"+nonce+"'; img-src data:; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-store")
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, nonce)))
	})
}

func (s *Server) clientIP(r *http.Request) string {
	if s.cfg.TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) audit(sess session, r *http.Request, action string, attrs ...any) {
	slog.Info("audit", append([]any{"actor", sess.Email, "action", action, "ip", s.clientIP(r)}, attrs...)...)
}

type flash struct{ Kind, Text string }

func (s *Server) setFlash(w http.ResponseWriter, kind, text string) {
	s.setCookie(w, flashCookie, s.sign.seal("flash", flash{kind, text}), 60)
}

func (s *Server) setCookie(w http.ResponseWriter, name, value string, maxAge int) {
	path := s.prefix
	if path == "" {
		path = "/"
	}
	http.SetCookie(w, &http.Cookie{Name: name, Value: value, Path: path, MaxAge: maxAge,
		HttpOnly: true, Secure: s.cfg.SecureCookies(), SameSite: http.SameSiteLaxMode})
}

// render writes a page with the common template data.
func (s *Server) render(w http.ResponseWriter, r *http.Request, status int, page string, sess *session, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["Base"] = s.prefix
	data["Nonce"], _ = r.Context().Value(ctxKey{}).(string)
	if sess != nil {
		data["Email"] = sess.Email
		data["CSRF"] = s.sign.csrf(*sess)
	}
	if c, err := r.Cookie(flashCookie); err == nil {
		var f flash
		if s.sign.open("flash", c.Value, &f) == nil {
			data["Flash"] = f
		}
		s.setCookie(w, flashCookie, "", -1)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := s.pages[page].ExecuteTemplate(w, "layout", data); err != nil {
		slog.Error("render", "page", page, "err", err)
	}
}

func (s *Server) message(w http.ResponseWriter, r *http.Request, status int, sess *session, heading, text, back, backText string) {
	s.render(w, r, status, "message", sess, map[string]any{"Heading": heading, "Text": text, "Back": back, "BackText": backText})
}

// requireSession checks the cookie and, for POST, the CSRF token.
func (s *Server) requireSession(h func(http.ResponseWriter, *http.Request, session)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var sess session
		ok := false
		if c, err := r.Cookie(sessionCookie); err == nil {
			sess, ok = s.sign.readSession(c.Value, s.now())
		}
		if !ok || !s.admins[sess.Email] {
			http.Redirect(w, r, s.prefix+"/login", http.StatusSeeOther)
			return
		}
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil || !constEq(r.PostFormValue("csrf"), s.sign.csrf(sess)) {
				s.message(w, r, http.StatusForbidden, &sess, "Form expired", "That request couldn't be verified. Go back, reload the page, and try again.", s.prefix+"/projects", "Projects")
				return
			}
		}
		h(w, r, sess)
	}
}

func constEq(a, b string) bool {
	return len(a) == len(b) && subtleEq(a, b)
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, s.prefix+"/projects", http.StatusSeeOther)
}

// ---- sign-in ----

func (s *Server) loginPage(w http.ResponseWriter, r *http.Request) {
	s.render(w, r, 200, "login", nil, map[string]any{"Form": s.sign.loginForm(s.now())})
}

func (s *Server) loginSubmit(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	if r.ParseForm() != nil || !s.sign.checkLoginForm(r.PostFormValue("form"), now) {
		s.message(w, r, http.StatusBadRequest, nil, "Try again", "That form expired. Reload the sign-in page and try again.", s.prefix+"/login", "Sign in")
		return
	}
	email := strings.ToLower(strings.TrimSpace(r.PostFormValue("email")))
	// Every attempt counts against the limits, whether or not the address is allowed, so a
	// limit can't be used to learn who the admins are.
	okMail, okIP := s.byMail.allow(keyHash(email), now), s.byIP.allow(s.clientIP(r), now)
	if !okMail || !okIP {
		slog.Warn("sign-in rate limited", "ip", s.clientIP(r))
		s.message(w, r, http.StatusTooManyRequests, nil, "Too many attempts", "Please wait a few minutes before asking for another link.", s.prefix+"/login", "Back")
		return
	}
	if addr, err := mail.ParseAddress(email); err == nil && addr.Address == email && s.admins[email] {
		token := s.links.issue(email, now)
		link := s.cfg.BaseURL + "/login/verify?token=" + token
		// Send in the background so the response time doesn't reveal whether mail was sent.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			body := "Sign in to the blowfish console:\n\n" + link + "\n\nThis link works once and expires in 15 minutes. If you didn't ask for it, ignore this email.\n"
			if err := s.mailer.Send(ctx, email, "Your blowfish sign-in link", body); err != nil {
				slog.Error("sending sign-in link failed", "err", err)
			}
		}()
		slog.Info("sign-in link issued", "actor", email, "ip", s.clientIP(r))
	} else {
		slog.Info("sign-in requested for an address not on the list", "ip", s.clientIP(r))
	}
	s.message(w, r, 200, nil, "Check your email", "If that address is on the admin list, a sign-in link is on its way. It works once, and expires in 15 minutes.", s.prefix+"/login", "Back")
}

// verifyPage asks for a click before using the token, so mail scanners that fetch links
// can't use it up.
func (s *Server) verifyPage(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if !s.links.peek(token, s.now()) {
		s.message(w, r, http.StatusBadRequest, nil, "Link expired", "That sign-in link has expired or was already used. Ask for a new one.", s.prefix+"/login", "Sign in")
		return
	}
	s.render(w, r, 200, "login_confirm", nil, map[string]any{"Token": token})
}

func (s *Server) verifySubmit(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	if !s.verify.allow(s.clientIP(r), now) {
		s.message(w, r, http.StatusTooManyRequests, nil, "Too many attempts", "Please wait a few minutes.", s.prefix+"/login", "Back")
		return
	}
	_ = r.ParseForm()
	email, ok := s.links.consume(r.PostFormValue("token"), now)
	if !ok || !s.admins[email] {
		s.message(w, r, http.StatusBadRequest, nil, "Link expired", "That sign-in link has expired or was already used. Ask for a new one.", s.prefix+"/login", "Sign in")
		return
	}
	value, sess := s.sign.newSession(email, now)
	s.setCookie(w, sessionCookie, value, int(sessionTTL.Seconds()))
	s.audit(sess, r, "login")
	http.Redirect(w, r, s.prefix+"/projects", http.StatusSeeOther)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request, sess session) {
	s.setCookie(w, sessionCookie, "", -1)
	s.audit(sess, r, "logout")
	http.Redirect(w, r, s.prefix+"/login", http.StatusSeeOther)
}

// ---- helpers for API failures ----

func (s *Server) apiFailure(w http.ResponseWriter, r *http.Request, sess session, err error) {
	slog.Error("blowfish API call failed", "path", r.URL.Path, "err", err)
	text := "Couldn't reach blowfish. " + err.Error()
	var ae *APIError
	if errors.As(err, &ae) && (ae.Status == 401 || ae.Status == 403) {
		text = "blowfish rejected the webapp's API key. Check BLOWFISH_WEBAPP_API_KEY: it must be a live superadmin key."
	}
	s.message(w, r, http.StatusBadGateway, &sess, "Something went wrong", text, s.prefix+"/projects", "Projects")
}

// fail reports an API error back on the page the user came from, as a flash message.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, sess session, back string, err error) {
	var ae *APIError
	if errors.As(err, &ae) && ae.Status != 401 && ae.Status != 403 && ae.Status != 502 {
		s.setFlash(w, "error", capitalize(ae.Message))
		http.Redirect(w, r, s.prefix+back, http.StatusSeeOther)
		return
	}
	s.apiFailure(w, r, sess, err)
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func validProject(w http.ResponseWriter, r *http.Request, s *Server, sess session) (string, bool) {
	id := r.PathValue("id")
	if !projectIDRe.MatchString(id) {
		s.message(w, r, http.StatusNotFound, &sess, "Not found", "There is no such project.", s.prefix+"/projects", "Projects")
		return "", false
	}
	return id, true
}

func expiry(r *http.Request, now time.Time) (*time.Time, error) {
	v := r.PostFormValue("expires")
	if v == "" {
		return nil, nil
	}
	days, err := strconv.Atoi(v)
	if err != nil || days < 1 || days > 3650 {
		return nil, errors.New("invalid expiry")
	}
	t := now.Add(time.Duration(days) * 24 * time.Hour)
	return &t, nil
}

// ---- projects ----

func (s *Server) projects(w http.ResponseWriter, r *http.Request, sess session) {
	list, err := s.api.Projects(r.Context())
	if err != nil {
		s.apiFailure(w, r, sess, err)
		return
	}
	s.render(w, r, 200, "projects", &sess, map[string]any{"Nav": "projects", "Projects": list})
}

func (s *Server) createProject(w http.ResponseWriter, r *http.Request, sess session) {
	id, name := strings.TrimSpace(r.PostFormValue("id")), strings.TrimSpace(r.PostFormValue("name"))
	if !projectIDRe.MatchString(id) || name == "" {
		s.setFlash(w, "error", "A project needs a name and an ID of lowercase letters, digits, and dashes.")
		http.Redirect(w, r, s.prefix+"/projects", http.StatusSeeOther)
		return
	}
	if _, err := s.api.CreateProject(r.Context(), id, name, "webapp:"+sess.Email); err != nil {
		s.fail(w, r, sess, "/projects", err)
		return
	}
	s.audit(sess, r, "project.create", "project", id)
	s.setFlash(w, "ok", "Created project "+name+".")
	http.Redirect(w, r, s.prefix+"/projects/"+id, http.StatusSeeOther)
}

func (s *Server) project(w http.ResponseWriter, r *http.Request, sess session) {
	id, ok := validProject(w, r, s, sess)
	if !ok {
		return
	}
	project, err := s.api.Project(r.Context(), id)
	if err != nil {
		var ae *APIError
		if errors.As(err, &ae) && ae.Status == 404 {
			s.message(w, r, http.StatusNotFound, &sess, "Not found", "There is no such project.", s.prefix+"/projects", "Projects")
			return
		}
		s.apiFailure(w, r, sess, err)
		return
	}
	keys, err := s.api.ProjectKeys(r.Context(), id)
	if err != nil {
		s.apiFailure(w, r, sess, err)
		return
	}
	s.render(w, r, 200, "project", &sess, map[string]any{
		"Nav": "projects", "Project": project, "Keys": keys, "KeyPath": "/projects/" + id + "/keys",
	})
}

func (s *Server) renameProject(w http.ResponseWriter, r *http.Request, sess session) {
	id, ok := validProject(w, r, s, sess)
	if !ok {
		return
	}
	name := strings.TrimSpace(r.PostFormValue("name"))
	back := "/projects/" + id
	if name == "" {
		s.setFlash(w, "error", "A project needs a name.")
	} else if _, err := s.api.RenameProject(r.Context(), id, name); err != nil {
		s.fail(w, r, sess, back, err)
		return
	} else {
		s.audit(sess, r, "project.rename", "project", id, "name", name)
		s.setFlash(w, "ok", "Renamed.")
	}
	http.Redirect(w, r, s.prefix+back, http.StatusSeeOther)
}

func (s *Server) deleteProjectPage(w http.ResponseWriter, r *http.Request, sess session) {
	id, ok := validProject(w, r, s, sess)
	if !ok {
		return
	}
	s.render(w, r, 200, "confirm", &sess, map[string]any{
		"Nav": "projects", "Heading": "Delete project " + id + "?",
		"Text":   "This revokes every key of the project, immediately, and can't be undone.",
		"Action": "/projects/" + id + "/delete", "Button": "Delete project",
		"TypeToConfirm": id, "Force": true, "Back": "/projects/" + id,
	})
}

func (s *Server) deleteProject(w http.ResponseWriter, r *http.Request, sess session) {
	id, ok := validProject(w, r, s, sess)
	if !ok {
		return
	}
	back := "/projects/" + id
	if r.PostFormValue("confirm") != id {
		s.setFlash(w, "error", "The ID you typed didn't match, so nothing was deleted.")
		http.Redirect(w, r, s.prefix+back+"/delete", http.StatusSeeOther)
		return
	}
	force := r.PostFormValue("force") == "1"
	if err := s.api.DeleteProject(r.Context(), id, force); err != nil {
		var ae *APIError
		if errors.As(err, &ae) && ae.Status == 409 {
			s.setFlash(w, "error", "The project still has namespaces. Tick the box to delete them too.")
			http.Redirect(w, r, s.prefix+back+"/delete", http.StatusSeeOther)
			return
		}
		s.fail(w, r, sess, back, err)
		return
	}
	s.audit(sess, r, "project.delete", "project", id, "force", force)
	s.setFlash(w, "ok", "Deleted project "+id+".")
	http.Redirect(w, r, s.prefix+"/projects", http.StatusSeeOther)
}

func (s *Server) createProjectKey(w http.ResponseWriter, r *http.Request, sess session) {
	id, ok := validProject(w, r, s, sess)
	if !ok {
		return
	}
	back := "/projects/" + id
	name, kind := strings.TrimSpace(r.PostFormValue("name")), r.PostFormValue("kind")
	exp, err := expiry(r, s.now())
	if name == "" || err != nil || (kind != "admin" && kind != "read_write" && kind != "read") {
		s.setFlash(w, "error", "A key needs a name and a kind.")
		http.Redirect(w, r, s.prefix+back, http.StatusSeeOther)
		return
	}
	c, err := s.api.CreateProjectKey(r.Context(), id, name, kind, exp, "webapp:"+sess.Email)
	if err != nil {
		s.fail(w, r, sess, back, err)
		return
	}
	s.audit(sess, r, "key.create", "project", id, "key", c.Key.ID, "kind", kind)
	s.render(w, r, 200, "key_created", &sess, map[string]any{"Nav": "projects", "Key": c.Key, "Token": c.Token, "Back": back})
}

func (s *Server) revokeProjectKeyPage(w http.ResponseWriter, r *http.Request, sess session) {
	id, ok := validProject(w, r, s, sess)
	if !ok {
		return
	}
	key := r.PathValue("key")
	if !keyIDRe.MatchString(key) {
		s.message(w, r, http.StatusNotFound, &sess, "Not found", "There is no such key.", s.prefix+"/projects/"+id, "Back")
		return
	}
	s.render(w, r, 200, "confirm", &sess, map[string]any{
		"Nav": "projects", "Heading": "Revoke this key?",
		"Text":   "Anything using it stops working within seconds, and a revoked key can't be restored. Key " + key[:8] + "…",
		"Action": "/projects/" + id + "/keys/" + key + "/revoke", "Button": "Revoke key", "Back": "/projects/" + id,
	})
}

func (s *Server) revokeProjectKey(w http.ResponseWriter, r *http.Request, sess session) {
	id, ok := validProject(w, r, s, sess)
	if !ok {
		return
	}
	key := r.PathValue("key")
	back := "/projects/" + id
	if !keyIDRe.MatchString(key) {
		s.message(w, r, http.StatusNotFound, &sess, "Not found", "There is no such key.", s.prefix+back, "Back")
		return
	}
	if err := s.api.RevokeProjectKey(r.Context(), id, key); err != nil {
		s.fail(w, r, sess, back, err)
		return
	}
	s.audit(sess, r, "key.revoke", "project", id, "key", key)
	s.setFlash(w, "ok", "Key revoked.")
	http.Redirect(w, r, s.prefix+back, http.StatusSeeOther)
}

// ---- org superadmin keys ----

func (s *Server) orgKeys(w http.ResponseWriter, r *http.Request, sess session) {
	keys, err := s.api.OrgKeys(r.Context())
	if err != nil {
		s.apiFailure(w, r, sess, err)
		return
	}
	own := ""
	if me, err := s.api.WhoAmI(r.Context()); err == nil {
		own = me.KeyID
	}
	s.render(w, r, 200, "org_keys", &sess, map[string]any{"Nav": "org", "Keys": keys, "KeyPath": "/org/keys", "OwnKey": own})
}

func (s *Server) createOrgKey(w http.ResponseWriter, r *http.Request, sess session) {
	name := strings.TrimSpace(r.PostFormValue("name"))
	exp, err := expiry(r, s.now())
	if name == "" || err != nil {
		s.setFlash(w, "error", "A key needs a name.")
		http.Redirect(w, r, s.prefix+"/org/keys", http.StatusSeeOther)
		return
	}
	c, err := s.api.CreateOrgKey(r.Context(), name, exp, "webapp:"+sess.Email)
	if err != nil {
		s.fail(w, r, sess, "/org/keys", err)
		return
	}
	s.audit(sess, r, "orgkey.create", "key", c.Key.ID)
	s.render(w, r, 200, "key_created", &sess, map[string]any{"Nav": "org", "Key": c.Key, "Token": c.Token, "Back": "/org/keys"})
}

func (s *Server) ownKeyGuard(w http.ResponseWriter, r *http.Request, sess session, key string) bool {
	if !keyIDRe.MatchString(key) {
		s.message(w, r, http.StatusNotFound, &sess, "Not found", "There is no such key.", s.prefix+"/org/keys", "Back")
		return false
	}
	if me, err := s.api.WhoAmI(r.Context()); err == nil && me.KeyID == key {
		s.message(w, r, http.StatusBadRequest, &sess, "That's the webapp's own key", "Revoking it would lock this console out of blowfish. Create a new key, put it in BLOWFISH_WEBAPP_API_KEY, restart the webapp, and then revoke the old one with the API.", s.prefix+"/org/keys", "Back")
		return false
	}
	return true
}

func (s *Server) revokeOrgKeyPage(w http.ResponseWriter, r *http.Request, sess session) {
	key := r.PathValue("key")
	if !s.ownKeyGuard(w, r, sess, key) {
		return
	}
	s.render(w, r, 200, "confirm", &sess, map[string]any{
		"Nav": "org", "Heading": "Revoke this superadmin key?",
		"Text":   "Anything using it stops working within seconds. Key " + key[:8] + "…",
		"Action": "/org/keys/" + key + "/revoke", "Button": "Revoke key", "Back": "/org/keys",
	})
}

func (s *Server) revokeOrgKey(w http.ResponseWriter, r *http.Request, sess session) {
	key := r.PathValue("key")
	if !s.ownKeyGuard(w, r, sess, key) {
		return
	}
	if err := s.api.RevokeOrgKey(r.Context(), key); err != nil {
		s.fail(w, r, sess, "/org/keys", err)
		return
	}
	s.audit(sess, r, "orgkey.revoke", "key", key)
	s.setFlash(w, "ok", "Key revoked.")
	http.Redirect(w, r, s.prefix+"/org/keys", http.StatusSeeOther)
}
