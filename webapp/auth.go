package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	linkTTL    = 15 * time.Minute
	sessionTTL = 12 * time.Hour
	formTTL    = time.Hour
)

var b64 = base64.RawURLEncoding

func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b64.EncodeToString(b)
}

func sha(s string) [32]byte { return sha256.Sum256([]byte(s)) }

// signer makes and checks HMAC-signed values: sessions, flash messages, and login form tokens.
type signer struct{ secret []byte }

func (s signer) mac(purpose, data string) string {
	h := hmac.New(sha256.New, s.secret)
	h.Write([]byte(purpose))
	h.Write([]byte{0})
	h.Write([]byte(data))
	return b64.EncodeToString(h.Sum(nil))
}

// seal returns payload.signature, with the purpose bound into the signature.
func (s signer) seal(purpose string, payload any) string {
	raw, _ := json.Marshal(payload)
	data := b64.EncodeToString(raw)
	return data + "." + s.mac(purpose, data)
}

func (s signer) open(purpose, value string, out any) error {
	data, sig, ok := strings.Cut(value, ".")
	if !ok || subtle.ConstantTimeCompare([]byte(sig), []byte(s.mac(purpose, data))) != 1 {
		return errors.New("bad signature")
	}
	raw, err := b64.DecodeString(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// session is the content of the signed cookie.
type session struct {
	Email   string `json:"e"`
	ID      string `json:"i"`
	Expires int64  `json:"x"`
}

func (s signer) newSession(email string, now time.Time) (string, session) {
	sess := session{Email: email, ID: randomToken(16), Expires: now.Add(sessionTTL).Unix()}
	return s.seal("session", sess), sess
}

func (s signer) readSession(cookie string, now time.Time) (session, bool) {
	var sess session
	if s.open("session", cookie, &sess) != nil || sess.Email == "" || now.Unix() >= sess.Expires {
		return session{}, false
	}
	return sess, true
}

// csrf derives a per-session token, so forms need no server-side state.
func (s signer) csrf(sess session) string { return s.mac("csrf", sess.ID) }

// loginForm is a stateless anti-forgery token for the sign-in form, which has no session yet.
func (s signer) loginForm(now time.Time) string {
	ts := strconv.FormatInt(now.Unix(), 10)
	return ts + "." + s.mac("login", ts)
}

func (s signer) checkLoginForm(token string, now time.Time) bool {
	ts, sig, ok := strings.Cut(token, ".")
	if !ok || subtle.ConstantTimeCompare([]byte(sig), []byte(s.mac("login", ts))) != 1 {
		return false
	}
	t, err := strconv.ParseInt(ts, 10, 64)
	return err == nil && now.Unix()-t < int64(formTTL.Seconds()) && t <= now.Unix()+60
}

// linkStore holds single-use sign-in links in memory, keyed by the hash of the token.
type linkStore struct {
	mu    sync.Mutex
	links map[[32]byte]link
}

type link struct {
	email   string
	expires time.Time
}

func newLinkStore() *linkStore { return &linkStore{links: map[[32]byte]link{}} }

// issue creates a link token for an email. Only the hash is kept.
func (l *linkStore) issue(email string, now time.Time) string {
	token := randomToken(32)
	l.mu.Lock()
	defer l.mu.Unlock()
	for h, v := range l.links {
		if now.After(v.expires) {
			delete(l.links, h)
		}
	}
	if len(l.links) >= 10000 {
		l.links = map[[32]byte]link{}
	}
	l.links[sha(token)] = link{email: email, expires: now.Add(linkTTL)}
	return token
}

// peek reports whether a token is currently valid, without using it.
func (l *linkStore) peek(token string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	v, ok := l.links[sha(token)]
	return ok && now.Before(v.expires)
}

// consume uses a token exactly once.
func (l *linkStore) consume(token string, now time.Time) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	h := sha(token)
	v, ok := l.links[h]
	if !ok {
		return "", false
	}
	delete(l.links, h)
	if now.After(v.expires) {
		return "", false
	}
	return v.email, true
}

// limiter is a sliding-window rate limiter.
type limiter struct {
	mu     sync.Mutex
	hits   map[string][]time.Time
	limit  int
	window time.Duration
}

func newLimiter(limit int, window time.Duration) *limiter {
	return &limiter{hits: map[string][]time.Time{}, limit: limit, window: window}
}

// allow records an attempt and reports whether it is within the limit.
func (l *limiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.hits) > 50000 {
		l.hits = map[string][]time.Time{}
	}
	recent := l.hits[key][:0]
	for _, t := range l.hits[key] {
		if now.Sub(t) < l.window {
			recent = append(recent, t)
		}
	}
	if len(recent) >= l.limit {
		l.hits[key] = recent
		return false
	}
	l.hits[key] = append(recent, now)
	return true
}

// keyHash keeps email addresses out of limiter memory dumps and logs.
func keyHash(s string) string {
	h := sha(strings.ToLower(s))
	return hex.EncodeToString(h[:8])
}
