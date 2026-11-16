package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Config is read from BLOWFISH_WEBAPP_* environment variables.
type Config struct {
	Listen        string
	BaseURL       string // public URL, including any path prefix such as /console
	APIURL        string // blowfish base URL
	APIKey        string // a superadmin key
	Admins        []string
	SessionSecret []byte
	Mailer        string // exedev, smtp, or log
	TrustProxy    bool   // take the client IP from X-Forwarded-For

	SMTPAddr  string
	SMTPUser  string
	SMTPPass  string
	SMTPFrom  string
	ExedevURL string
}

func env(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv("BLOWFISH_WEBAPP_" + name)); v != "" {
		return v
	}
	return fallback
}

// LoadConfig reads and validates the environment.
func LoadConfig() (*Config, error) {
	c := &Config{
		Listen:        env("LISTEN", "127.0.0.1:8090"),
		BaseURL:       strings.TrimRight(env("BASE_URL", "http://127.0.0.1:8090"), "/"),
		APIURL:        strings.TrimRight(env("API_URL", "http://127.0.0.1:8000"), "/"),
		APIKey:        env("API_KEY", ""),
		SessionSecret: []byte(env("SESSION_SECRET", "")),
		Mailer:        env("MAILER", "log"),
		SMTPAddr:      env("SMTP_ADDR", ""),
		SMTPUser:      env("SMTP_USER", ""),
		SMTPPass:      env("SMTP_PASS", ""),
		SMTPFrom:      env("SMTP_FROM", ""),
		ExedevURL:     env("EXEDEV_EMAIL_URL", "http://169.254.169.254/gateway/email/send"),
	}
	if v := env("TRUST_PROXY", ""); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("BLOWFISH_WEBAPP_TRUST_PROXY: %w", err)
		}
		c.TrustProxy = b
	}
	for _, a := range strings.Split(env("ADMINS", ""), ",") {
		if a = strings.ToLower(strings.TrimSpace(a)); a != "" {
			c.Admins = append(c.Admins, a)
		}
	}
	return c, c.validate()
}

func (c *Config) validate() error {
	var errs []error
	if c.APIKey == "" {
		errs = append(errs, errors.New("BLOWFISH_WEBAPP_API_KEY is required (a superadmin key)"))
	}
	if len(c.SessionSecret) < 32 {
		errs = append(errs, errors.New("BLOWFISH_WEBAPP_SESSION_SECRET is required, at least 32 characters (try: openssl rand -hex 32)"))
	}
	if len(c.Admins) == 0 {
		errs = append(errs, errors.New("BLOWFISH_WEBAPP_ADMINS is required (comma-separated emails)"))
	}
	if u, err := url.Parse(c.BaseURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, errors.New("BLOWFISH_WEBAPP_BASE_URL must be an http(s) URL"))
	}
	switch c.Mailer {
	case "log", "exedev":
	case "smtp":
		if c.SMTPAddr == "" || c.SMTPFrom == "" {
			errs = append(errs, errors.New("the smtp mailer needs BLOWFISH_WEBAPP_SMTP_ADDR and BLOWFISH_WEBAPP_SMTP_FROM"))
		}
	default:
		errs = append(errs, fmt.Errorf("BLOWFISH_WEBAPP_MAILER must be exedev, smtp, or log, not %q", c.Mailer))
	}
	return errors.Join(errs...)
}

// PathPrefix is the URL path the app is mounted under, such as "/console", or "".
func (c *Config) PathPrefix() string {
	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return ""
	}
	return strings.TrimRight(u.Path, "/")
}

// SecureCookies reports whether cookies carry the Secure flag: always, except on a plain-http
// base URL, which is for local development.
func (c *Config) SecureCookies() bool { return !strings.HasPrefix(c.BaseURL, "http://") }
