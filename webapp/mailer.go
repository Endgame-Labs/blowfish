package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/smtp"
	"strings"
	"time"
)

// Mailer delivers plain-text mail.
type Mailer interface {
	Send(ctx context.Context, to, subject, body string) error
}

func newMailer(c *Config) Mailer {
	switch c.Mailer {
	case "exedev":
		return &exedevMailer{url: c.ExedevURL, client: &http.Client{Timeout: 15 * time.Second}}
	case "smtp":
		return &smtpMailer{addr: c.SMTPAddr, user: c.SMTPUser, pass: c.SMTPPass, from: c.SMTPFrom}
	default:
		return logMailer{}
	}
}

// logMailer prints the message, for development.
type logMailer struct{}

func (logMailer) Send(_ context.Context, to, subject, body string) error {
	slog.Info("mail (log mailer)", "to", to, "subject", subject, "body", body)
	return nil
}

// exedevMailer uses the exe.dev VM email gateway.
type exedevMailer struct {
	url    string
	client *http.Client
}

func (m *exedevMailer) Send(ctx context.Context, to, subject, body string) error {
	payload, _ := json.Marshal(map[string]string{"to": to, "subject": subject, "body": body})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("email gateway returned %s", resp.Status)
	}
	return nil
}

// smtpMailer sends through an SMTP server, upgrading to TLS when the server offers STARTTLS.
type smtpMailer struct{ addr, user, pass, from string }

func (m *smtpMailer) Send(_ context.Context, to, subject, body string) error {
	for _, v := range []string{to, subject, m.from} {
		if strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("header injection attempt")
		}
	}
	host := m.addr
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	var auth smtp.Auth
	if m.user != "" {
		auth = smtp.PlainAuth("", m.user, m.pass, host)
	}
	msg := "From: " + m.from + "\r\nTo: " + to + "\r\nSubject: " + subject +
		"\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n" +
		strings.ReplaceAll(body, "\n", "\r\n") + "\r\n"
	return smtp.SendMail(m.addr, auth, m.from, []string{to}, []byte(msg))
}
