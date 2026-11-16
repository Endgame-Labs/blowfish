package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// API talks to blowfish's admin API with a superadmin key.
type API struct {
	base   string
	key    string
	client *http.Client
}

func newAPI(base, key string) *API {
	return &API{base: base, key: key, client: &http.Client{Timeout: 20 * time.Second}}
}

// APIError is an error response from blowfish.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("blowfish returned %d: %s", e.Status, e.Message)
}

type Project struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	CreatedBy string    `json:"created_by"`
}

type Key struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Kind       string     `json:"kind"`
	Project    string     `json:"project"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	CreatedBy  string     `json:"created_by"`
	LastUsedAt *time.Time `json:"last_used_at"`
}

type WhoAmI struct {
	Kind  string `json:"kind"`
	KeyID string `json:"key_id"`
}

func (a *API) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.key)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("can't reach blowfish: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Error == "" {
			e.Error = http.StatusText(resp.StatusCode)
		}
		return &APIError{Status: resp.StatusCode, Message: e.Error}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (a *API) WhoAmI(ctx context.Context) (w WhoAmI, err error) {
	err = a.do(ctx, "GET", "/_blowfish/v1/whoami", nil, &w)
	return
}

func (a *API) Projects(ctx context.Context) ([]Project, error) {
	var out struct {
		Projects []Project `json:"projects"`
	}
	err := a.do(ctx, "GET", "/_blowfish/v1/projects", nil, &out)
	return out.Projects, err
}

func (a *API) Project(ctx context.Context, id string) (p Project, err error) {
	err = a.do(ctx, "GET", "/_blowfish/v1/projects/"+url.PathEscape(id), nil, &p)
	return
}

func (a *API) CreateProject(ctx context.Context, id, name, by string) (p Project, err error) {
	err = a.do(ctx, "POST", "/_blowfish/v1/projects",
		map[string]string{"id": id, "name": name, "created_by": by}, &p)
	return
}

func (a *API) RenameProject(ctx context.Context, id, name string) (p Project, err error) {
	err = a.do(ctx, "PATCH", "/_blowfish/v1/projects/"+url.PathEscape(id), map[string]string{"name": name}, &p)
	return
}

func (a *API) DeleteProject(ctx context.Context, id string, force bool) error {
	return a.do(ctx, "DELETE", fmt.Sprintf("/_blowfish/v1/projects/%s?force=%t", url.PathEscape(id), force), nil, nil)
}

func (a *API) ProjectKeys(ctx context.Context, id string) ([]Key, error) {
	var out struct {
		Keys []Key `json:"keys"`
	}
	err := a.do(ctx, "GET", "/_blowfish/v1/projects/"+url.PathEscape(id)+"/keys", nil, &out)
	return out.Keys, err
}

type created struct {
	Key   Key    `json:"key"`
	Token string `json:"token"`
}

func (a *API) CreateProjectKey(ctx context.Context, id, name, kind string, expires *time.Time, by string) (c created, err error) {
	body := map[string]any{"name": name, "kind": kind, "created_by": by}
	if expires != nil {
		body["expires_at"] = expires.UTC().Format(time.RFC3339)
	}
	err = a.do(ctx, "POST", "/_blowfish/v1/projects/"+url.PathEscape(id)+"/keys", body, &c)
	return
}

func (a *API) RevokeProjectKey(ctx context.Context, id, key string) error {
	return a.do(ctx, "DELETE", "/_blowfish/v1/projects/"+url.PathEscape(id)+"/keys/"+url.PathEscape(key), nil, nil)
}

func (a *API) OrgKeys(ctx context.Context) ([]Key, error) {
	var out struct {
		Keys []Key `json:"keys"`
	}
	err := a.do(ctx, "GET", "/_blowfish/v1/org/keys", nil, &out)
	return out.Keys, err
}

func (a *API) CreateOrgKey(ctx context.Context, name string, expires *time.Time, by string) (c created, err error) {
	body := map[string]any{"name": name, "created_by": by}
	if expires != nil {
		body["expires_at"] = expires.UTC().Format(time.RFC3339)
	}
	err = a.do(ctx, "POST", "/_blowfish/v1/org/keys", body, &c)
	return
}

func (a *API) RevokeOrgKey(ctx context.Context, key string) error {
	return a.do(ctx, "DELETE", "/_blowfish/v1/org/keys/"+url.PathEscape(key), nil, nil)
}
