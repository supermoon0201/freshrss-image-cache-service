package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestNormalizeURLRemovesFragment(t *testing.T) {
	server := &Server{
		cfg: Config{
			AllowedSchemes: map[string]struct{}{
				"http":  {},
				"https": {},
			},
		},
	}

	got, err := server.normalizeURL("https://example.com/image.png?x=1#fragment")
	if err != nil {
		t.Fatalf("normalizeURL returned error: %v", err)
	}

	want := "https://example.com/image.png?x=1"
	if got != want {
		t.Fatalf("normalizeURL = %q, want %q", got, want)
	}
}

func TestNormalizeURLRejectsUnsupportedScheme(t *testing.T) {
	server := &Server{
		cfg: Config{
			AllowedSchemes: map[string]struct{}{
				"http":  {},
				"https": {},
			},
		},
	}

	if _, err := server.normalizeURL("ftp://example.com/file"); err == nil {
		t.Fatal("normalizeURL accepted unsupported scheme")
	}
}

func TestDecideContentTypeFallsBackToSniffing(t *testing.T) {
	pngPreview := []byte{
		0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n',
		0x00, 0x00, 0x00, 0x0d, 'I', 'H', 'D', 'R',
	}

	got := decideContentType("", pngPreview)
	if got != "image/png" {
		t.Fatalf("decideContentType = %q, want %q", got, "image/png")
	}
}

func TestValidateUpstreamBodyRejectsChallengePage(t *testing.T) {
	resp := &http.Response{
		Header: http.Header{
			"Content-Type": []string{"text/html; charset=utf-8"},
		},
		Body: http.NoBody,
	}

	resp.Body = http.NoBody
	resp.Body = &readCloser{data: []byte("<html><body>Just a moment...</body></html>")}

	if _, _, err := validateUpstreamBody(resp); err == nil {
		t.Fatal("validateUpstreamBody accepted challenge page")
	}
}

func TestMatchUpstreamHeaderRuleWildcard(t *testing.T) {
	server := &Server{
		cfg: Config{
			UpstreamHeaderRules: []UpstreamHeaderRule{
				{
					Name:      "cdn",
					Hosts:     []string{"*.example.com"},
					Referer:   "https://www.example.com/post/1",
					UserAgent: "Mozilla/5.0 test",
				},
			},
		},
	}

	rule, ok := server.matchUpstreamHeaderRule("https://img.example.com/static/a.png")
	if !ok {
		t.Fatal("expected to match upstream rule")
	}
	if got := rule.headerMap()["Referer"]; got != "https://www.example.com/post/1" {
		t.Fatalf("rule referer = %q", got)
	}
}

func TestBuildForwardedOptionsRejectsDisallowedHeader(t *testing.T) {
	server := &Server{}
	_, err := server.buildForwardedOptions("https://example.com/a.png", map[string]string{
		"X-Forwarded-For": "1.1.1.1",
	})
	if err == nil {
		t.Fatal("expected disallowed header rejection")
	}
}

func TestBuildForwardedOptionsRejectsSensitiveHost(t *testing.T) {
	server := &Server{
		cfg: Config{
			CredentialedHosts: []string{"allowed.example.com"},
		},
	}

	_, err := server.buildForwardedOptions("https://blocked.example.com/a.png", map[string]string{
		"Cookie": "session=abc",
	})
	if err == nil {
		t.Fatal("expected sensitive host rejection")
	}
}

func TestEnsureCachedUsesRuleHeadersAndCachesResult(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Referer"); got != "https://www.example.com/post/1" {
			http.Error(w, "missing referer", http.StatusForbidden)
			return
		}
		if got := r.Header.Get("User-Agent"); got != "Mozilla/5.0 rule-test" {
			http.Error(w, "missing user-agent", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.cfg.UpstreamHeaderRules = []UpstreamHeaderRule{
		{
			Name:      "local-test",
			Hosts:     []string{"127.0.0.1"},
			Referer:   "https://www.example.com/post/1",
			UserAgent: "Mozilla/5.0 rule-test",
		},
	}
	server.httpClient = upstream.Client()

	result, err := server.ensureCached(context.Background(), upstream.URL+"/image.png", upstreamFetchOptions{})
	if err != nil {
		t.Fatalf("ensureCached returned error: %v", err)
	}
	if result.Status != "MISS" {
		t.Fatalf("first ensureCached status = %q, want MISS", result.Status)
	}

	result, err = server.ensureCached(context.Background(), upstream.URL+"/image.png", upstreamFetchOptions{})
	if err != nil {
		t.Fatalf("ensureCached second call returned error: %v", err)
	}
	if result.Status != "HIT" {
		t.Fatalf("second ensureCached status = %q, want HIT", result.Status)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want 1", got)
	}
}

func TestHandleGetForwardsSafeHeadersAndCachesResult(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Referer"); got != "https://hellogithub.com/" {
			http.Error(w, "missing referer", http.StatusForbidden)
			return
		}
		if got := r.Header.Get("Origin"); got != "https://hellogithub.com" {
			http.Error(w, "missing origin", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.httpClient = upstream.Client()

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+upstream.URL+"/hello.png", nil)
	req.Header.Set("Referer", "https://hellogithub.com/")
	req.Header.Set("Origin", "https://hellogithub.com")
	req.Header.Set("User-Agent", "Mozilla/5.0 test")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
	rec := httptest.NewRecorder()

	server.handleGet(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("first GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Piccache-Status"); got != "MISS" {
		t.Fatalf("first GET X-Piccache-Status = %q, want MISS", got)
	}

	rec = httptest.NewRecorder()
	server.handleGet(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("second GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Piccache-Status"); got != "HIT" {
		t.Fatalf("second GET X-Piccache-Status = %q, want HIT", got)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want 1", got)
	}
}

func TestHandleHeadFetchesAndCachesWithoutBody(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Referer"); got != "https://hellogithub.com/" {
			http.Error(w, "missing referer", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.httpClient = upstream.Client()

	req := httptest.NewRequest(http.MethodHead, "/piccache?url="+upstream.URL+"/head.png", nil)
	req.Header.Set("Referer", "https://hellogithub.com/")
	rec := httptest.NewRecorder()

	server.handlePiccache(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("first HEAD status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Piccache-Status"); got != "MISS" {
		t.Fatalf("first HEAD X-Piccache-Status = %q, want MISS", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "image/png" {
		t.Fatalf("first HEAD Content-Type = %q, want image/png", got)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("first HEAD body len = %d, want 0", rec.Body.Len())
	}

	rec = httptest.NewRecorder()
	server.handlePiccache(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("second HEAD status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Piccache-Status"); got != "HIT" {
		t.Fatalf("second HEAD X-Piccache-Status = %q, want HIT", got)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("second HEAD body len = %d, want 0", rec.Body.Len())
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want 1", got)
	}
}

func TestEnsureCachedWithSensitiveForwardedHeadersBypassesSharedCache(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Cookie"); got != "session=abc" {
			http.Error(w, "missing cookie", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.cfg.CredentialedHosts = []string{"127.0.0.1"}
	server.httpClient = upstream.Client()

	options, err := server.buildForwardedOptions(upstream.URL+"/private.png", map[string]string{
		"Cookie": "session=abc",
	})
	if err != nil {
		t.Fatalf("buildForwardedOptions returned error: %v", err)
	}

	if _, err := server.ensureCached(context.Background(), upstream.URL+"/private.png", options); err != nil {
		t.Fatalf("ensureCached returned error: %v", err)
	}
	if _, err := server.ensureCached(context.Background(), upstream.URL+"/private.png", options); err != nil {
		t.Fatalf("ensureCached second call returned error: %v", err)
	}

	if got := requests.Load(); got != 2 {
		t.Fatalf("upstream requests = %d, want 2", got)
	}

	cacheKey := server.cacheKey(upstream.URL+"/private.png", "private")
	if _, err := os.Stat(server.metaPath(cacheKey)); !os.IsNotExist(err) {
		t.Fatalf("expected no private cache file, stat err = %v", err)
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	cacheDir := t.TempDir()
	return &Server{
		cfg: Config{
			CacheDir:            cacheDir,
			FetchTimeout:        5 * time.Second,
			MaxBodyBytes:        1 << 20,
			CacheTTL:            time.Hour,
			MaxCacheBytes:       1 << 30,
			UpstreamConcurrency: 4,
			AllowedSchemes: map[string]struct{}{
				"http":  {},
				"https": {},
			},
		},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		fetchSem:   make(chan struct{}, 4),
	}
}

func minimalPNG() []byte {
	return []byte{
		0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n',
		0x00, 0x00, 0x00, 0x0d, 'I', 'H', 'D', 'R',
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
		0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53, 0xde,
	}
}

type readCloser struct {
	data []byte
	read bool
}

func (r *readCloser) Read(p []byte) (int, error) {
	if r.read {
		return 0, http.ErrBodyReadAfterClose
	}
	n := copy(p, r.data)
	r.read = true
	return n, nil
}

func (r *readCloser) Close() error {
	return nil
}

func TestMain(m *testing.M) {
	code := m.Run()
	_ = os.RemoveAll(filepath.Join(".", "data"))
	os.Exit(code)
}
