package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
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

func TestInferUpstreamHeadersFromURL(t *testing.T) {
	headers := inferUpstreamHeaders("https://img.hellogithub.com/i/z5ncAjLHSpGDTr1_1771822196.png")
	if got := headers["Referer"]; got != "https://hellogithub.com/" {
		t.Fatalf("Referer = %q, want %q", got, "https://hellogithub.com/")
	}
	if got := headers["Origin"]; got != "https://hellogithub.com" {
		t.Fatalf("Origin = %q, want %q", got, "https://hellogithub.com")
	}
}

func TestHandleGetInfersHeadersFromTargetURL(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Referer"); got != "https://hellogithub.com/" {
			http.Error(w, "invalid referer", http.StatusForbidden)
			return
		}
		if got := r.Header.Get("Origin"); got != "https://hellogithub.com" {
			http.Error(w, "invalid origin", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.httpClient = upstream.Client()

	targetURL := "https://img.hellogithub.com/i/demo.png"
	server.httpClient = rewriteTargetHostClient(t, upstream, targetURL)

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	rec := httptest.NewRecorder()

	server.handleGet(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Piccache-Status"); got != "MISS" {
		t.Fatalf("GET X-Piccache-Status = %q, want MISS", got)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want 1", got)
	}
}

func TestFetchAndStoreFallsBackToHostRootHeadersOn403(t *testing.T) {
	var requests atomic.Int32
	targetURL := "https://cdn.assets.example.co.uk/img/demo.png"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			if got := r.Header.Get("Referer"); got != "https://example.co.uk/" {
				http.Error(w, "unexpected first referer", http.StatusForbidden)
				return
			}
			http.Error(w, "need host referer", http.StatusForbidden)
		case 2:
			if got := r.Header.Get("Referer"); got != "https://cdn.assets.example.co.uk/" {
				http.Error(w, "unexpected fallback referer", http.StatusForbidden)
				return
			}
			if got := r.Header.Get("Origin"); got != "https://cdn.assets.example.co.uk" {
				http.Error(w, "unexpected fallback origin", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(minimalPNG())
		default:
			t.Fatalf("unexpected request count = %d", requests.Load())
		}
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.httpClient = rewriteTargetHostClient(t, upstream, targetURL)

	result, err := server.ensureCached(context.Background(), targetURL, upstreamFetchOptions{})
	if err != nil {
		t.Fatalf("ensureCached returned error: %v", err)
	}
	if result.Status != "MISS" {
		t.Fatalf("ensureCached status = %q, want MISS", result.Status)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("upstream requests = %d, want 2", got)
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

func TestDefaultConfigUsesHigherUpstreamConcurrency(t *testing.T) {
	t.Setenv("UPSTREAM_CONCURRENCY", "")

	cfg := defaultConfig()
	if cfg.UpstreamConcurrency != 64 {
		t.Fatalf("default UpstreamConcurrency = %d, want 64", cfg.UpstreamConcurrency)
	}
}

func TestNewServerTunesTransportForHighConcurrency(t *testing.T) {
	t.Setenv("UPSTREAM_CONCURRENCY", "")

	cfg := defaultConfig()
	cfg.CacheDir = t.TempDir()
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}

	if got := cap(server.fetchSem); got != 64 {
		t.Fatalf("fetchSem cap = %d, want 64", got)
	}

	transport, ok := server.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("unexpected transport type")
	}
	if got := transport.MaxIdleConns; got != 256 {
		t.Fatalf("MaxIdleConns = %d, want 256", got)
	}
	if got := transport.MaxIdleConnsPerHost; got != 64 {
		t.Fatalf("MaxIdleConnsPerHost = %d, want 64", got)
	}
	if got := transport.MaxConnsPerHost; got != 64 {
		t.Fatalf("MaxConnsPerHost = %d, want 64", got)
	}
}

func TestConcurrentCacheHitsUseMemoryMetaCache(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.httpClient = upstream.Client()

	if _, err := server.ensureCached(context.Background(), upstream.URL+"/hot.png", upstreamFetchOptions{}); err != nil {
		t.Fatalf("warm cache returned error: %v", err)
	}

	var diskReads atomic.Int32
	server.metaDiskReadHook = func(string) {
		diskReads.Add(1)
	}

	const readers = 64
	var wg sync.WaitGroup
	results := make(chan cacheResult, readers)
	errs := make(chan error, readers)

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := server.ensureCached(context.Background(), upstream.URL+"/hot.png", upstreamFetchOptions{})
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}

	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent ensureCached returned error: %v", err)
	}
	for result := range results {
		if result.Status != "HIT" {
			t.Fatalf("concurrent hit status = %q, want HIT", result.Status)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want 1", got)
	}
	if got := diskReads.Load(); got != 0 {
		t.Fatalf("meta disk reads = %d, want 0", got)
	}
}

func TestHandleGetServesCachedBlobWithoutServeFile(t *testing.T) {
	server := newTestServer(t)
	targetURL := "https://example.com/static.png"
	options := server.resolveFetchOptions(targetURL, upstreamFetchOptions{})
	cacheKey := server.cacheKey(targetURL, options.CacheVariant)
	meta := Meta{
		SourceURL:   targetURL,
		ContentType: "image/png",
		Length:      int64(len(minimalPNG())),
		CreatedAt:   time.Now().UTC(),
		ETag:        buildETag(cacheKey, int64(len(minimalPNG())), time.Now().UTC()),
	}
	if err := os.MkdirAll(filepath.Dir(server.blobPath(cacheKey)), 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(server.blobPath(cacheKey), minimalPNG(), 0o644); err != nil {
		t.Fatalf("WriteFile blob returned error: %v", err)
	}
	if err := writeJSONAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeJSONAtomic returned error: %v", err)
	}
	server.writeMetaToMemory(cacheKey, meta)

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	rec := httptest.NewRecorder()
	server.handleGet(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, minimalPNG()) {
		t.Fatalf("GET body mismatch")
	}
	if got := rec.Header().Get("Content-Length"); got != "33" {
		t.Fatalf("Content-Length = %q, want 33", got)
	}
}

func TestEnsureCachedServesStaleAndRefreshesInBackground(t *testing.T) {
	var requests atomic.Int32
	refreshed := make(chan struct{}, 1)
	upstreamBody := atomic.Value{}
	upstreamBody.Store(minimalPNG())
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(upstreamBody.Load().([]byte))
		select {
		case refreshed <- struct{}{}:
		default:
		}
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.httpClient = upstream.Client()

	if _, err := server.ensureCached(context.Background(), upstream.URL+"/stale.png", upstreamFetchOptions{}); err != nil {
		t.Fatalf("warm ensureCached returned error: %v", err)
	}
	select {
	case <-refreshed:
	default:
	}
	options := server.resolveFetchOptions(upstream.URL+"/stale.png", upstreamFetchOptions{})
	cacheKey := server.cacheKey(upstream.URL+"/stale.png", options.CacheVariant)
	meta, ok := server.readMeta(cacheKey)
	if !ok {
		t.Fatal("expected cached meta")
	}
	meta.CreatedAt = time.Now().Add(-(server.cfg.CacheTTL + time.Minute)).UTC()
	if err := writeJSONAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeJSONAtomic returned error: %v", err)
	}
	server.writeMetaToMemory(cacheKey, meta)
	refreshedBody := append(append([]byte{}, minimalPNG()...), 0x00)
	upstreamBody.Store(refreshedBody)

	result, err := server.ensureCached(context.Background(), upstream.URL+"/stale.png", upstreamFetchOptions{})
	if err != nil {
		t.Fatalf("ensureCached stale returned error: %v", err)
	}
	if result.Status != "STALE" {
		t.Fatalf("stale status = %q, want STALE", result.Status)
	}

	select {
	case <-refreshed:
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not complete")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		updated, ok := server.readMeta(cacheKey)
		if ok && updated.CreatedAt.After(meta.CreatedAt) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected refreshed CreatedAt to advance")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := requests.Load(); got < 2 {
		t.Fatalf("upstream requests = %d, want at least 2", got)
	}
}

func TestEnsureCachedUsesConditionalRevalidation(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			w.Header().Set("Content-Type", "image/png")
			w.Header().Set("ETag", "\"upstream-etag\"")
			w.Header().Set("Last-Modified", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
			_, _ = w.Write(minimalPNG())
		case 2:
			if got := r.Header.Get("If-None-Match"); got != "\"upstream-etag\"" {
				t.Fatalf("If-None-Match = %q", got)
			}
			w.WriteHeader(http.StatusNotModified)
		default:
			t.Fatalf("unexpected request count = %d", requests.Load())
		}
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.httpClient = upstream.Client()

	if _, err := server.ensureCached(context.Background(), upstream.URL+"/etag.png", upstreamFetchOptions{}); err != nil {
		t.Fatalf("warm ensureCached returned error: %v", err)
	}
	options := server.resolveFetchOptions(upstream.URL+"/etag.png", upstreamFetchOptions{})
	cacheKey := server.cacheKey(upstream.URL+"/etag.png", options.CacheVariant)
	meta, _ := server.readMeta(cacheKey)
	meta.CreatedAt = time.Now().Add(-2 * server.cfg.CacheTTL).UTC()
	if err := writeJSONAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeJSONAtomic returned error: %v", err)
	}
	server.writeMetaToMemory(cacheKey, meta)
	server.cfg.StaleGracePeriod = 0

	result, err := server.ensureCached(context.Background(), upstream.URL+"/etag.png", upstreamFetchOptions{})
	if err != nil {
		t.Fatalf("ensureCached returned error: %v", err)
	}
	if result.Status != "REVALIDATED" {
		t.Fatalf("status = %q, want REVALIDATED", result.Status)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("upstream requests = %d, want 2", got)
	}
}

func TestPerHostConcurrencyLimitApplies(t *testing.T) {
	var active atomic.Int32
	var maxSeen atomic.Int32
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		for {
			max := maxSeen.Load()
			if current <= max || maxSeen.CompareAndSwap(max, current) {
				break
			}
		}
		<-release
		active.Add(-1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.httpClient = rewriteTargetHostClient(t, upstream, "https://img.example.com/a.png")
	server.cfg.PerHostConcurrency = 2
	server.fetchSem = make(chan struct{}, 8)

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := server.ensureCached(context.Background(), "https://img.example.com/"+strconv.Itoa(idx)+".png", upstreamFetchOptions{})
			errs <- err
		}(i)
	}

	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("ensureCached returned error: %v", err)
		}
	}
	if got := maxSeen.Load(); got > 2 {
		t.Fatalf("max concurrent upstream requests = %d, want <= 2", got)
	}
}

func TestMetaCacheLRUEvictsOldEntries(t *testing.T) {
	server := newTestServer(t)
	server.cfg.MetaCacheEntries = 1
	server.writeMetaToMemory("a", Meta{SourceURL: "a"})
	server.writeMetaToMemory("b", Meta{SourceURL: "b"})

	if _, ok := server.readMetaFromMemory("a"); ok {
		t.Fatal("expected oldest entry to be evicted")
	}
	if meta, ok := server.readMetaFromMemory("b"); !ok || meta.SourceURL != "b" {
		t.Fatal("expected newest entry to remain")
	}
}

func TestWarmMetaCacheLoadsDiskEntries(t *testing.T) {
	server := newTestServer(t)
	urlA := "https://example.com/a.png"
	options := server.resolveFetchOptions(urlA, upstreamFetchOptions{})
	cacheKey := server.cacheKey(urlA, options.CacheVariant)
	meta := Meta{
		SourceURL:   urlA,
		ContentType: "image/png",
		Length:      int64(len(minimalPNG())),
		CreatedAt:   time.Now().UTC(),
		ETag:        buildETag(cacheKey, int64(len(minimalPNG())), time.Now().UTC()),
	}
	if err := os.MkdirAll(filepath.Dir(server.blobPath(cacheKey)), 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(server.blobPath(cacheKey), minimalPNG(), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := writeJSONAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeJSONAtomic returned error: %v", err)
	}

	server.warmMetaCache(context.Background(), 1)
	if got := server.metaCacheLen(); got != 1 {
		t.Fatalf("meta cache len = %d, want 1", got)
	}
	if _, ok := server.readMetaFromMemory(cacheKey); !ok {
		t.Fatal("expected warmed meta in memory")
	}
}

func TestCleanupExpiredFilesProcessesOnlySelectedShards(t *testing.T) {
	server := newTestServer(t)

	makeExpired := func(rawURL string) string {
		options := server.resolveFetchOptions(rawURL, upstreamFetchOptions{})
		cacheKey := server.cacheKey(rawURL, options.CacheVariant)
		meta := Meta{
			SourceURL:   rawURL,
			ContentType: "image/png",
			Length:      int64(len(minimalPNG())),
			CreatedAt:   time.Now().Add(-2 * server.cfg.CacheTTL).UTC(),
			ETag:        buildETag(cacheKey, int64(len(minimalPNG())), time.Now().UTC()),
		}
		if err := os.MkdirAll(filepath.Dir(server.blobPath(cacheKey)), 0o755); err != nil {
			t.Fatalf("MkdirAll returned error: %v", err)
		}
		if err := os.WriteFile(server.blobPath(cacheKey), minimalPNG(), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
		if err := writeJSONAtomic(server.metaPath(cacheKey), meta); err != nil {
			t.Fatalf("writeJSONAtomic returned error: %v", err)
		}
		return cacheKey
	}

	cacheKeyA := makeExpired("https://example.com/a.png")
	cacheKeyB := makeExpired("https://another.example.com/b.png")
	shards := server.listShardDirs()
	if len(shards) < 2 {
		t.Fatalf("expected at least two shards, got %d", len(shards))
	}

	server.cleanupExpiredFiles(shards[:1])

	_, errA := os.Stat(server.metaPath(cacheKeyA))
	_, errB := os.Stat(server.metaPath(cacheKeyB))
	if os.IsNotExist(errA) == os.IsNotExist(errB) {
		t.Fatalf("expected only one selected shard to be cleaned; errA=%v errB=%v", errA, errB)
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	cacheDir := t.TempDir()
	server, err := NewServer(Config{
		CacheDir:            cacheDir,
		FetchTimeout:        5 * time.Second,
		MaxBodyBytes:        1 << 20,
		CacheTTL:            time.Hour,
		MaxCacheBytes:       1 << 30,
		UpstreamConcurrency: 4,
		PerHostConcurrency:  2,
		MetaCacheEntries:    32,
		StaleGracePeriod:    5 * time.Minute,
		WarmMetaEntries:     16,
		JanitorShardBatch:   1,
		AllowedSchemes: map[string]struct{}{
			"http":  {},
			"https": {},
		},
	})
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	server.httpClient = &http.Client{Timeout: 5 * time.Second}
	return server
}

func rewriteTargetHostClient(t *testing.T, upstream *httptest.Server, targetURL string) *http.Client {
	t.Helper()
	target, err := url.Parse(targetURL)
	if err != nil {
		t.Fatalf("parse target url: %v", err)
	}
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	baseTransport, ok := upstream.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatal("unexpected upstream transport type")
	}
	clone := baseTransport.Clone()
	clone.Proxy = nil
	client := &http.Client{
		Timeout: upstream.Client().Timeout,
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			rewritten := req.Clone(req.Context())
			rewritten.URL.Scheme = upstreamURL.Scheme
			rewritten.URL.Host = upstreamURL.Host
			rewritten.Host = target.Host
			return clone.RoundTrip(rewritten)
		}),
	}
	return client
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
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
