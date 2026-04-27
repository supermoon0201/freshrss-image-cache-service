package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestNormalizeURLRemovesFragment 验证 fragment 不参与缓存键和实际回源请求。
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

// TestNormalizeURLRejectsUnsupportedScheme 验证只允许配置里的协议。
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

// TestDecideContentTypeFallsBackToSniffing 验证没有合法 Content-Type 头时，
// 仍然能靠内容探测识别图片类型。
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

// TestValidateUpstreamBodyRejectsChallengePage 验证挑战页不会被误当成图片缓存。
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

// TestMatchUpstreamHeaderRuleWildcard 验证域名规则里的通配符可以正常命中。
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

// TestBuildForwardedOptionsRejectsDisallowedHeader 验证非白名单头不能透传到上游。
func TestBuildForwardedOptionsRejectsDisallowedHeader(t *testing.T) {
	server := &Server{}
	_, err := server.buildForwardedOptions("https://example.com/a.png", map[string]string{
		"X-Forwarded-For": "1.1.1.1",
	})
	if err == nil {
		t.Fatal("expected disallowed header rejection")
	}
}

// TestBuildForwardedOptionsRejectsSensitiveHost 验证敏感头只能发往白名单 host。
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

// TestEnsureCachedUsesRuleHeadersAndCachesResult 验证静态规则补头能够真正参与回源，
// 且首次 miss 后第二次能直接命中缓存。
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

// TestHandleGetForwardsSafeHeadersAndCachesResult 验证 GET 请求会继承允许的安全头，
// 并在首次抓取后写入缓存供第二次命中。
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
	if got := rec.Header().Get("Cache-Control"); got != "public, max-age=3600" {
		t.Fatalf("first GET Cache-Control = %q, want public, max-age=3600", got)
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

// TestHandleGetIgnoresUserAgentAndLanguageForSharedCache 验证共享缓存分片默认不再被
// User-Agent / Accept-Language 切开，只要 Referer / Origin 语义相同就应复用缓存。
func TestHandleGetIgnoresUserAgentAndLanguageForSharedCache(t *testing.T) {
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

	firstReq := httptest.NewRequest(http.MethodGet, "/piccache?url="+upstream.URL+"/hello2.png", nil)
	firstReq.Header.Set("Referer", "https://hellogithub.com/")
	firstReq.Header.Set("Origin", "https://hellogithub.com")
	firstReq.Header.Set("User-Agent", "Mozilla/5.0 test-a")
	firstReq.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
	firstRec := httptest.NewRecorder()
	server.handleGet(firstRec, firstReq)

	if firstRec.Code != http.StatusOK {
		t.Fatalf("first GET status = %d, want 200; body=%s", firstRec.Code, firstRec.Body.String())
	}
	if got := firstRec.Header().Get("X-Piccache-Status"); got != "MISS" {
		t.Fatalf("first GET X-Piccache-Status = %q, want MISS", got)
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/piccache?url="+upstream.URL+"/hello2.png", nil)
	secondReq.Header.Set("Referer", "https://hellogithub.com/")
	secondReq.Header.Set("Origin", "https://hellogithub.com")
	secondReq.Header.Set("User-Agent", "Mozilla/5.0 test-b")
	secondReq.Header.Set("Accept-Language", "en-US,en;q=0.8")
	secondRec := httptest.NewRecorder()
	server.handleGet(secondRec, secondReq)

	if secondRec.Code != http.StatusOK {
		t.Fatalf("second GET status = %d, want 200; body=%s", secondRec.Code, secondRec.Body.String())
	}
	if got := secondRec.Header().Get("X-Piccache-Status"); got != "HIT" {
		t.Fatalf("second GET X-Piccache-Status = %q, want HIT", got)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want 1", got)
	}
}

// TestHandleGetKeepsRefererOriginInSharedCacheVariant 验证共享缓存仍然需要保留
// Referer / Origin 语义，防止不同来源头互相污染。
func TestHandleGetKeepsRefererOriginInSharedCacheVariant(t *testing.T) {
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

	firstReq := httptest.NewRequest(http.MethodGet, "/piccache?url="+upstream.URL+"/hello3.png", nil)
	firstReq.Header.Set("Referer", "https://hellogithub.com/")
	firstReq.Header.Set("Origin", "https://hellogithub.com")
	firstRec := httptest.NewRecorder()
	server.handleGet(firstRec, firstReq)

	if firstRec.Code != http.StatusOK {
		t.Fatalf("first GET status = %d, want 200; body=%s", firstRec.Code, firstRec.Body.String())
	}
	if got := firstRec.Header().Get("X-Piccache-Status"); got != "MISS" {
		t.Fatalf("first GET X-Piccache-Status = %q, want MISS", got)
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/piccache?url="+upstream.URL+"/hello3.png", nil)
	secondReq.Header.Set("Referer", "https://another.example/")
	secondReq.Header.Set("Origin", "https://another.example")
	secondRec := httptest.NewRecorder()
	server.handleGet(secondRec, secondReq)

	if secondRec.Code != http.StatusBadGateway {
		t.Fatalf("second GET status = %d, want 502; body=%s", secondRec.Code, secondRec.Body.String())
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("upstream requests = %d, want 2", got)
	}
}

// TestHandleHeadFetchesAndCachesWithoutBody 验证 HEAD 和 GET 走同一套缓存逻辑，
// 只是最终不返回 body。
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
	if got := rec.Header().Get("Cache-Control"); got != "public, max-age=3600" {
		t.Fatalf("first HEAD Cache-Control = %q, want public, max-age=3600", got)
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

// TestInferUpstreamHeadersFromURL 验证自动推导的 Referer / Origin 是否符合预期。
func TestInferUpstreamHeadersFromURL(t *testing.T) {
	headers := inferUpstreamHeaders("https://img.hellogithub.com/i/z5ncAjLHSpGDTr1_1771822196.png")
	if got := headers["Referer"]; got != "https://hellogithub.com/" {
		t.Fatalf("Referer = %q, want %q", got, "https://hellogithub.com/")
	}
	if got := headers["Origin"]; got != "https://hellogithub.com" {
		t.Fatalf("Origin = %q, want %q", got, "https://hellogithub.com")
	}
}

// TestHandleGetInfersHeadersFromTargetURL 验证调用方不显式传 Referer / Origin 时，
// 服务仍可根据目标 URL 自动推导并抓取成功。
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

// TestFetchAndStoreFallsBackToHostRootHeadersOn403 验证自动推导头第一次失败后，
// 服务会按预期回退到目标 host 根路径再尝试一次。
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

// TestEnsureCachedWithSensitiveForwardedHeadersBypassesSharedCache 验证带 Cookie 的抓取
// 不会污染共享磁盘缓存，因此连续两次请求都会真正回源。
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

// TestHandlePostWarmupSharesCacheKeyWithSubsequentGet 验证 POST 预热和后续 GET
// 在未显式提供上游头时，能够落到同一份共享缓存，而不是再次 MISS 回源。
func TestHandlePostWarmupSharesCacheKeyWithSubsequentGet(t *testing.T) {
	var requests atomic.Int32
	targetURL := "https://23img.com/i/2026/04/22/demo.jpg"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Referer"); got != "https://23img.com/" {
			http.Error(w, "invalid referer", http.StatusForbidden)
			return
		}
		if got := r.Header.Get("Origin"); got != "https://23img.com" {
			http.Error(w, "invalid origin", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.cfg.AccessToken = "test-token"
	server.httpClient = rewriteTargetHostClient(t, upstream, targetURL)

	postBody := bytes.NewBufferString(`{"url":"` + targetURL + `","access_token":"test-token"}`)
	postReq := httptest.NewRequest(http.MethodPost, "/piccache", postBody)
	postReq.Header.Set("Content-Type", "application/json")
	postRec := httptest.NewRecorder()

	server.handlePost(postRec, postReq)

	if postRec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, want 200; body=%s", postRec.Code, postRec.Body.String())
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests after POST = %d, want 1", got)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/piccache?url="+url.QueryEscape(targetURL), nil)
	getRec := httptest.NewRecorder()
	server.handleGet(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body=%s", getRec.Code, getRec.Body.String())
	}
	if got := getRec.Header().Get("X-Piccache-Status"); got != "HIT" {
		t.Fatalf("GET X-Piccache-Status = %q, want HIT", got)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests after GET = %d, want 1", got)
	}
}

// TestHandlePostWarmupMatchesGetWhenRefererOriginEqualInferred 验证 POST 预热走自动推导时，
// 后续 GET 即使显式带了与推导结果相同的 Referer / Origin，也应命中同一份缓存。
func TestHandlePostWarmupMatchesGetWhenRefererOriginEqualInferred(t *testing.T) {
	var requests atomic.Int32
	targetURL := "https://23img.com/i/2026/04/22/zcxcj9.jpg"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Referer"); got != "https://23img.com/" {
			http.Error(w, "invalid referer", http.StatusForbidden)
			return
		}
		if got := r.Header.Get("Origin"); got != "https://23img.com" {
			http.Error(w, "invalid origin", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(minimalPNG())
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.cfg.AccessToken = "test-token"
	server.httpClient = rewriteTargetHostClient(t, upstream, targetURL)

	postBody := bytes.NewBufferString(`{"url":"` + targetURL + `","access_token":"test-token"}`)
	postReq := httptest.NewRequest(http.MethodPost, "/piccache", postBody)
	postReq.Header.Set("Content-Type", "application/json")
	postRec := httptest.NewRecorder()
	server.handlePost(postRec, postReq)

	if postRec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, want 200; body=%s", postRec.Code, postRec.Body.String())
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests after POST = %d, want 1", got)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/piccache?url="+url.QueryEscape(targetURL), nil)
	getReq.Header.Set("Referer", "https://23img.com/")
	getReq.Header.Set("Origin", "https://23img.com")
	getRec := httptest.NewRecorder()
	server.handleGet(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body=%s", getRec.Code, getRec.Body.String())
	}
	if got := getRec.Header().Get("X-Piccache-Status"); got != "HIT" {
		t.Fatalf("GET X-Piccache-Status = %q, want HIT", got)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests after GET = %d, want 1", got)
	}
}

// TestDefaultConfigUsesHigherUpstreamConcurrency 锁定新的默认全局并发值。
func TestDefaultConfigUsesHigherUpstreamConcurrency(t *testing.T) {
	t.Setenv("UPSTREAM_CONCURRENCY", "")

	cfg := defaultConfig()
	if cfg.UpstreamConcurrency != 64 {
		t.Fatalf("default UpstreamConcurrency = %d, want 64", cfg.UpstreamConcurrency)
	}
}

// TestNewServerTunesTransportForHighConcurrency 验证 NewServer 会按默认并发调大连接池，
// 防止后续有人把 transport 又改回保守配置。
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

// TestLoadConfigFromEnvRejectsInvalidDuration 验证启动阶段遇到坏格式 duration 会直接报错，
// 防止服务悄悄带着默认值启动。
func TestLoadConfigFromEnvRejectsInvalidDuration(t *testing.T) {
	t.Setenv("ACCESS_TOKEN", "test-token")
	t.Setenv("FETCH_TIMEOUT", "15seconds???")

	_, err := loadConfigFromEnv()
	if err == nil {
		t.Fatal("loadConfigFromEnv unexpectedly accepted invalid duration")
	}
	if !strings.Contains(err.Error(), "FETCH_TIMEOUT") {
		t.Fatalf("loadConfigFromEnv error = %v, want FETCH_TIMEOUT context", err)
	}
}

// TestLoadConfigFromEnvRejectsInvalidUpstreamHeaderRulesJSON 验证规则 JSON 配错时启动会失败，
// 避免静默忽略导致线上行为和预期不一致。
func TestLoadConfigFromEnvRejectsInvalidUpstreamHeaderRulesJSON(t *testing.T) {
	t.Setenv("ACCESS_TOKEN", "test-token")
	t.Setenv("UPSTREAM_HEADER_RULES_JSON", `[{]`)

	_, err := loadConfigFromEnv()
	if err == nil {
		t.Fatal("loadConfigFromEnv unexpectedly accepted invalid upstream header rules json")
	}
	if !strings.Contains(err.Error(), "UPSTREAM_HEADER_RULES_JSON") {
		t.Fatalf("loadConfigFromEnv error = %v, want UPSTREAM_HEADER_RULES_JSON context", err)
	}
}

// TestNewServerNormalizesWarmMetaEntries 验证非法的预热条目上限会回退到默认值，
// 防止负数把启动预热放大成“全量扫描”。
func TestNewServerNormalizesWarmMetaEntries(t *testing.T) {
	cfg := defaultConfig()
	cfg.CacheDir = t.TempDir()
	cfg.WarmMetaEntries = -1

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	if got := server.cfg.WarmMetaEntries; got != 2048 {
		t.Fatalf("WarmMetaEntries = %d, want 2048", got)
	}
}

// TestSafeDialContextUsesValidatedIPAddress 验证 safeDialContext 不会在校验后
// 再把原始域名交给底层拨号，从而避免 DNS 重解析绕过 SSRF 防护。
func TestSafeDialContextUsesValidatedIPAddress(t *testing.T) {
	originalLookup := lookupNetIP
	originalDial := dialContext
	t.Cleanup(func() {
		lookupNetIP = originalLookup
		dialContext = originalDial
	})

	lookupNetIP = func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		if network != "ip" {
			t.Fatalf("lookup network = %q, want ip", network)
		}
		if host != "example.com" {
			t.Fatalf("lookup host = %q, want example.com", host)
		}
		return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
	}

	var dialAddress string
	dialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialAddress = address
		return nil, errors.New("stop after capture")
	}

	_, err := safeDialContext(context.Background(), "tcp", "example.com:443")
	if err == nil || err.Error() != "stop after capture" {
		t.Fatalf("safeDialContext error = %v, want injected dial error", err)
	}
	if dialAddress != "93.184.216.34:443" {
		t.Fatalf("dial address = %q, want 93.184.216.34:443", dialAddress)
	}
}

// TestSafeDialContextRejectsPrivateAddress 验证解析结果里只要是私网地址就直接拒绝，
// 不会继续进入实际拨号。
func TestSafeDialContextRejectsPrivateAddress(t *testing.T) {
	originalLookup := lookupNetIP
	originalDial := dialContext
	t.Cleanup(func() {
		lookupNetIP = originalLookup
		dialContext = originalDial
	})

	lookupNetIP = func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}

	dialCalled := false
	dialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialCalled = true
		return nil, errors.New("unexpected dial")
	}

	_, err := safeDialContext(context.Background(), "tcp", "localhost:80")
	if !errors.Is(err, errPrivateAddress) {
		t.Fatalf("safeDialContext error = %v, want errPrivateAddress", err)
	}
	if dialCalled {
		t.Fatal("dialContext was called for private address")
	}
}

// TestRunJanitorStopsOnContextCancel 验证后台 janitor 能在 context 取消后及时退出，
// 为优雅停机提供基本保障。
func TestRunJanitorStopsOnContextCancel(t *testing.T) {
	server := newTestServer(t)
	server.cfg.JanitorInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.runJanitor(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runJanitor did not stop after context cancellation")
	}
}

// TestRunShutsDownHTTPServerOnContextCancel 验证 run(ctx, cfg) 在 context 取消时
// 会触发 HTTP server 的 Shutdown，并在合理时间内正常退出。
func TestRunShutsDownHTTPServerOnContextCancel(t *testing.T) {
	originalServeHTTP := serveHTTP
	originalShutdownHTTP := shutdownHTTP
	t.Cleanup(func() {
		serveHTTP = originalServeHTTP
		shutdownHTTP = originalShutdownHTTP
	})

	listenStarted := make(chan struct{})
	shutdownCalled := make(chan struct{})
	serveReleased := make(chan struct{})

	serveHTTP = func(server *http.Server) error {
		close(listenStarted)
		<-serveReleased
		return http.ErrServerClosed
	}
	shutdownHTTP = func(server *http.Server, ctx context.Context) error {
		close(shutdownCalled)
		close(serveReleased)
		return nil
	}

	cfg := defaultConfig()
	cfg.CacheDir = t.TempDir()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.AccessToken = "test-token"
	cfg.WarmMetaOnStart = false
	cfg.JanitorInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg)
	}()

	select {
	case <-listenStarted:
	case <-time.After(time.Second):
		t.Fatal("run did not start HTTP serving")
	}

	cancel()

	select {
	case <-shutdownCalled:
	case <-time.After(time.Second):
		t.Fatal("run did not call Shutdown after context cancellation")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not exit after shutdown")
	}
}

// TestRunRejectsDefaultAccessToken 验证服务不会带着公开占位 token 启动。
func TestRunRejectsDefaultAccessToken(t *testing.T) {
	cfg := defaultConfig()
	cfg.CacheDir = t.TempDir()
	cfg.AccessToken = defaultAccessToken

	err := run(context.Background(), cfg)
	if err == nil {
		t.Fatal("run unexpectedly accepted default access token")
	}
	if !strings.Contains(err.Error(), "ACCESS_TOKEN") {
		t.Fatalf("run error = %v, want ACCESS_TOKEN context", err)
	}
}

// TestConcurrentCacheHitsUseMemoryMetaCache 验证热点命中优先走内存元数据，
// 而不是每次都去读磁盘上的 .meta。
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

// TestMetaFileRoundTrip 验证新的紧凑元数据格式可以正确写回并读出。
func TestMetaFileRoundTrip(t *testing.T) {
	server := newTestServer(t)
	cacheKey := strings.Repeat("a", 64)
	want := Meta{
		SourceURL:            "https://example.com/a.png",
		ContentType:          "image/png",
		Length:               12345,
		CreatedAt:            time.Now().UTC().Truncate(time.Nanosecond),
		ETag:                 "\"etag\"",
		UpstreamETag:         "\"upstream-etag\"",
		UpstreamLastModified: time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat),
	}

	if err := writeMetaAtomic(server.metaPath(cacheKey), want); err != nil {
		t.Fatalf("writeMetaAtomic returned error: %v", err)
	}

	got, err := readMetaFile(server.metaPath(cacheKey))
	if err != nil {
		t.Fatalf("readMetaFile returned error: %v", err)
	}
	if got != want {
		t.Fatalf("meta round trip mismatch: got %+v want %+v", got, want)
	}
}

// TestHandleGetServesCachedBlobWithoutServeFile 验证热路径改成“自己打开缓存文件并流式返回”后，
// 仍然能正确输出 body 和 Content-Length。
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
	if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeMetaAtomic returned error: %v", err)
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

// TestHandleGetFreshHitSkipsEnsureCached 验证“未过期的本地命中”会直接走快速路径，
// 不再进入 ensureCached 和 singleflight 慢路径。
func TestHandleGetFreshHitSkipsEnsureCached(t *testing.T) {
	server := newTestServer(t)
	targetURL := "https://example.com/fresh.png"
	options := server.resolveFetchOptions(targetURL, upstreamFetchOptions{})
	cacheKey := server.cacheKey(targetURL, options.CacheVariant)
	body := minimalPNG()
	now := time.Now().UTC()
	meta := Meta{
		SourceURL:   targetURL,
		ContentType: "image/png",
		Length:      int64(len(body)),
		CreatedAt:   now,
		ETag:        buildETag(cacheKey, int64(len(body)), now),
	}
	if err := os.MkdirAll(filepath.Dir(server.blobPath(cacheKey)), 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(server.blobPath(cacheKey), body, 0o644); err != nil {
		t.Fatalf("WriteFile blob returned error: %v", err)
	}
	if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeMetaAtomic returned error: %v", err)
	}
	server.writeMetaToMemory(cacheKey, meta)

	var ensureCalls atomic.Int32
	server.ensureCachedHook = func(string) {
		ensureCalls.Add(1)
	}

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	rec := httptest.NewRecorder()
	server.handleGet(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := ensureCalls.Load(); got != 0 {
		t.Fatalf("ensureCached calls = %d, want 0", got)
	}
	if got := rec.Header().Get("X-Piccache-Status"); got != "HIT" {
		t.Fatalf("X-Piccache-Status = %q, want HIT", got)
	}
}

// TestHandleGetCachesSmallBlobInMemory 验证热点小文件第一次读盘后会进入内存 blob LRU，
// 第二次命中直接从内存返回，不再重复读磁盘。
func TestHandleGetCachesSmallBlobInMemory(t *testing.T) {
	server := newTestServer(t)
	server.cfg.BlobCacheEntries = 8
	server.cfg.BlobCacheMaxBytes = 1 << 20
	server.cfg.BlobFileMaxBytes = 1 << 20

	targetURL := "https://example.com/hot-small.png"
	options := server.resolveFetchOptions(targetURL, upstreamFetchOptions{})
	cacheKey := server.cacheKey(targetURL, options.CacheVariant)
	body := minimalPNG()
	now := time.Now().UTC()
	meta := Meta{
		SourceURL:   targetURL,
		ContentType: "image/png",
		Length:      int64(len(body)),
		CreatedAt:   now,
		ETag:        buildETag(cacheKey, int64(len(body)), now),
	}
	if err := os.MkdirAll(filepath.Dir(server.blobPath(cacheKey)), 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(server.blobPath(cacheKey), body, 0o644); err != nil {
		t.Fatalf("WriteFile blob returned error: %v", err)
	}
	if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeMetaAtomic returned error: %v", err)
	}
	server.writeMetaToMemory(cacheKey, meta)

	var diskReads atomic.Int32
	server.blobDiskReadHook = func(string) {
		diskReads.Add(1)
	}

	firstReq := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	firstRec := httptest.NewRecorder()
	server.handleGet(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first GET status = %d, want 200; body=%s", firstRec.Code, firstRec.Body.String())
	}
	if got := diskReads.Load(); got != 1 {
		t.Fatalf("disk reads after first GET = %d, want 1", got)
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	secondRec := httptest.NewRecorder()
	server.handleGet(secondRec, secondReq)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second GET status = %d, want 200; body=%s", secondRec.Code, secondRec.Body.String())
	}
	if got := secondRec.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("second GET body mismatch")
	}
	if got := diskReads.Load(); got != 1 {
		t.Fatalf("disk reads after second GET = %d, want 1", got)
	}
}

// TestEnsureCachedServesStaleAndRefreshesInBackground 验证 stale-while-revalidate：
// 请求先拿到旧内容，后台再异步刷新元数据和文件。
func TestEnsureCachedServesStaleAndRefreshesInBackground(t *testing.T) {
	var requests atomic.Int32
	// 用一个 channel 观察后台刷新是否真的发生。
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
	if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeMetaAtomic returned error: %v", err)
	}
	server.writeMetaToMemory(cacheKey, meta)
	// 改一下上游 body，让后台刷新确实有新内容可写。
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

	// 后台刷新是异步的，这里用短轮询等待元数据时间戳更新。
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

// TestEnsureCachedUsesConditionalRevalidation 验证本地已有旧缓存时，
// 会带上条件请求头去拿 304，而不是整文件重下。
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
	if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeMetaAtomic returned error: %v", err)
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

// TestPerHostConcurrencyLimitApplies 验证“全局并发之外，再加单 host 并发上限”
// 这条保护是否真的生效。
func TestPerHostConcurrencyLimitApplies(t *testing.T) {
	var active atomic.Int32
	var maxSeen atomic.Int32
	// release 用来故意把上游请求卡住，方便统计同时在跑多少个请求。
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

// TestMetaCacheLRUEvictsOldEntries 验证元数据缓存达到上限后会淘汰最旧条目。
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

// TestWarmMetaCacheLoadsDiskEntries 验证启动预热逻辑能把磁盘上的元数据读回内存 LRU。
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
	if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
		t.Fatalf("writeMetaAtomic returned error: %v", err)
	}

	server.warmMetaCache(context.Background(), 1)
	if got := server.metaCacheLen(); got != 1 {
		t.Fatalf("meta cache len = %d, want 1", got)
	}
	if _, ok := server.readMetaFromMemory(cacheKey); !ok {
		t.Fatal("expected warmed meta in memory")
	}
}

// TestCleanupExpiredFilesProcessesOnlySelectedShards 验证 janitor 的过期清理
// 是按选中的 shard 分批处理，而不是每次全盘粗扫。
func TestCleanupExpiredFilesProcessesOnlySelectedShards(t *testing.T) {
	server := newTestServer(t)

	// makeExpired 帮我们快速造一个“磁盘上存在但已经过期”的缓存项。
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
		if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
			t.Fatalf("writeMetaAtomic returned error: %v", err)
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

// TestCleanupExpiredFilesOnStartProcessesAllShards 验证启动清理会全量处理所有 shard。
// 即使常规 janitor 被配置成每轮只扫 1 个 shard，容器启动后的这次清理也应尽快清空所有过期项。
func TestCleanupExpiredFilesOnStartProcessesAllShards(t *testing.T) {
	server := newTestServer(t)
	server.cfg.JanitorShardBatch = 1

	makeEntry := func(rawURL string, createdAt time.Time) string {
		options := server.resolveFetchOptions(rawURL, upstreamFetchOptions{})
		cacheKey := server.cacheKey(rawURL, options.CacheVariant)
		meta := Meta{
			SourceURL:   rawURL,
			ContentType: "image/png",
			Length:      int64(len(minimalPNG())),
			CreatedAt:   createdAt.UTC(),
			ETag:        buildETag(cacheKey, int64(len(minimalPNG())), createdAt.UTC()),
		}
		if err := os.MkdirAll(filepath.Dir(server.blobPath(cacheKey)), 0o755); err != nil {
			t.Fatalf("MkdirAll returned error: %v", err)
		}
		if err := os.WriteFile(server.blobPath(cacheKey), minimalPNG(), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
		if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
			t.Fatalf("writeMetaAtomic returned error: %v", err)
		}
		return cacheKey
	}

	expiredAt := time.Now().Add(-2 * server.cfg.CacheTTL)
	freshAt := time.Now()
	expiredA := makeEntry("https://example.com/startup-a.png", expiredAt)
	expiredB := makeEntry("https://another.example.com/startup-b.png", expiredAt)
	fresh := makeEntry("https://fresh.example.com/startup-c.png", freshAt)
	if len(server.listShardDirs()) < 2 {
		t.Fatalf("expected at least two shards, got %d", len(server.listShardDirs()))
	}

	server.cleanupExpiredFilesOnStart()

	for _, cacheKey := range []string{expiredA, expiredB} {
		if _, err := os.Stat(server.metaPath(cacheKey)); !os.IsNotExist(err) {
			t.Fatalf("expected expired meta %s to be removed, stat err = %v", cacheKey, err)
		}
		if _, err := os.Stat(server.blobPath(cacheKey)); !os.IsNotExist(err) {
			t.Fatalf("expected expired blob %s to be removed, stat err = %v", cacheKey, err)
		}
	}
	if _, err := os.Stat(server.metaPath(fresh)); err != nil {
		t.Fatalf("expected fresh meta to remain, stat err = %v", err)
	}
	if _, err := os.Stat(server.blobPath(fresh)); err != nil {
		t.Fatalf("expected fresh blob to remain, stat err = %v", err)
	}
}

// newTestServer 为测试创建一套尽量轻量、但行为接近生产的 Server。
// 它会走真实的 NewServer，避免测试绕过初始化逻辑。
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

// rewriteTargetHostClient 用于把“逻辑上访问某个真实域名”的请求改写到本地 httptest server。
// 这样可以同时保留目标 URL 的 host 信息，又不需要真的访问公网。
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

// minimalPNG 返回一段最小可识别的 PNG 字节，用作测试桩。
func minimalPNG() []byte {
	return []byte{
		0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n',
		0x00, 0x00, 0x00, 0x0d, 'I', 'H', 'D', 'R',
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
		0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53, 0xde,
	}
}

// readCloser 是一个只读一次的测试桩，用来模拟简单响应体。
type readCloser struct {
	data []byte
	read bool
}

// roundTripFunc 让普通函数也能实现 http.RoundTripper，方便测试时自定义客户端行为。
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

// TestMain 在所有测试结束后清理仓库根目录下可能残留的 data 目录。
func TestMain(m *testing.M) {
	code := m.Run()
	_ = os.RemoveAll(filepath.Join(".", "data"))
	os.Exit(code)
}
