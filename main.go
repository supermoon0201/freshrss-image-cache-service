package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/publicsuffix"
	"golang.org/x/sync/singleflight"
)

// Config 定义服务配置。
type Config struct {
	ListenAddr          string
	CacheDir            string
	AccessToken         string
	FetchTimeout        time.Duration
	MaxBodyBytes        int64
	CacheTTL            time.Duration
	JanitorInterval     time.Duration
	MaxCacheBytes       int64
	UpstreamConcurrency int
	AllowedSchemes      map[string]struct{}
	UpstreamHeaderRules []UpstreamHeaderRule
	CredentialedHosts   []string
}

type UpstreamHeaderRule struct {
	Name           string            `json:"name"`
	Hosts          []string          `json:"hosts"`
	Referer        string            `json:"referer"`
	Origin         string            `json:"origin"`
	UserAgent      string            `json:"user_agent"`
	AcceptLanguage string            `json:"accept_language"`
	Headers        map[string]string `json:"headers"`
}

// Meta 存放缓存元数据。
type Meta struct {
	SourceURL   string    `json:"source_url"`
	ContentType string    `json:"content_type"`
	Length      int64     `json:"length"`
	CreatedAt   time.Time `json:"created_at"`
	ETag        string    `json:"etag"`
}

// Server 是图片缓存服务。
type Server struct {
	cfg        Config
	httpClient *http.Client
	group      singleflight.Group
	fetchSem   chan struct{}
}

type cacheResult struct {
	Meta   Meta
	Status string
}

// proactiveRequest 对应 FreshRSS proactive cache 的请求体。
type proactiveRequest struct {
	URL             string            `json:"url"`
	AccessToken     string            `json:"access_token"`
	UpstreamHeaders map[string]string `json:"upstream_headers"`
}

type fetchMode string

const (
	fetchModeAnonymous fetchMode = "anonymous"
	fetchModeRule      fetchMode = "rule"
	fetchModeForwarded fetchMode = "forwarded"
	fetchModeInferred  fetchMode = "inferred"
)

type upstreamFetchOptions struct {
	Headers      map[string]string
	CacheVariant string
	Cacheable    bool
	Mode         fetchMode
	RetryHeaders []map[string]string
}

type cacheEntry struct {
	Key        string
	BlobPath   string
	MetaPath   string
	Length     int64
	ModifiedAt time.Time
}

var (
	errPrivateAddress      = errors.New("target resolves to private or loopback address")
	errUnexpectedMediaType = errors.New("upstream response is not an image or video")
	errUpstreamChallenge   = errors.New("upstream returned a likely challenge page")
)

func main() {
	cfg := Config{
		ListenAddr:          envOrDefault("LISTEN_ADDR", "127.0.0.1:9090"),
		CacheDir:            envOrDefault("CACHE_DIR", "./data/cache"),
		AccessToken:         envOrDefault("ACCESS_TOKEN", "change-me"),
		FetchTimeout:        parseDurationOrDefault("FETCH_TIMEOUT", 15*time.Second),
		MaxBodyBytes:        parseInt64OrDefault("MAX_BODY_BYTES", 20<<20), // 20MB
		CacheTTL:            parseDurationOrDefault("CACHE_TTL", 30*24*time.Hour),
		JanitorInterval:     parseDurationOrDefault("JANITOR_INTERVAL", time.Hour),
		MaxCacheBytes:       parseInt64OrDefault("MAX_CACHE_BYTES", 10<<30), // 10GB
		UpstreamConcurrency: int(parseInt64OrDefault("UPSTREAM_CONCURRENCY", 16)),
		AllowedSchemes:      map[string]struct{}{"http": {}, "https": {}},
		UpstreamHeaderRules: parseUpstreamHeaderRules("UPSTREAM_HEADER_RULES_JSON"),
		CredentialedHosts:   parseCSVEnv("CREDENTIAL_FORWARD_HOSTS"),
	}

	server, err := NewServer(cfg)
	if err != nil {
		log.Fatalf("init server failed: %v", err)
	}

	go server.runJanitor(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", server.handleHealth)
	mux.HandleFunc("/piccache", server.handlePiccache)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("piccache listening on %s", cfg.ListenAddr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen failed: %v", err)
	}
}

func NewServer(cfg Config) (*Server, error) {
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir cache dir: %w", err)
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           safeDialContext,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}

	return &Server{
		cfg: cfg,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   cfg.FetchTimeout,
		},
		fetchSem: make(chan struct{}, cfg.UpstreamConcurrency),
	}, nil
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handlePiccache(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r)
	case http.MethodHead:
		s.handleHead(w, r)
	case http.MethodPost:
		s.handlePost(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	s.handleRead(w, r, false)
}

func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	s.handleRead(w, r, true)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request, headOnly bool) {
	rawURL := strings.TrimSpace(r.URL.Query().Get("url"))
	if rawURL == "" {
		http.Error(w, "missing url", http.StatusBadRequest)
		return
	}

	normalizedURL, err := s.normalizeURL(rawURL)
	if err != nil {
		http.Error(w, "invalid url", http.StatusBadRequest)
		return
	}

	forwardedOptions, err := s.buildForwardedOptionsFromRequest(normalizedURL, r.Header, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	effective := s.resolveFetchOptions(normalizedURL, forwardedOptions)
	cacheKey := s.cacheKey(normalizedURL, effective.CacheVariant)

	value, err, shared := s.group.Do(cacheKey, func() (any, error) {
		return s.ensureCached(r.Context(), normalizedURL, forwardedOptions)
	})
	if err != nil {
		statusCode := http.StatusBadGateway
		if errors.Is(err, errPrivateAddress) {
			statusCode = http.StatusForbidden
		}
		http.Error(w, err.Error(), statusCode)
		return
	}

	result := value.(cacheResult)
	if shared {
		w.Header().Set("X-Piccache-Status", "SHARED")
	} else {
		w.Header().Set("X-Piccache-Status", result.Status)
	}

	w.Header().Set("Content-Type", result.Meta.ContentType)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	if result.Meta.ETag != "" {
		w.Header().Set("ETag", result.Meta.ETag)
		if match := strings.TrimSpace(r.Header.Get("If-None-Match")); match == result.Meta.ETag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.Header().Set("Last-Modified", result.Meta.CreatedAt.UTC().Format(http.TimeFormat))

	if headOnly {
		w.Header().Set("Content-Length", strconv.FormatInt(result.Meta.Length, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	http.ServeFile(w, r, s.blobPath(cacheKey))
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req proactiveRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.AccessToken != s.cfg.AccessToken {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	normalizedURL, err := s.normalizeURL(strings.TrimSpace(req.URL))
	if err != nil {
		http.Error(w, "invalid url", http.StatusBadRequest)
		return
	}

	forwardedOptions, err := s.buildForwardedOptions(normalizedURL, req.UpstreamHeaders)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if _, err := s.ensureCached(r.Context(), normalizedURL, forwardedOptions); err != nil {
		statusCode := http.StatusBadGateway
		if errors.Is(err, errPrivateAddress) {
			statusCode = http.StatusForbidden
		}
		http.Error(w, err.Error(), statusCode)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "OK"})
}

func (s *Server) normalizeURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Host == "" {
		return "", errors.New("missing host")
	}
	if _, ok := s.cfg.AllowedSchemes[strings.ToLower(parsed.Scheme)]; !ok {
		return "", errors.New("unsupported scheme")
	}
	parsed.Fragment = ""
	return parsed.String(), nil
}

func (s *Server) ensureCached(ctx context.Context, normalizedURL string, forwarded upstreamFetchOptions) (cacheResult, error) {
	effective := s.resolveFetchOptions(normalizedURL, forwarded)
	if !effective.Cacheable {
		meta, err := s.fetchAndStore(ctx, normalizedURL, effective)
		if err != nil {
			return cacheResult{}, err
		}
		return cacheResult{Meta: meta, Status: "MISS"}, nil
	}

	cacheKey := s.cacheKey(normalizedURL, effective.CacheVariant)

	// 命中且未过期，直接返回。
	if meta, ok := s.readMeta(cacheKey); ok && !s.isExpired(meta) {
		return cacheResult{Meta: meta, Status: "HIT"}, nil
	}

	// 限制并发抓取数，避免上游被你自己打爆。
	select {
	case s.fetchSem <- struct{}{}:
		defer func() { <-s.fetchSem }()
	case <-ctx.Done():
		return cacheResult{}, ctx.Err()
	}

	// 双检，避免并发下重复下载。
	if meta, ok := s.readMeta(cacheKey); ok && !s.isExpired(meta) {
		return cacheResult{Meta: meta, Status: "HIT"}, nil
	}

	meta, err := s.fetchAndStore(ctx, normalizedURL, effective)
	if err != nil {
		return cacheResult{}, err
	}
	return cacheResult{Meta: meta, Status: "MISS"}, nil
}

func (s *Server) fetchAndStore(ctx context.Context, normalizedURL string, options upstreamFetchOptions) (Meta, error) {
	attemptHeaders := append([]map[string]string{cloneHeaders(options.Headers)}, cloneHeaderList(options.RetryHeaders)...)
	resp, usedHeaders, err := s.doUpstreamRequestWithRetries(ctx, normalizedURL, attemptHeaders, options.Mode)
	if err != nil {
		return Meta{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Meta{}, fmt.Errorf("upstream status: %s", resp.Status)
	}

	contentType, bodyReader, err := validateUpstreamBody(resp)
	if err != nil {
		return Meta{}, err
	}

	cacheVariant := options.CacheVariant
	if cacheVariant == "" {
		cacheVariant = buildCacheVariant(usedHeaders)
	}
	cacheKey := s.cacheKey(normalizedURL, cacheVariant)
	if !options.Cacheable {
		return s.drainUpstreamBody(normalizedURL, cacheKey, contentType, bodyReader)
	}

	finalPath := s.blobPath(cacheKey)
	tmpPath := finalPath + ".tmp"

	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return Meta{}, err
	}

	out, err := os.Create(tmpPath)
	if err != nil {
		return Meta{}, err
	}

	limitedBody := io.LimitReader(bodyReader, s.cfg.MaxBodyBytes+1)
	written, copyErr := io.Copy(out, limitedBody)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		if copyErr != nil {
			return Meta{}, copyErr
		}
		return Meta{}, closeErr
	}
	if written > s.cfg.MaxBodyBytes {
		_ = os.Remove(tmpPath)
		return Meta{}, fmt.Errorf("image too large: %d bytes", written)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return Meta{}, err
	}

	createdAt := time.Now().UTC()
	meta := Meta{
		SourceURL:   normalizedURL,
		ContentType: contentType,
		Length:      written,
		CreatedAt:   createdAt,
		ETag:        buildETag(cacheKey, written, createdAt),
	}
	if err := writeJSONAtomic(s.metaPath(cacheKey), meta); err != nil {
		_ = os.Remove(finalPath)
		return Meta{}, err
	}
	log.Printf("upstream fetch ok url=%s mode=%s cache=public", normalizedURL, options.Mode)
	return meta, nil
}

func (s *Server) drainUpstreamBody(normalizedURL, cacheKey, contentType string, bodyReader io.Reader) (Meta, error) {
	limitedBody := io.LimitReader(bodyReader, s.cfg.MaxBodyBytes+1)
	written, err := io.Copy(io.Discard, limitedBody)
	if err != nil {
		return Meta{}, err
	}
	if written > s.cfg.MaxBodyBytes {
		return Meta{}, fmt.Errorf("image too large: %d bytes", written)
	}
	createdAt := time.Now().UTC()
	meta := Meta{
		SourceURL:   normalizedURL,
		ContentType: contentType,
		Length:      written,
		CreatedAt:   createdAt,
		ETag:        buildETag(cacheKey, written, createdAt),
	}
	log.Printf("upstream fetch ok url=%s mode=forwarded cache=private", normalizedURL)
	return meta, nil
}

func (s *Server) doUpstreamRequest(ctx context.Context, normalizedURL string, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, normalizedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "piccache/1.0")
	req.Header.Set("Accept", "image/*,video/*,*/*;q=0.8")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return s.httpClient.Do(req)
}

func (s *Server) doUpstreamRequestWithRetries(ctx context.Context, normalizedURL string, headerAttempts []map[string]string, mode fetchMode) (*http.Response, map[string]string, error) {
	if len(headerAttempts) == 0 {
		headerAttempts = []map[string]string{{}}
	}
	var lastResp *http.Response
	for idx, headers := range headerAttempts {
		resp, err := s.doUpstreamRequest(ctx, normalizedURL, headers)
		if err != nil {
			return nil, nil, err
		}
		if resp.StatusCode == http.StatusForbidden && idx < len(headerAttempts)-1 {
			log.Printf("upstream 403 url=%s mode=%s retry=%d", normalizedURL, mode, idx+1)
			_ = resp.Body.Close()
			continue
		}
		lastResp = resp
		return lastResp, headers, nil
	}
	return lastResp, headerAttempts[len(headerAttempts)-1], nil
}

func (s *Server) readMeta(cacheKey string) (Meta, bool) {
	metaPath := s.metaPath(cacheKey)
	blobPath := s.blobPath(cacheKey)

	payload, err := os.ReadFile(metaPath)
	if err != nil {
		return Meta{}, false
	}
	if _, err := os.Stat(blobPath); err != nil {
		return Meta{}, false
	}

	var meta Meta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return Meta{}, false
	}
	return meta, true
}

func (s *Server) isExpired(meta Meta) bool {
	if s.cfg.CacheTTL <= 0 {
		return false
	}
	return time.Since(meta.CreatedAt) > s.cfg.CacheTTL
}

func (s *Server) cacheKey(normalizedURL, variant string) string {
	sum := sha256.Sum256([]byte(normalizedURL + "\n" + variant))
	return hex.EncodeToString(sum[:])
}

func (s *Server) shardDir(cacheKey string) string {
	return filepath.Join(s.cfg.CacheDir, cacheKey[:2], cacheKey[2:4])
}

func (s *Server) blobPath(cacheKey string) string {
	return filepath.Join(s.shardDir(cacheKey), cacheKey+".bin")
}

func (s *Server) metaPath(cacheKey string) string {
	return filepath.Join(s.shardDir(cacheKey), cacheKey+".json")
}

func (s *Server) defaultFetchOptions() upstreamFetchOptions {
	return upstreamFetchOptions{
		Headers:      map[string]string{},
		CacheVariant: "anonymous",
		Cacheable:    true,
		Mode:         fetchModeAnonymous,
	}
}

func (s *Server) resolveFetchOptions(normalizedURL string, forwarded upstreamFetchOptions) upstreamFetchOptions {
	options := s.defaultFetchOptions()
	if rule, ok := s.matchUpstreamHeaderRule(normalizedURL); ok {
		options.Headers = cloneHeaders(rule.headerMap())
		options.CacheVariant = rule.cacheVariant()
		options.Mode = fetchModeRule
	}

	baseHeaders := cloneHeaders(options.Headers)
	if len(forwarded.Headers) > 0 {
		baseHeaders = mergeHeaders(baseHeaders, forwarded.Headers)
		if options.Mode != fetchModeRule {
			options.Mode = fetchModeForwarded
		}
		options.Cacheable = forwarded.Cacheable
	}

	if !hasAuthorityHeaders(baseHeaders) {
		inferred, retries := inferUpstreamHeaderOptions(normalizedURL, baseHeaders)
		if len(inferred) > 0 {
			baseHeaders = inferred
			options.RetryHeaders = retries
			if options.Mode == fetchModeAnonymous || options.Mode == fetchModeForwarded {
				options.Mode = fetchModeInferred
			}
		}
	}

	options.Headers = baseHeaders
	if len(forwarded.Headers) == 0 {
		if options.Mode == fetchModeInferred {
			options.CacheVariant = buildInferredCacheVariant(normalizedURL, options.Headers)
		}
		return options
	}
	if !forwarded.Cacheable {
		options.CacheVariant = "private"
		return options
	}
	if options.Mode == fetchModeInferred {
		options.CacheVariant = buildInferredCacheVariant(normalizedURL, options.Headers)
		return options
	}
	options.CacheVariant = buildCacheVariant(options.Headers)
	return options
}

func (s *Server) matchUpstreamHeaderRule(normalizedURL string) (UpstreamHeaderRule, bool) {
	parsed, err := url.Parse(normalizedURL)
	if err != nil {
		return UpstreamHeaderRule{}, false
	}
	host := strings.ToLower(parsed.Hostname())
	for _, rule := range s.cfg.UpstreamHeaderRules {
		for _, pattern := range rule.Hosts {
			if hostMatchesPattern(host, pattern) {
				return rule, true
			}
		}
	}
	return UpstreamHeaderRule{}, false
}

func (s *Server) buildForwardedOptions(normalizedURL string, raw map[string]string) (upstreamFetchOptions, error) {
	return s.buildForwardedOptionsFromMap(normalizedURL, raw, true)
}

func (s *Server) buildForwardedOptionsFromRequest(normalizedURL string, headers http.Header, allowSensitive bool) (upstreamFetchOptions, error) {
	raw := make(map[string]string)
	for _, key := range allowedForwardHeaders(allowSensitive) {
		if value := strings.TrimSpace(headers.Get(key)); value != "" {
			raw[key] = value
		}
	}
	return s.buildForwardedOptionsFromMap(normalizedURL, raw, allowSensitive)
}

func (s *Server) buildForwardedOptionsFromMap(normalizedURL string, raw map[string]string, allowSensitive bool) (upstreamFetchOptions, error) {
	forwarded := make(map[string]string)
	if len(raw) == 0 {
		return upstreamFetchOptions{Headers: forwarded, Cacheable: true}, nil
	}

	parsed, err := url.Parse(normalizedURL)
	if err != nil {
		return upstreamFetchOptions{}, err
	}
	host := strings.ToLower(parsed.Hostname())

	cacheable := true
	for key, value := range raw {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(key))
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if !isAllowedForwardHeader(canonical) {
			return upstreamFetchOptions{}, fmt.Errorf("upstream header %q is not allowed", canonical)
		}
		if isSensitiveForwardHeader(canonical) {
			if !allowSensitive {
				continue
			}
			cacheable = false
			if !hostAllowed(host, s.cfg.CredentialedHosts) {
				return upstreamFetchOptions{}, fmt.Errorf("credentialed upstream headers are not allowed for host %q", host)
			}
		}
		forwarded[canonical] = trimmed
	}

	return upstreamFetchOptions{
		Headers:      forwarded,
		CacheVariant: buildCacheVariant(forwarded),
		Cacheable:    cacheable,
		Mode:         fetchModeForwarded,
	}, nil
}

func isAllowedForwardHeader(header string) bool {
	switch header {
	case "Referer", "Origin", "Cookie", "Authorization", "User-Agent", "Accept-Language":
		return true
	default:
		return false
	}
}

func isSensitiveForwardHeader(header string) bool {
	return header == "Cookie" || header == "Authorization"
}

func allowedForwardHeaders(allowSensitive bool) []string {
	headers := []string{"Referer", "Origin", "User-Agent", "Accept-Language"}
	if allowSensitive {
		headers = append(headers, "Cookie", "Authorization")
	}
	return headers
}

func hasAuthorityHeaders(headers map[string]string) bool {
	return strings.TrimSpace(headers["Referer"]) != "" || strings.TrimSpace(headers["Origin"]) != ""
}

func hostAllowed(host string, patterns []string) bool {
	if len(patterns) == 0 {
		return false
	}
	for _, pattern := range patterns {
		if hostMatchesPattern(host, pattern) {
			return true
		}
	}
	return false
}

func hostMatchesPattern(host, pattern string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	if host == "" || pattern == "" {
		return false
	}
	if strings.HasPrefix(pattern, "*.") {
		suffix := strings.TrimPrefix(pattern, "*.")
		return host == suffix || strings.HasSuffix(host, "."+suffix)
	}
	return host == pattern
}

func (r UpstreamHeaderRule) headerMap() map[string]string {
	headers := make(map[string]string)
	if v := strings.TrimSpace(r.Referer); v != "" {
		headers["Referer"] = v
	}
	if v := strings.TrimSpace(r.Origin); v != "" {
		headers["Origin"] = v
	}
	if v := strings.TrimSpace(r.UserAgent); v != "" {
		headers["User-Agent"] = v
	}
	if v := strings.TrimSpace(r.AcceptLanguage); v != "" {
		headers["Accept-Language"] = v
	}
	for key, value := range r.Headers {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(key))
		trimmed := strings.TrimSpace(value)
		if canonical == "" || trimmed == "" || isSensitiveForwardHeader(canonical) {
			continue
		}
		headers[canonical] = trimmed
	}
	return headers
}

func (r UpstreamHeaderRule) cacheVariant() string {
	return buildCacheVariant(r.headerMap())
}

func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers))
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}

func mergeHeaders(base, overlay map[string]string) map[string]string {
	merged := cloneHeaders(base)
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

func cloneHeaderList(items []map[string]string) []map[string]string {
	cloned := make([]map[string]string, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, cloneHeaders(item))
	}
	return cloned
}

func buildCacheVariant(headers map[string]string) string {
	if len(headers) == 0 {
		return "anonymous"
	}
	keys := make([]string, 0, len(headers))
	for key := range headers {
		if isSensitiveForwardHeader(key) {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return "anonymous"
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+headers[key])
	}
	return strings.Join(parts, "\n")
}

func buildInferredCacheVariant(normalizedURL string, headers map[string]string) string {
	parsed, err := url.Parse(normalizedURL)
	host := normalizedURL
	if err == nil {
		host = strings.ToLower(parsed.Hostname())
	}
	filtered := make(map[string]string)
	for key, value := range headers {
		if key == "Referer" || key == "Origin" || isSensitiveForwardHeader(key) {
			continue
		}
		filtered[key] = value
	}
	variant := "inferred-host=" + host
	extra := buildCacheVariant(filtered)
	if extra == "anonymous" {
		return variant
	}
	return variant + "\n" + extra
}

func inferUpstreamHeaders(normalizedURL string) map[string]string {
	headers, _ := inferUpstreamHeaderOptions(normalizedURL, map[string]string{})
	return headers
}

func inferUpstreamHeaderOptions(normalizedURL string, baseHeaders map[string]string) (map[string]string, []map[string]string) {
	parsed, err := url.Parse(normalizedURL)
	if err != nil {
		return nil, nil
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "" {
		return nil, nil
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return nil, nil
	}

	rootHost := registrableHost(host)
	candidates := make([]map[string]string, 0, 2)
	candidates = append(candidates, authorityHeaders(scheme, rootHost))
	if host != rootHost {
		candidates = append(candidates, authorityHeaders(scheme, host))
	}

	var attempts []map[string]string
	seen := make(map[string]struct{})
	for _, candidate := range candidates {
		merged := mergeHeaders(baseHeaders, candidate)
		key := buildCacheVariant(merged)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		attempts = append(attempts, merged)
	}
	if len(attempts) == 0 {
		return nil, nil
	}
	if len(attempts) == 1 {
		return attempts[0], nil
	}
	return attempts[0], attempts[1:]
}

func authorityHeaders(scheme, host string) map[string]string {
	origin := scheme + "://" + host
	return map[string]string{
		"Origin":  origin,
		"Referer": origin + "/",
	}
}

func registrableHost(host string) string {
	if host == "" {
		return host
	}
	root, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err == nil && root != "" {
		return strings.ToLower(root)
	}
	return strings.ToLower(host)
}

// safeDialContext 禁止访问私网、回环等地址，避免 SSRF。
func safeDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		host = address
		port = "80"
	}

	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		if isPrivateOrLoopback(ip) {
			return nil, errPrivateAddress
		}
	}

	var dialer net.Dialer
	return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
}

func isPrivateOrLoopback(ip netip.Addr) bool {
	return ip.IsPrivate() ||
		ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified()
}

func validateUpstreamBody(resp *http.Response) (string, io.Reader, error) {
	const sniffBytes = 512

	preview, err := io.ReadAll(io.LimitReader(resp.Body, sniffBytes))
	if err != nil {
		return "", nil, err
	}

	contentType := decideContentType(resp.Header.Get("Content-Type"), preview)
	if !isAllowedMediaType(contentType) {
		if looksLikeChallengePage(preview) {
			return "", nil, fmt.Errorf("%w: %s", errUpstreamChallenge, compactPreview(preview))
		}
		return "", nil, fmt.Errorf("%w: %s", errUnexpectedMediaType, contentType)
	}

	return contentType, io.MultiReader(bytes.NewReader(preview), resp.Body), nil
}

func decideContentType(header string, preview []byte) string {
	mediaType, _, err := mime.ParseMediaType(header)
	if err == nil && mediaType != "" {
		return mediaType
	}
	return http.DetectContentType(preview)
}

func isAllowedMediaType(contentType string) bool {
	return strings.HasPrefix(contentType, "image/") ||
		strings.HasPrefix(contentType, "video/")
}

func looksLikeChallengePage(preview []byte) bool {
	lower := strings.ToLower(string(preview))
	challengeHints := []string{
		"<html",
		"<body",
		"just a moment",
		"cloudflare",
		"captcha",
		"turnstile",
		"attention required",
	}
	for _, hint := range challengeHints {
		if strings.Contains(lower, hint) {
			return true
		}
	}
	return false
}

func compactPreview(preview []byte) string {
	cleaned := strings.TrimSpace(strings.Join(strings.Fields(string(preview)), " "))
	if len(cleaned) > 120 {
		return cleaned[:120]
	}
	return cleaned
}

func buildETag(cacheKey string, length int64, createdAt time.Time) string {
	return fmt.Sprintf(`"%s-%d-%d"`, cacheKey[:16], length, createdAt.Unix())
}

func writeJSONAtomic(path string, value any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(dir, ".tmp-*.json")
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(tmpFile)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpFile.Name())
		return err
	}
	if err := os.Rename(tmpFile.Name(), path); err != nil {
		_ = os.Remove(tmpFile.Name())
		return err
	}
	return nil
}

func (s *Server) runJanitor(ctx context.Context) {
	s.cleanupExpiredFiles()
	s.cleanupOversizeCache()

	if s.cfg.JanitorInterval <= 0 {
		return
	}

	ticker := time.NewTicker(s.cfg.JanitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanupExpiredFiles()
			s.cleanupOversizeCache()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) cleanupExpiredFiles() {
	removed := 0

	_ = filepath.WalkDir(s.cfg.CacheDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}

		payload, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}

		var meta Meta
		if err := json.Unmarshal(payload, &meta); err != nil {
			_ = os.Remove(path)
			_ = os.Remove(strings.TrimSuffix(path, ".json") + ".bin")
			removed++
			return nil
		}
		if !s.isExpired(meta) {
			return nil
		}

		_ = os.Remove(strings.TrimSuffix(path, ".json") + ".bin")
		_ = os.Remove(path)
		removed++
		return nil
	})

	if removed > 0 {
		log.Printf("cleanup removed %d expired cache entries", removed)
	}
}

func (s *Server) scanCacheEntries() ([]cacheEntry, int64, error) {
	var entries []cacheEntry
	var total int64

	err := filepath.WalkDir(s.cfg.CacheDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}

		payload, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}

		var meta Meta
		if err := json.Unmarshal(payload, &meta); err != nil {
			return nil
		}

		blobPath := strings.TrimSuffix(path, ".json") + ".bin"
		stat, statErr := os.Stat(blobPath)
		if statErr != nil {
			return nil
		}

		entries = append(entries, cacheEntry{
			Key:        strings.TrimSuffix(filepath.Base(path), ".json"),
			BlobPath:   blobPath,
			MetaPath:   path,
			Length:     stat.Size(),
			ModifiedAt: stat.ModTime(),
		})
		total += stat.Size()
		return nil
	})

	return entries, total, err
}

func (s *Server) cleanupOversizeCache() {
	if s.cfg.MaxCacheBytes <= 0 {
		return
	}

	entries, total, err := s.scanCacheEntries()
	if err != nil {
		log.Printf("scan cache failed: %v", err)
		return
	}
	if total <= s.cfg.MaxCacheBytes {
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ModifiedAt.Before(entries[j].ModifiedAt)
	})

	var removedBytes int64
	removedFiles := 0

	for _, entry := range entries {
		if total-removedBytes <= s.cfg.MaxCacheBytes {
			break
		}
		_ = os.Remove(entry.BlobPath)
		_ = os.Remove(entry.MetaPath)
		removedBytes += entry.Length
		removedFiles++
	}

	if removedFiles > 0 {
		log.Printf("oversize cleanup removed files=%d bytes=%d", removedFiles, removedBytes)
	}
}

func envOrDefault(key, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}

func parseDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func parseInt64OrDefault(key string, defaultValue int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func parseCSVEnv(key string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseUpstreamHeaderRules(key string) []UpstreamHeaderRule {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}
	var rules []UpstreamHeaderRule
	if err := json.Unmarshal([]byte(value), &rules); err != nil {
		log.Printf("parse %s failed: %v", key, err)
		return nil
	}
	return rules
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s remote=%s cost=%s", r.Method, r.URL.RequestURI(), r.RemoteAddr, time.Since(startedAt))
	})
}
