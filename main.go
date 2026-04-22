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
	"sync"
	"time"
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
	group      FlightGroup
	fetchSem   chan struct{}
}

type cacheResult struct {
	Meta   Meta
	Status string
}

// proactiveRequest 对应 FreshRSS proactive cache 的请求体。
type proactiveRequest struct {
	URL         string `json:"url"`
	AccessToken string `json:"access_token"`
}

// FlightGroup 是最小版 singleflight。
// 生产环境可替换为 golang.org/x/sync/singleflight。
type FlightGroup struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	wg  sync.WaitGroup
	val any
	err error
}

func (g *FlightGroup) Do(key string, fn func() (any, error)) (any, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*flightCall)
	}
	if call, ok := g.m[key]; ok {
		g.mu.Unlock()
		call.wg.Wait()
		return call.val, call.err, true
	}

	call := &flightCall{}
	call.wg.Add(1)
	g.m[key] = call
	g.mu.Unlock()

	call.val, call.err = fn()
	call.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return call.val, call.err, false
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
		MaxBodyBytes:        parseInt64OrDefault("MAX_BODY_BYTES", 20<<20),     // 20MB
		CacheTTL:            parseDurationOrDefault("CACHE_TTL", 30*24*time.Hour),
		JanitorInterval:     parseDurationOrDefault("JANITOR_INTERVAL", time.Hour),
		MaxCacheBytes:       parseInt64OrDefault("MAX_CACHE_BYTES", 10<<30), // 10GB
		UpstreamConcurrency: int(parseInt64OrDefault("UPSTREAM_CONCURRENCY", 16)),
		AllowedSchemes:      map[string]struct{}{"http": {}, "https": {}},
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
	case http.MethodPost:
		s.handlePost(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
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

	value, err, shared := s.group.Do(normalizedURL, func() (any, error) {
		return s.ensureCached(r.Context(), normalizedURL)
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

	http.ServeFile(w, r, s.blobPath(normalizedURL))
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

	if _, err := s.ensureCached(r.Context(), normalizedURL); err != nil {
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

func (s *Server) ensureCached(ctx context.Context, normalizedURL string) (cacheResult, error) {
	// 命中且未过期，直接返回。
	if meta, ok := s.readMeta(normalizedURL); ok && !s.isExpired(meta) {
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
	if meta, ok := s.readMeta(normalizedURL); ok && !s.isExpired(meta) {
		return cacheResult{Meta: meta, Status: "HIT"}, nil
	}

	meta, err := s.fetchAndStore(ctx, normalizedURL)
	if err != nil {
		return cacheResult{}, err
	}
	return cacheResult{Meta: meta, Status: "MISS"}, nil
}

func (s *Server) fetchAndStore(ctx context.Context, normalizedURL string) (Meta, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, normalizedURL, nil)
	if err != nil {
		return Meta{}, err
	}
	req.Header.Set("User-Agent", "piccache/1.0")
	req.Header.Set("Accept", "image/*,video/*,*/*;q=0.8")

	resp, err := s.httpClient.Do(req)
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

	finalPath := s.blobPath(normalizedURL)
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
		ETag:        buildETag(s.cacheKey(normalizedURL), written, createdAt),
	}
	if err := writeJSONAtomic(s.metaPath(normalizedURL), meta); err != nil {
		_ = os.Remove(finalPath)
		return Meta{}, err
	}
	return meta, nil
}

func (s *Server) readMeta(normalizedURL string) (Meta, bool) {
	metaPath := s.metaPath(normalizedURL)
	blobPath := s.blobPath(normalizedURL)

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

func (s *Server) cacheKey(normalizedURL string) string {
	sum := sha256.Sum256([]byte(normalizedURL))
	return hex.EncodeToString(sum[:])
}

func (s *Server) shardDir(normalizedURL string) string {
	key := s.cacheKey(normalizedURL)
	return filepath.Join(s.cfg.CacheDir, key[:2], key[2:4])
}

func (s *Server) blobPath(normalizedURL string) string {
	key := s.cacheKey(normalizedURL)
	return filepath.Join(s.shardDir(normalizedURL), key+".bin")
}

func (s *Server) metaPath(normalizedURL string) string {
	key := s.cacheKey(normalizedURL)
	return filepath.Join(s.shardDir(normalizedURL), key+".json")
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

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s remote=%s cost=%s", r.Method, r.URL.RequestURI(), r.RemoteAddr, time.Since(startedAt))
	})
}