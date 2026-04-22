package main

import (
	"bytes"
	"container/list"
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

	"golang.org/x/net/publicsuffix"
	"golang.org/x/sync/singleflight"
)

// Config 定义服务配置。
// 这里大部分字段都直接来自环境变量，用来控制监听地址、缓存目录、
// 上游抓取并发、内存元数据缓存大小、过期宽限时间等运行时行为。
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
	PerHostConcurrency  int
	MetaCacheEntries    int
	StaleGracePeriod    time.Duration
	WarmMetaOnStart     bool
	WarmMetaEntries     int
	JanitorShardBatch   int
	AllowedSchemes      map[string]struct{}
	UpstreamHeaderRules []UpstreamHeaderRule
	CredentialedHosts   []string
}

// UpstreamHeaderRule 描述“命中某些域名时，自动补哪些上游请求头”。
// 这类规则适合固定 Referer、Origin、User-Agent 等无敏感信息的场景。
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
// 它既会落到磁盘上的 .json 文件，也会进入进程内存中的元数据缓存。
// 其中：
// - ETag 是本服务自己生成的缓存版本号，给客户端做条件请求用。
// - UpstreamETag / UpstreamLastModified 是上游服务返回的值，给回源重验证用。
type Meta struct {
	SourceURL            string    `json:"source_url"`
	ContentType          string    `json:"content_type"`
	Length               int64     `json:"length"`
	CreatedAt            time.Time `json:"created_at"`
	ETag                 string    `json:"etag"`
	UpstreamETag         string    `json:"upstream_etag,omitempty"`
	UpstreamLastModified string    `json:"upstream_last_modified,omitempty"`
}

// Server 是图片缓存服务。
// 这个结构体里放了请求处理过程中需要反复复用的状态：
// - httpClient：访问上游资源。
// - group：对同一个缓存 key 做 singleflight，避免并发重复抓取。
// - refreshGroup：对 stale 后台刷新做去重。
// - fetchSem / hostFetchSem：限制全局和单 host 的抓取并发。
// - metaCache / metaCacheList：进程内元数据 LRU。
// - copyBufferPool：复用下载时的缓冲区，减少分配和 GC。
type Server struct {
	cfg              Config
	httpClient       *http.Client
	group            singleflight.Group
	refreshGroup     singleflight.Group
	fetchSem         chan struct{}
	metaCacheMu      sync.RWMutex
	metaCache        map[string]*list.Element
	metaCacheList    *list.List
	hostFetchMu      sync.Mutex
	hostFetchSem     map[string]chan struct{}
	janitorMu        sync.Mutex
	janitorCursor    int
	copyBufferPool   sync.Pool
	metaDiskReadHook func(string)
}

// metaCacheEntry 是 LRU 链表里的节点内容。
// map 负责 O(1) 定位，list 负责记录冷热顺序。
type metaCacheEntry struct {
	Key  string
	Meta Meta
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
	// anonymous 表示完全匿名抓取，不额外补特殊请求头。
	fetchModeAnonymous fetchMode = "anonymous"
	// rule 表示命中了静态规则配置。
	fetchModeRule fetchMode = "rule"
	// forwarded 表示调用方显式透传了上游请求头。
	fetchModeForwarded fetchMode = "forwarded"
	// inferred 表示服务根据目标 URL 自动推导了 Referer / Origin。
	fetchModeInferred fetchMode = "inferred"
)

// upstreamFetchOptions 是真正发请求前整理出来的抓取参数。
// 它把“规则头”“调用方透传头”“自动推导头”统一折叠成一个结构。
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
	// 这里定义的是一些会在不同调用链路里复用的错误。
	// 用固定 error 值的好处是可以通过 errors.Is 做语义判断。
	errPrivateAddress      = errors.New("target resolves to private or loopback address")
	errUnexpectedMediaType = errors.New("upstream response is not an image or video")
	errUpstreamChallenge   = errors.New("upstream returned a likely challenge page")
)

// main 负责组装配置、初始化服务，并启动 HTTP 监听与后台任务。
func main() {
	cfg := defaultConfig()

	server, err := NewServer(cfg)
	if err != nil {
		log.Fatalf("init server failed: %v", err)
	}

	// 预热和 janitor 都放在后台 goroutine 中做，
	// 这样服务可以先开始监听，再慢慢恢复热数据和清理任务。
	if cfg.WarmMetaOnStart {
		go server.warmMetaCache(context.Background(), cfg.WarmMetaEntries)
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

// NewServer 创建服务实例，并把运行时依赖都初始化好。
// 这里会顺手把一些配置值规范化，避免 0 或负数把服务搞到不可用状态。
func NewServer(cfg Config) (*Server, error) {
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir cache dir: %w", err)
	}

	upstreamConcurrency := normalizeUpstreamConcurrency(cfg.UpstreamConcurrency)
	cfg.UpstreamConcurrency = upstreamConcurrency
	cfg.PerHostConcurrency = normalizePositive(cfg.PerHostConcurrency, minInt(8, upstreamConcurrency))
	cfg.MetaCacheEntries = normalizePositive(cfg.MetaCacheEntries, 4096)
	cfg.JanitorShardBatch = normalizePositive(cfg.JanitorShardBatch, 32)
	// 这里显式放大连接池，避免高并发 miss 时卡在连接建立与复用上。
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           safeDialContext,
		MaxIdleConns:          maxInt(128, upstreamConcurrency*4),
		MaxIdleConnsPerHost:   maxInt(32, upstreamConcurrency),
		MaxConnsPerHost:       upstreamConcurrency,
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
		fetchSem:      make(chan struct{}, upstreamConcurrency),
		metaCache:     make(map[string]*list.Element),
		metaCacheList: list.New(),
		hostFetchSem:  make(map[string]chan struct{}),
		// 下载上游文件时复用固定大小的缓冲区，减少频繁分配。
		copyBufferPool: sync.Pool{
			New: func() any {
				buf := make([]byte, 32<<10)
				return &buf
			},
		},
	}, nil
}

// handleHealth 是最简单的存活检查接口。
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handlePiccache 根据 HTTP 方法把请求分发到对应处理函数。
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

// handleGet 读取缓存并返回 body。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	s.handleRead(w, r, false)
}

// handleHead 只返回响应头，不返回 body。
func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	s.handleRead(w, r, true)
}

// handleRead 是 GET / HEAD 的公共实现。
// 主流程是：
// 1. 解析并标准化 url。
// 2. 构造上游抓取参数。
// 3. 对同一个 cacheKey 做 singleflight，避免并发重复抓取。
// 4. 根据缓存结果回写响应头。
// 5. GET 时把缓存文件流式写回客户端。
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

	// 同一个 key 在同一时刻只允许一个 goroutine 真正落到 ensureCached，
	// 其余并发请求直接等待共享结果。
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

	// 不再使用 http.ServeFile，避免它再次做文件探测和额外的 stat。
	if err := s.serveCachedBlob(w, r, cacheKey, result.Meta); err != nil {
		http.Error(w, "cache file missing", http.StatusNotFound)
	}
}

// handlePost 提供主动预热接口。
// 调用方把 url 和 access_token 发过来，服务会提前把缓存准备好。
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

// normalizeURL 负责把输入 URL 规范化：
// - 要求必须带 host。
// - 只允许配置过的 scheme。
// - 去掉 fragment，因为 fragment 不会参与实际 HTTP 请求。
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

// ensureCached 是缓存读取的核心函数。
// 它要在“磁盘缓存是否命中”“是否已过期”“是否允许返回 stale”
// “是否需要真实回源”之间做选择。
func (s *Server) ensureCached(ctx context.Context, normalizedURL string, forwarded upstreamFetchOptions) (cacheResult, error) {
	effective := s.resolveFetchOptions(normalizedURL, forwarded)
	if !effective.Cacheable {
		// 私有抓取（带 Cookie / Authorization）不会进共享磁盘缓存，
		// 但流程上仍然走统一的下载与校验逻辑。
		meta, _, err := s.fetchAndStore(ctx, normalizedURL, effective, nil)
		if err != nil {
			return cacheResult{}, err
		}
		return cacheResult{Meta: meta, Status: "MISS"}, nil
	}

	cacheKey := s.cacheKey(normalizedURL, effective.CacheVariant)

	// 第一轮先尽量走缓存，这一步通常是最快路径。
	if meta, ok := s.readMeta(cacheKey); ok {
		if !s.isExpired(meta) {
			return cacheResult{Meta: meta, Status: "HIT"}, nil
		}
		if s.canServeStale(cacheKey, meta) {
			// 已过期但仍在宽限期内时，先把旧内容返回给用户，
			// 再在后台异步刷新，降低尾延迟和上游压力。
			s.refreshStaleCache(normalizedURL, effective, cacheKey, meta)
			return cacheResult{Meta: meta, Status: "STALE"}, nil
		}
	}

	// 真正回源前，先拿全局和单 host 的并发许可。
	release, err := s.acquireFetchSlots(ctx, normalizedURL)
	if err != nil {
		return cacheResult{}, err
	}
	defer release()

	// 拿到并发许可后再做一次双检，防止前一个并发请求已经把缓存写好了。
	var existing *Meta
	if meta, ok := s.readMeta(cacheKey); ok {
		if !s.isExpired(meta) {
			return cacheResult{Meta: meta, Status: "HIT"}, nil
		}
		if s.canServeStale(cacheKey, meta) {
			s.refreshStaleCache(normalizedURL, effective, cacheKey, meta)
			return cacheResult{Meta: meta, Status: "STALE"}, nil
		}
		existing = &meta
	}

	// existing 不为空时，会尽量走条件回源，拿 304 而不是整文件重下。
	meta, revalidated, err := s.fetchAndStore(ctx, normalizedURL, effective, existing)
	if err != nil {
		return cacheResult{}, err
	}
	if revalidated {
		return cacheResult{Meta: meta, Status: "REVALIDATED"}, nil
	}
	return cacheResult{Meta: meta, Status: "MISS"}, nil
}

// fetchAndStore 负责真正访问上游，并把结果更新到本地缓存。
// 返回值里的 bool 表示这次是否通过 304 重验证，而不是下载了新文件。
func (s *Server) fetchAndStore(ctx context.Context, normalizedURL string, options upstreamFetchOptions, existing *Meta) (Meta, bool, error) {
	attemptHeaders := append([]map[string]string{cloneHeaders(options.Headers)}, cloneHeaderList(options.RetryHeaders)...)
	for idx := range attemptHeaders {
		// 如果本地已经有旧元数据，这里会把条件请求头补上。
		attemptHeaders[idx] = mergeHeaders(attemptHeaders[idx], s.conditionalHeaders(existing))
	}
	resp, usedHeaders, err := s.doUpstreamRequestWithRetries(ctx, normalizedURL, attemptHeaders, options.Mode)
	if err != nil {
		return Meta{}, false, err
	}
	defer resp.Body.Close()

	cacheVariant := options.CacheVariant
	if cacheVariant == "" {
		cacheVariant = buildCacheVariant(usedHeaders)
	}
	cacheKey := s.cacheKey(normalizedURL, cacheVariant)

	// 304 表示上游确认“文件没变”，这时只要刷新本地时间戳即可。
	if resp.StatusCode == http.StatusNotModified && existing != nil && options.Cacheable {
		meta, err := s.revalidateMeta(cacheKey, *existing, resp.Header)
		return meta, true, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Meta{}, false, fmt.Errorf("upstream status: %s", resp.Status)
	}

	contentType, bodyReader, err := validateUpstreamBody(resp)
	if err != nil {
		return Meta{}, false, err
	}

	if !options.Cacheable {
		// 私有响应不落盘，只把响应体读完做大小与类型校验。
		meta, err := s.drainUpstreamBody(normalizedURL, cacheKey, contentType, bodyReader)
		return meta, false, err
	}

	finalPath := s.blobPath(cacheKey)
	tmpPath := finalPath + ".tmp"

	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return Meta{}, false, err
	}

	out, err := os.Create(tmpPath)
	if err != nil {
		return Meta{}, false, err
	}

	// 多读 1 个字节，用来判断是否超过配置的最大体积限制。
	limitedBody := io.LimitReader(bodyReader, s.cfg.MaxBodyBytes+1)
	bufPtr := s.copyBufferPool.Get().(*[]byte)
	written, copyErr := io.CopyBuffer(out, limitedBody, *bufPtr)
	s.copyBufferPool.Put(bufPtr)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		if copyErr != nil {
			return Meta{}, false, copyErr
		}
		return Meta{}, false, closeErr
	}
	if written > s.cfg.MaxBodyBytes {
		_ = os.Remove(tmpPath)
		return Meta{}, false, fmt.Errorf("image too large: %d bytes", written)
	}

	// 先写临时文件再 rename，避免写到一半时被别的请求读到残缺文件。
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return Meta{}, false, err
	}

	createdAt := time.Now().UTC()
	meta := Meta{
		SourceURL:            normalizedURL,
		ContentType:          contentType,
		Length:               written,
		CreatedAt:            createdAt,
		ETag:                 buildETag(cacheKey, written, createdAt),
		UpstreamETag:         strings.TrimSpace(resp.Header.Get("ETag")),
		UpstreamLastModified: strings.TrimSpace(resp.Header.Get("Last-Modified")),
	}
	if err := writeJSONAtomic(s.metaPath(cacheKey), meta); err != nil {
		_ = os.Remove(finalPath)
		return Meta{}, false, err
	}
	s.writeMetaToMemory(cacheKey, meta)
	log.Printf("upstream fetch ok url=%s mode=%s cache=public", normalizedURL, options.Mode)
	return meta, false, nil
}

// drainUpstreamBody 用在“不能缓存到共享磁盘”的响应上。
// 它会把 body 全部读完，只保留返回给调用链路需要的元信息。
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

// doUpstreamRequest 组装并发起一次对上游的 GET 请求。
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

// doUpstreamRequestWithRetries 支持有限重试。
// 现在主要用来处理自动推导 Referer / Origin 后第一次 403 的回退场景。
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

// readMeta 优先读内存 LRU，读不到再回退到磁盘上的 .json。
// 这就是高并发 hit 场景能变快的关键之一。
func (s *Server) readMeta(cacheKey string) (Meta, bool) {
	if meta, ok := s.readMetaFromMemory(cacheKey); ok {
		return meta, true
	}

	metaPath := s.metaPath(cacheKey)
	blobPath := s.blobPath(cacheKey)
	if s.metaDiskReadHook != nil {
		s.metaDiskReadHook(cacheKey)
	}

	payload, err := os.ReadFile(metaPath)
	if err != nil {
		s.deleteMetaCache(cacheKey)
		return Meta{}, false
	}
	if _, err := os.Stat(blobPath); err != nil {
		s.deleteMetaCache(cacheKey)
		return Meta{}, false
	}

	var meta Meta
	if err := json.Unmarshal(payload, &meta); err != nil {
		s.deleteMetaCache(cacheKey)
		return Meta{}, false
	}
	s.writeMetaToMemory(cacheKey, meta)
	return meta, true
}

// readMetaFromMemory 从 LRU 中取元数据，并把命中的节点提到队头。
func (s *Server) readMetaFromMemory(cacheKey string) (Meta, bool) {
	s.metaCacheMu.RLock()
	_, ok := s.metaCache[cacheKey]
	s.metaCacheMu.RUnlock()
	if !ok {
		return Meta{}, false
	}

	s.metaCacheMu.Lock()
	if current, ok := s.metaCache[cacheKey]; ok {
		s.metaCacheList.MoveToFront(current)
		meta := current.Value.(*metaCacheEntry).Meta
		s.metaCacheMu.Unlock()
		return meta, true
	}
	s.metaCacheMu.Unlock()
	return Meta{}, false
}

// writeMetaToMemory 把元数据写入 LRU。
// 已存在就更新并提升为最新；超出上限时淘汰最旧项。
func (s *Server) writeMetaToMemory(cacheKey string, meta Meta) {
	s.metaCacheMu.Lock()
	if element, ok := s.metaCache[cacheKey]; ok {
		element.Value.(*metaCacheEntry).Meta = meta
		s.metaCacheList.MoveToFront(element)
		s.metaCacheMu.Unlock()
		return
	}
	entry := &metaCacheEntry{Key: cacheKey, Meta: meta}
	element := s.metaCacheList.PushFront(entry)
	s.metaCache[cacheKey] = element
	for s.cfg.MetaCacheEntries > 0 && s.metaCacheList.Len() > s.cfg.MetaCacheEntries {
		tail := s.metaCacheList.Back()
		if tail == nil {
			break
		}
		s.metaCacheList.Remove(tail)
		delete(s.metaCache, tail.Value.(*metaCacheEntry).Key)
	}
	s.metaCacheMu.Unlock()
}

// deleteMetaCache 从内存 LRU 中移除一个键。
func (s *Server) deleteMetaCache(cacheKey string) {
	s.metaCacheMu.Lock()
	if element, ok := s.metaCache[cacheKey]; ok {
		s.metaCacheList.Remove(element)
		delete(s.metaCache, cacheKey)
	}
	s.metaCacheMu.Unlock()
}

// conditionalHeaders 根据旧元数据生成条件回源头。
func (s *Server) conditionalHeaders(existing *Meta) map[string]string {
	if existing == nil {
		return nil
	}
	headers := map[string]string{}
	if tag := strings.TrimSpace(existing.UpstreamETag); tag != "" {
		headers["If-None-Match"] = tag
	}
	if modified := strings.TrimSpace(existing.UpstreamLastModified); modified != "" {
		headers["If-Modified-Since"] = modified
	}
	return headers
}

// revalidateMeta 处理 304 Not Modified。
// 文件内容没变，但我们要刷新本地元数据里的创建时间和缓存 ETag。
func (s *Server) revalidateMeta(cacheKey string, existing Meta, headers http.Header) (Meta, error) {
	refreshedAt := time.Now().UTC()
	existing.CreatedAt = refreshedAt
	existing.ETag = buildETag(cacheKey, existing.Length, refreshedAt)
	if tag := strings.TrimSpace(headers.Get("ETag")); tag != "" {
		existing.UpstreamETag = tag
	}
	if modified := strings.TrimSpace(headers.Get("Last-Modified")); modified != "" {
		existing.UpstreamLastModified = modified
	}
	if err := writeJSONAtomic(s.metaPath(cacheKey), existing); err != nil {
		return Meta{}, err
	}
	s.writeMetaToMemory(cacheKey, existing)
	return existing, nil
}

// canServeStale 判断“这个已过期缓存是否还能临时拿出来顶一下”。
func (s *Server) canServeStale(cacheKey string, meta Meta) bool {
	if s.cfg.StaleGracePeriod <= 0 {
		return false
	}
	if !s.isExpired(meta) {
		return false
	}
	if time.Since(meta.CreatedAt) > s.cfg.CacheTTL+s.cfg.StaleGracePeriod {
		return false
	}
	_, err := os.Stat(s.blobPath(cacheKey))
	return err == nil
}

// serveCachedBlob 直接把缓存文件流式写回客户端。
// 之所以单独实现，是为了避免 ServeFile 自己再做一轮文件探测。
func (s *Server) serveCachedBlob(w http.ResponseWriter, _ *http.Request, cacheKey string, meta Meta) error {
	file, err := os.Open(s.blobPath(cacheKey))
	if err != nil {
		return err
	}
	defer file.Close()
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Length, 10))
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, file)
	return err
}

// refreshStaleCache 在后台异步刷新 stale 缓存。
// refreshGroup 负责确保同一个 key 在同一时刻只刷新一次。
func (s *Server) refreshStaleCache(normalizedURL string, options upstreamFetchOptions, cacheKey string, existing Meta) {
	go func() {
		_, _, _ = s.refreshGroup.Do(cacheKey, func() (any, error) {
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.FetchTimeout)
			defer cancel()
			release, err := s.acquireFetchSlots(ctx, normalizedURL)
			if err != nil {
				return nil, err
			}
			defer release()
			_, _, err = s.fetchAndStore(ctx, normalizedURL, options, &existing)
			return nil, err
		})
	}()
}

// acquireFetchSlots 同时申请“全局抓取并发”和“单 host 抓取并发”。
// 返回值是一个 release 函数，调用方 defer 释放即可。
func (s *Server) acquireFetchSlots(ctx context.Context, normalizedURL string) (func(), error) {
	select {
	case s.fetchSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	released := false
	releaseGlobal := func() {
		if released {
			return
		}
		released = true
		<-s.fetchSem
	}

	host := normalizedURL
	if parsed, err := url.Parse(normalizedURL); err == nil && parsed.Hostname() != "" {
		host = strings.ToLower(parsed.Hostname())
	}
	if s.cfg.PerHostConcurrency <= 0 {
		return releaseGlobal, nil
	}

	hostSem := s.hostSemaphore(host)
	select {
	case hostSem <- struct{}{}:
		return func() {
			<-hostSem
			releaseGlobal()
		}, nil
	case <-ctx.Done():
		releaseGlobal()
		return nil, ctx.Err()
	}
}

// hostSemaphore 为每个 host 懒创建一个并发信号量。
func (s *Server) hostSemaphore(host string) chan struct{} {
	s.hostFetchMu.Lock()
	defer s.hostFetchMu.Unlock()
	sem, ok := s.hostFetchSem[host]
	if ok {
		return sem
	}
	sem = make(chan struct{}, s.cfg.PerHostConcurrency)
	s.hostFetchSem[host] = sem
	return sem
}

// isExpired 根据 CacheTTL 判断元数据是否过期。
func (s *Server) isExpired(meta Meta) bool {
	if s.cfg.CacheTTL <= 0 {
		return false
	}
	return time.Since(meta.CreatedAt) > s.cfg.CacheTTL
}

// cacheKey 用“标准化 URL + 变体”生成稳定的缓存键。
func (s *Server) cacheKey(normalizedURL, variant string) string {
	sum := sha256.Sum256([]byte(normalizedURL + "\n" + variant))
	return hex.EncodeToString(sum[:])
}

// shardDir 把缓存键拆成两级目录。
// 这样可以避免单目录文件太多，便于 janitor 分 shard 清理。
func (s *Server) shardDir(cacheKey string) string {
	return filepath.Join(s.cfg.CacheDir, cacheKey[:2], cacheKey[2:4])
}

// blobPath 返回二进制缓存文件路径。
func (s *Server) blobPath(cacheKey string) string {
	return filepath.Join(s.shardDir(cacheKey), cacheKey+".bin")
}

// metaPath 返回元数据 JSON 文件路径。
func (s *Server) metaPath(cacheKey string) string {
	return filepath.Join(s.shardDir(cacheKey), cacheKey+".json")
}

// defaultFetchOptions 给匿名抓取准备一套默认值。
func (s *Server) defaultFetchOptions() upstreamFetchOptions {
	return upstreamFetchOptions{
		Headers:      map[string]string{},
		CacheVariant: "anonymous",
		Cacheable:    true,
		Mode:         fetchModeAnonymous,
	}
}

// resolveFetchOptions 把各种来源的请求头合并成最终抓取参数。
// 优先级大致是：默认值 < 规则配置 < 调用方透传 < 自动推导。
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

// matchUpstreamHeaderRule 检查目标 URL 是否命中某条静态域名规则。
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

// buildForwardedOptions 处理 POST 主动预热接口传来的上游请求头。
func (s *Server) buildForwardedOptions(normalizedURL string, raw map[string]string) (upstreamFetchOptions, error) {
	return s.buildForwardedOptionsFromMap(normalizedURL, raw, true)
}

// buildForwardedOptionsFromRequest 处理 GET / HEAD 当前请求里允许继承的头。
func (s *Server) buildForwardedOptionsFromRequest(normalizedURL string, headers http.Header, allowSensitive bool) (upstreamFetchOptions, error) {
	raw := make(map[string]string)
	for _, key := range allowedForwardHeaders(allowSensitive) {
		if value := strings.TrimSpace(headers.Get(key)); value != "" {
			raw[key] = value
		}
	}
	return s.buildForwardedOptionsFromMap(normalizedURL, raw, allowSensitive)
}

// buildForwardedOptionsFromMap 把“允许透传的上游请求头”整理成统一结构。
// 同时在这里决定该请求是否还能进入共享缓存。
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

// isAllowedForwardHeader 定义哪些头允许被转发给上游。
func isAllowedForwardHeader(header string) bool {
	switch header {
	case "Referer", "Origin", "Cookie", "Authorization", "User-Agent", "Accept-Language":
		return true
	default:
		return false
	}
}

// isSensitiveForwardHeader 定义哪些头会让抓取结果变成私有缓存。
func isSensitiveForwardHeader(header string) bool {
	return header == "Cookie" || header == "Authorization"
}

// allowedForwardHeaders 根据场景列出允许继承的请求头。
func allowedForwardHeaders(allowSensitive bool) []string {
	headers := []string{"Referer", "Origin", "User-Agent", "Accept-Language"}
	if allowSensitive {
		headers = append(headers, "Cookie", "Authorization")
	}
	return headers
}

// hasAuthorityHeaders 判断当前头里是否已经带了 Referer / Origin。
func hasAuthorityHeaders(headers map[string]string) bool {
	return strings.TrimSpace(headers["Referer"]) != "" || strings.TrimSpace(headers["Origin"]) != ""
}

// hostAllowed 判断目标 host 是否落在白名单模式里。
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

// hostMatchesPattern 支持精确域名和 *.example.com 这样的通配写法。
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

// headerMap 把规则对象转成真正要发给上游的请求头 map。
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

// cacheVariant 根据规则生成缓存分片变体。
func (r UpstreamHeaderRule) cacheVariant() string {
	return buildCacheVariant(r.headerMap())
}

// cloneHeaders 做一份浅拷贝，避免多个调用方共享同一个 map。
func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers))
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}

// mergeHeaders 用 overlay 覆盖 base，生成新 map。
func mergeHeaders(base, overlay map[string]string) map[string]string {
	merged := cloneHeaders(base)
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

// cloneHeaderList 复制一组 header map。
func cloneHeaderList(items []map[string]string) []map[string]string {
	cloned := make([]map[string]string, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, cloneHeaders(item))
	}
	return cloned
}

// buildCacheVariant 把“会影响上游响应结果的头”编码成缓存变体字符串。
// 敏感头不会进入这里，因为它们不允许进入共享缓存。
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

// buildInferredCacheVariant 给自动推导 Referer / Origin 的场景生成变体。
// 这里把 host 纳入变体，避免不同站点的自动推导结果互相污染。
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

// inferUpstreamHeaders 是 inferUpstreamHeaderOptions 的简化包装。
func inferUpstreamHeaders(normalizedURL string) map[string]string {
	headers, _ := inferUpstreamHeaderOptions(normalizedURL, map[string]string{})
	return headers
}

// inferUpstreamHeaderOptions 根据目标 URL 自动推导上游所需的 Referer / Origin。
// 会优先尝试注册域根，再在需要时回退到目标 host 根路径。
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

// authorityHeaders 根据 scheme 和 host 构造一组 Origin / Referer。
func authorityHeaders(scheme, host string) map[string]string {
	origin := scheme + "://" + host
	return map[string]string{
		"Origin":  origin,
		"Referer": origin + "/",
	}
}

// registrableHost 提取像 example.co.uk 这样的注册域根。
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

// isPrivateOrLoopback 判断某个 IP 是否属于不允许访问的内网或特殊地址。
func isPrivateOrLoopback(ip netip.Addr) bool {
	return ip.IsPrivate() ||
		ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified()
}

// validateUpstreamBody 在真正写磁盘前先看一眼响应体。
// 它会做两件事：
// - 用头和预读内容判断媒体类型是否允许。
// - 识别常见的人机验证页面，避免把挑战页当图片缓存。
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

// decideContentType 优先信任合法的 Content-Type 头，缺失时再做内容探测。
func decideContentType(header string, preview []byte) string {
	mediaType, _, err := mime.ParseMediaType(header)
	if err == nil && mediaType != "" {
		return mediaType
	}
	return http.DetectContentType(preview)
}

// isAllowedMediaType 限定只缓存 image/* 和 video/*。
func isAllowedMediaType(contentType string) bool {
	return strings.HasPrefix(contentType, "image/") ||
		strings.HasPrefix(contentType, "video/")
}

// looksLikeChallengePage 用一些简单特征识别 Cloudflare / captcha 一类页面。
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

// compactPreview 把预览内容压成一行，用于错误日志和错误消息。
func compactPreview(preview []byte) string {
	cleaned := strings.TrimSpace(strings.Join(strings.Fields(string(preview)), " "))
	if len(cleaned) > 120 {
		return cleaned[:120]
	}
	return cleaned
}

// buildETag 为客户端生成缓存响应使用的 ETag。
func buildETag(cacheKey string, length int64, createdAt time.Time) string {
	return fmt.Sprintf(`"%s-%d-%d"`, cacheKey[:16], length, createdAt.Unix())
}

// writeJSONAtomic 原子写入 JSON 文件。
// 先写临时文件，再 rename 成正式文件，避免留下半截 JSON。
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

// runJanitor 周期性执行缓存清理任务。
func (s *Server) runJanitor(ctx context.Context) {
	s.runJanitorSweep()

	if s.cfg.JanitorInterval <= 0 {
		return
	}

	ticker := time.NewTicker(s.cfg.JanitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.runJanitorSweep()
		case <-ctx.Done():
			return
		}
	}
}

// runJanitorSweep 执行一次 janitor 轮次。
// 过期清理按 shard 分批做，容量清理则在一轮扫完后再统一做。
func (s *Server) runJanitorSweep() {
	shards, wrapped := s.nextJanitorShardBatch()
	s.cleanupExpiredFiles(shards)
	if wrapped {
		s.cleanupOversizeCache()
	}
}

// cleanupExpiredFiles 只清理本轮选中的 shard 目录。
func (s *Server) cleanupExpiredFiles(shardDirs []string) {
	removed := 0
	if len(shardDirs) == 0 {
		return
	}

	for _, shardDir := range shardDirs {
		_ = filepath.WalkDir(shardDir, func(path string, d os.DirEntry, err error) error {
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
				s.deleteMetaCache(cacheKeyFromMetaPath(path))
				_ = os.Remove(path)
				_ = os.Remove(strings.TrimSuffix(path, ".json") + ".bin")
				removed++
				return nil
			}
			if !s.isExpired(meta) {
				return nil
			}

			s.deleteMetaCache(cacheKeyFromMetaPath(path))
			_ = os.Remove(strings.TrimSuffix(path, ".json") + ".bin")
			_ = os.Remove(path)
			removed++
			return nil
		})
	}

	if removed > 0 {
		log.Printf("cleanup removed %d expired cache entries", removed)
	}
}

// scanCacheEntries 扫描整个缓存目录，收集容量清理需要的文件信息。
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

// cleanupOversizeCache 在总缓存大小超限时，按最旧文件优先淘汰。
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
		s.deleteMetaCache(entry.Key)
		_ = os.Remove(entry.BlobPath)
		_ = os.Remove(entry.MetaPath)
		removedBytes += entry.Length
		removedFiles++
	}

	if removedFiles > 0 {
		log.Printf("oversize cleanup removed files=%d bytes=%d", removedFiles, removedBytes)
	}
}

// envOrDefault 读取字符串环境变量，不存在时使用默认值。
func envOrDefault(key, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}

// defaultConfig 统一定义所有环境变量的默认值。
func defaultConfig() Config {
	return Config{
		ListenAddr:          envOrDefault("LISTEN_ADDR", "127.0.0.1:9090"),
		CacheDir:            envOrDefault("CACHE_DIR", "./data/cache"),
		AccessToken:         envOrDefault("ACCESS_TOKEN", "change-me"),
		FetchTimeout:        parseDurationOrDefault("FETCH_TIMEOUT", 15*time.Second),
		MaxBodyBytes:        parseInt64OrDefault("MAX_BODY_BYTES", 20<<20), // 20MB
		CacheTTL:            parseDurationOrDefault("CACHE_TTL", 30*24*time.Hour),
		JanitorInterval:     parseDurationOrDefault("JANITOR_INTERVAL", time.Hour),
		MaxCacheBytes:       parseInt64OrDefault("MAX_CACHE_BYTES", 10<<30), // 10GB
		UpstreamConcurrency: int(parseInt64OrDefault("UPSTREAM_CONCURRENCY", 64)),
		PerHostConcurrency:  int(parseInt64OrDefault("UPSTREAM_CONCURRENCY_PER_HOST", 8)),
		MetaCacheEntries:    int(parseInt64OrDefault("META_CACHE_ENTRIES", 4096)),
		StaleGracePeriod:    parseDurationOrDefault("STALE_GRACE_PERIOD", 10*time.Minute),
		WarmMetaOnStart:     parseBoolOrDefault("WARM_META_ON_START", true),
		WarmMetaEntries:     int(parseInt64OrDefault("WARM_META_ENTRIES", 2048)),
		JanitorShardBatch:   int(parseInt64OrDefault("JANITOR_SHARD_BATCH", 32)),
		AllowedSchemes:      map[string]struct{}{"http": {}, "https": {}},
		UpstreamHeaderRules: parseUpstreamHeaderRules("UPSTREAM_HEADER_RULES_JSON"),
		CredentialedHosts:   parseCSVEnv("CREDENTIAL_FORWARD_HOSTS"),
	}
}

// normalizeUpstreamConcurrency 保证全局并发至少为 1。
func normalizeUpstreamConcurrency(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}

// normalizePositive 保证正整数配置项至少回退到给定默认值。
func normalizePositive(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

// warmMetaCache 启动后异步预热一部分磁盘元数据到内存 LRU。
// 之所以异步做，是为了不阻塞服务启动。
func (s *Server) warmMetaCache(ctx context.Context, limit int) {
	remaining := limit
	if remaining == 0 {
		return
	}
	for _, shardDir := range s.listShardDirs() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = filepath.WalkDir(shardDir, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
				return nil
			}
			if remaining > 0 && s.metaCacheList.Len() >= remaining {
				return io.EOF
			}
			cacheKey := cacheKeyFromMetaPath(path)
			if _, ok := s.readMetaFromMemory(cacheKey); ok {
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
			if _, err := os.Stat(strings.TrimSuffix(path, ".json") + ".bin"); err != nil {
				return nil
			}
			s.writeMetaToMemory(cacheKey, meta)
			return nil
		})
		if remaining > 0 && s.metaCacheLen() >= remaining {
			return
		}
	}
}

// metaCacheLen 返回当前内存 LRU 中的条目数。
func (s *Server) metaCacheLen() int {
	s.metaCacheMu.RLock()
	defer s.metaCacheMu.RUnlock()
	return s.metaCacheList.Len()
}

// nextJanitorShardBatch 计算本轮 janitor 要处理的 shard 列表。
// wrapped=true 表示已经绕了一圈，适合在这一轮后执行容量清理。
func (s *Server) nextJanitorShardBatch() ([]string, bool) {
	shards := s.listShardDirs()
	if len(shards) == 0 {
		return nil, false
	}
	batchSize := normalizePositive(s.cfg.JanitorShardBatch, 32)

	s.janitorMu.Lock()
	defer s.janitorMu.Unlock()
	if s.janitorCursor >= len(shards) {
		s.janitorCursor = 0
	}
	start := s.janitorCursor
	end := minInt(start+batchSize, len(shards))
	batch := append([]string(nil), shards[start:end]...)
	s.janitorCursor = end
	wrapped := false
	if s.janitorCursor >= len(shards) {
		s.janitorCursor = 0
		wrapped = true
	}
	return batch, wrapped
}

// listShardDirs 列出当前缓存目录下已有的所有 shard 子目录。
func (s *Server) listShardDirs() []string {
	levelOne, err := os.ReadDir(s.cfg.CacheDir)
	if err != nil {
		return nil
	}
	shards := make([]string, 0)
	for _, first := range levelOne {
		if !first.IsDir() {
			continue
		}
		firstPath := filepath.Join(s.cfg.CacheDir, first.Name())
		levelTwo, err := os.ReadDir(firstPath)
		if err != nil {
			continue
		}
		for _, second := range levelTwo {
			if second.IsDir() {
				shards = append(shards, filepath.Join(firstPath, second.Name()))
			}
		}
	}
	sort.Strings(shards)
	return shards
}

// cacheKeyFromMetaPath 从 xxx.json 路径反推出缓存键。
func cacheKeyFromMetaPath(path string) string {
	return strings.TrimSuffix(filepath.Base(path), ".json")
}

// maxInt 返回两个整数中的较大值。
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// minInt 返回两个整数中的较小值。
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// parseDurationOrDefault 解析 time.Duration 格式的环境变量。
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

// parseInt64OrDefault 解析整数环境变量。
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

// parseBoolOrDefault 解析布尔环境变量。
func parseBoolOrDefault(key string, defaultValue bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

// parseCSVEnv 读取逗号分隔的环境变量列表。
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

// parseUpstreamHeaderRules 读取并解析 JSON 形式的规则配置。
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

// loggingMiddleware 打印每次请求的基本访问日志和耗时。
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s remote=%s cost=%s", r.Method, r.URL.RequestURI(), r.RemoteAddr, time.Since(startedAt))
	})
}
