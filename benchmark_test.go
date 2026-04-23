package main

import (
	"bytes"
	"container/list"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// benchmarkDiscardLogger 避免 benchmark 被同步日志 I/O 干扰。
func benchmarkDiscardLogger(b *testing.B) {
	b.Helper()
	originalWriter := log.Writer()
	b.Cleanup(func() {
		log.SetOutput(originalWriter)
	})
	log.SetOutput(io.Discard)
}

func BenchmarkHandleGetFreshHitMetaOnly(b *testing.B) {
	benchmarkDiscardLogger(b)
	server := newBenchmarkServer(b)
	targetURL := "https://example.com/fresh-meta.png"
	body := makeBenchmarkBlob(900 << 10)
	cacheKey := seedBenchmarkCache(b, server, targetURL, body)
	server.cfg.BlobCacheEntries = 0
	server.cfg.BlobCacheMaxBytes = 0
	server.cfg.BlobFileMaxBytes = 0
	server.deleteBlobCache(cacheKey)

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		server.handleGet(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkHandleGetFreshHitMetaOnlyParallel(b *testing.B) {
	benchmarkDiscardLogger(b)
	server := newBenchmarkServer(b)
	targetURL := "https://example.com/fresh-meta-parallel.png"
	body := makeBenchmarkBlob(900 << 10)
	cacheKey := seedBenchmarkCache(b, server, targetURL, body)
	server.cfg.BlobCacheEntries = 0
	server.cfg.BlobCacheMaxBytes = 0
	server.cfg.BlobFileMaxBytes = 0
	server.deleteBlobCache(cacheKey)

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rec := httptest.NewRecorder()
			server.handleGet(rec, req)
			if rec.Code != http.StatusOK {
				b.Fatalf("GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
			}
		}
	})
}

func BenchmarkHandleGetFreshHitBlobMemory(b *testing.B) {
	benchmarkDiscardLogger(b)
	server := newBenchmarkServer(b)
	targetURL := "https://example.com/fresh-blob-memory.png"
	body := makeBenchmarkBlob(64 << 10)
	cacheKey := seedBenchmarkCache(b, server, targetURL, body)
	server.writeBlobToMemory(cacheKey, body)

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		server.handleGet(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkHandleGetFreshHitBlobMemoryParallel(b *testing.B) {
	benchmarkDiscardLogger(b)
	server := newBenchmarkServer(b)
	targetURL := "https://example.com/fresh-blob-memory-parallel.png"
	body := makeBenchmarkBlob(64 << 10)
	cacheKey := seedBenchmarkCache(b, server, targetURL, body)
	server.writeBlobToMemory(cacheKey, body)

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rec := httptest.NewRecorder()
			server.handleGet(rec, req)
			if rec.Code != http.StatusOK {
				b.Fatalf("GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
			}
		}
	})
}

func BenchmarkHandleGetFreshHitBlobDisk(b *testing.B) {
	benchmarkDiscardLogger(b)
	server := newBenchmarkServer(b)
	targetURL := "https://example.com/fresh-blob-disk.png"
	body := makeBenchmarkBlob(900 << 10)
	cacheKey := seedBenchmarkCache(b, server, targetURL, body)
	server.deleteBlobCache(cacheKey)

	req := httptest.NewRequest(http.MethodGet, "/piccache?url="+targetURL, nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		server.handleGet(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkEnsureCachedMiss(b *testing.B) {
	benchmarkDiscardLogger(b)
	server := newBenchmarkServer(b)
	body := makeBenchmarkBlob(256 << 10)
	server.httpClient = &http.Client{
		Timeout: 5 * time.Second,
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header: http.Header{
					"Content-Type": []string{"image/png"},
				},
				Body:          io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)),
			}, nil
		}),
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetURL := "https://example.com/miss-" + strconv.Itoa(i) + ".png"
		result, err := server.ensureCached(context.Background(), targetURL, upstreamFetchOptions{})
		if err != nil {
			b.Fatalf("ensureCached returned error: %v", err)
		}
		if result.Status != "MISS" {
			b.Fatalf("ensureCached status = %q, want MISS", result.Status)
		}
	}
}

func BenchmarkWarmMetaCache1024(b *testing.B) {
	benchmarkDiscardLogger(b)
	server := newBenchmarkServer(b)
	entryCount := 1024
	seedBenchmarkEntries(b, server, entryCount, false)
	server.cfg.MetaCacheEntries = entryCount
	server.cfg.WarmMetaEntries = entryCount

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clearBenchmarkMetaCache(server)
		server.warmMetaCache(context.Background(), entryCount)
		if got := server.metaCacheLen(); got != entryCount {
			b.Fatalf("meta cache len = %d, want %d", got, entryCount)
		}
	}
}

func BenchmarkScanCacheEntries4096(b *testing.B) {
	benchmarkDiscardLogger(b)
	server := newBenchmarkServer(b)
	entryCount := 4096
	totalBytes := seedBenchmarkEntries(b, server, entryCount, false)

	b.ReportAllocs()
	b.SetBytes(totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, total, err := server.scanCacheEntries()
		if err != nil {
			b.Fatalf("scanCacheEntries returned error: %v", err)
		}
		if len(entries) != entryCount {
			b.Fatalf("scanCacheEntries len = %d, want %d", len(entries), entryCount)
		}
		if total != totalBytes {
			b.Fatalf("scanCacheEntries total = %d, want %d", total, totalBytes)
		}
	}
}

func BenchmarkCleanupExpiredFiles1024(b *testing.B) {
	benchmarkDiscardLogger(b)
	entryCount := 1024

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		server := newBenchmarkServer(b)
		seedBenchmarkEntries(b, server, entryCount, true)
		shards := server.listShardDirs()
		b.StartTimer()

		server.cleanupExpiredFiles(shards)

		b.StopTimer()
		if got := countBenchmarkMetaFiles(b, server); got != 0 {
			b.Fatalf("remaining meta files = %d, want 0", got)
		}
		b.StartTimer()
	}
}

func newBenchmarkServer(b *testing.B) *Server {
	b.Helper()
	server, err := NewServer(Config{
		CacheDir:            b.TempDir(),
		FetchTimeout:        5 * time.Second,
		MaxBodyBytes:        1 << 20,
		CacheTTL:            time.Hour,
		MaxCacheBytes:       1 << 30,
		UpstreamConcurrency: 4,
		PerHostConcurrency:  2,
		MetaCacheEntries:    32,
		BlobCacheEntries:    32,
		BlobCacheMaxBytes:   8 << 20,
		BlobFileMaxBytes:    1 << 20,
		StaleGracePeriod:    5 * time.Minute,
		WarmMetaEntries:     16,
		JanitorShardBatch:   1,
		AllowedSchemes: map[string]struct{}{
			"http":  {},
			"https": {},
		},
	})
	if err != nil {
		b.Fatalf("NewServer returned error: %v", err)
	}
	server.httpClient = &http.Client{Timeout: 5 * time.Second}
	return server
}

func seedBenchmarkCache(b *testing.B, server *Server, targetURL string, body []byte) string {
	b.Helper()
	options := server.resolveFetchOptions(targetURL, upstreamFetchOptions{})
	cacheKey := server.cacheKey(targetURL, options.CacheVariant)
	now := time.Now().UTC()
	meta := Meta{
		SourceURL:   targetURL,
		ContentType: "image/png",
		Length:      int64(len(body)),
		CreatedAt:   now,
		ETag:        buildETag(cacheKey, int64(len(body)), now),
	}
	if err := os.MkdirAll(filepath.Dir(server.blobPath(cacheKey)), 0o755); err != nil {
		b.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(server.blobPath(cacheKey), body, 0o644); err != nil {
		b.Fatalf("WriteFile blob returned error: %v", err)
	}
	if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
		b.Fatalf("writeMetaAtomic returned error: %v", err)
	}
	server.writeMetaToMemory(cacheKey, meta)
	return cacheKey
}

func seedBenchmarkEntries(b *testing.B, server *Server, count int, expired bool) int64 {
	b.Helper()
	var totalBytes int64
	body := makeBenchmarkBlob(4 << 10)
	now := time.Now().UTC()
	for i := 0; i < count; i++ {
		targetURL := "https://example.com/bench/" + strconv.Itoa(i) + ".png"
		options := server.resolveFetchOptions(targetURL, upstreamFetchOptions{})
		cacheKey := server.cacheKey(targetURL, options.CacheVariant)
		createdAt := now
		if expired {
			createdAt = now.Add(-(server.cfg.CacheTTL + time.Minute))
		}
		meta := Meta{
			SourceURL:   targetURL,
			ContentType: "image/png",
			Length:      int64(len(body)),
			CreatedAt:   createdAt,
			ETag:        buildETag(cacheKey, int64(len(body)), createdAt),
		}
		if err := os.MkdirAll(filepath.Dir(server.blobPath(cacheKey)), 0o755); err != nil {
			b.Fatalf("MkdirAll returned error: %v", err)
		}
		if err := os.WriteFile(server.blobPath(cacheKey), body, 0o644); err != nil {
			b.Fatalf("WriteFile blob returned error: %v", err)
		}
		if err := writeMetaAtomic(server.metaPath(cacheKey), meta); err != nil {
			b.Fatalf("writeMetaAtomic returned error: %v", err)
		}
		totalBytes += int64(len(body))
	}
	return totalBytes
}

func clearBenchmarkMetaCache(server *Server) {
	server.metaCacheMu.Lock()
	defer server.metaCacheMu.Unlock()
	server.metaCache = make(map[string]*list.Element)
	server.metaCacheList = list.New()
}

func countBenchmarkMetaFiles(b *testing.B, server *Server) int {
	b.Helper()
	count := 0
	err := filepath.WalkDir(server.cfg.CacheDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, currentMetaSuffix) {
			count++
		}
		return nil
	})
	if err != nil {
		b.Fatalf("WalkDir returned error: %v", err)
	}
	return count
}

func makeBenchmarkBlob(size int) []byte {
	if size <= len(minimalPNG()) {
		return append([]byte(nil), minimalPNG()...)
	}
	blob := make([]byte, size)
	copy(blob, minimalPNG())
	for i := len(minimalPNG()); i < len(blob); i++ {
		blob[i] = byte(i)
	}
	return blob
}
