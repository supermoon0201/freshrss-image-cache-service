package main

import (
	"net/http"
	"testing"
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
