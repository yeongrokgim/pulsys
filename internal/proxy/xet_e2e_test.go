// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Pulsys

package proxy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pulsys-io/pulsys/internal/cache"
	"github.com/pulsys-io/pulsys/internal/config"
	"github.com/pulsys-io/pulsys/internal/logx"
	"github.com/pulsys-io/pulsys/internal/proxy"
	"github.com/pulsys-io/pulsys/internal/upstream"
)

// xetUpstream simulates the two upstream hops a Hugging Face Xet
// download performs:
//
//  1. GET huggingface.co/<repo>/resolve/main/<file>
//     -> 302 with:
//     Location: https://cas-bridge.xethub.hf.co/.../<sha>?<presign>
//     Link:     <...>; rel="xet-auth", <...>; rel="xet-reconstruction-info"
//     The presign query string is FRESH on every request (mimicking real
//     AWS-style signatures whose Date/Signature change each time).
//
//  2. GET cas-bridge.xethub.hf.co/.../<sha>?<presign>
//     -> 200 with the file body.
//
// The proxy MUST:
//   - strip the xet-* Link relations from the 302 (so the HF python
//     client does not bypass us via the Xet protocol)
//   - rewrite Location to /_p/cas-bridge.xethub.hf.co/...
//   - cache the file body keyed by path only (ignoring the rotating
//     presign) so a second cold redirect with a different signature
//     warm-hits.
type xetUpstream struct {
	body []byte
	hash string
	// commit, when set, makes the resolve 302 carry the resolve
	// metadata real HF emits (X-Repo-Commit, X-Linked-Etag,
	// X-Linked-Size) -- what huggingface_hub reads to identify the
	// artifact.
	commit       string
	resolveCount atomic.Int64 // calls to /resolve/main/...
	casCount     atomic.Int64 // calls to cas-bridge bytes path
	presignNonce atomic.Int64 // monotonic counter to vary the presign each call
}

func (u *xetUpstream) Do(_ context.Context, _, host, path, _ string, reqHdr http.Header, _ []byte) (*upstream.Response, error) {
	switch host {
	case "huggingface.co", "hf.co":
		u.resolveCount.Add(1)
		nonce := u.presignNonce.Add(1)
		loc := fmt.Sprintf(
			"https://cas-bridge.xethub.hf.co/xet-bridge-us/abc/%s?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260515T%06dZ&X-Amz-Signature=%016x",
			u.hash, nonce, nonce,
		)
		h := http.Header{}
		h.Set("Location", loc)
		h.Set("X-Xet-Hash", u.hash)
		if u.commit != "" {
			h.Set("X-Repo-Commit", u.commit)
			h.Set("X-Linked-Etag", `"`+u.hash+`"`)
			h.Set("X-Linked-Size", strconv.Itoa(len(u.body)))
		}
		h.Set("Link",
			`<https://huggingface.co/api/models/x/y/xet-read-token/abc>; rel="xet-auth", `+
				`<https://cas-server.xethub.hf.co/v1/reconstructions/`+u.hash+`>; rel="xet-reconstruction-info"`)
		return &upstream.Response{
			Status: http.StatusFound,
			Header: h,
			Body:   io.NopCloser(bytes.NewReader(nil)),
		}, nil

	case "cas-bridge.xethub.hf.co":
		u.casCount.Add(1)
		h := http.Header{}
		h.Set("Content-Type", "application/octet-stream")
		h.Set("ETag", `"`+u.hash+`"`)
		// Single-range support so the 206 code paths (parallel chunk
		// downloads) are exercisable end to end.
		if start, end, ok := parseTestByteRange(reqHdr.Get("Range"), len(u.body)); ok {
			chunk := u.body[start:end]
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(u.body)))
			h.Set("Content-Length", strconv.Itoa(len(chunk)))
			return &upstream.Response{
				Status:        http.StatusPartialContent,
				Header:        h,
				ContentLength: int64(len(chunk)),
				Body:          io.NopCloser(bytes.NewReader(chunk)),
			}, nil
		}
		h.Set("Content-Length", strconv.Itoa(len(u.body)))
		return &upstream.Response{
			Status:        http.StatusOK,
			Header:        h,
			ContentLength: int64(len(u.body)),
			Body:          io.NopCloser(bytes.NewReader(u.body)),
		}, nil
	}
	return nil, errors.New("xetUpstream: unexpected host " + host)
}

// parseTestByteRange parses a single "bytes=a-b" Range against a body
// of length n.  end is exclusive.
func parseTestByteRange(rng string, n int) (start, end int, ok bool) {
	rest, found := strings.CutPrefix(rng, "bytes=")
	if !found {
		return 0, 0, false
	}
	a, b, found := strings.Cut(rest, "-")
	if !found {
		return 0, 0, false
	}
	start, err := strconv.Atoi(a)
	if err != nil || start < 0 || start >= n {
		return 0, 0, false
	}
	end, err = strconv.Atoi(b)
	if err != nil {
		return 0, 0, false
	}
	if end < start || end >= n {
		end = n
	}
	return start, end + 1, true
}

// newXetProxyServer wires a real proxy.Handler over httptest.NewServer.
// PublicBaseURL is set to the test server's URL so that
// rewrite.LocationToProxy emits absolute /_p/ Locations the test
// http.Client can follow.
func newXetProxyServer(tb testing.TB, fake upstream.Client) (*http.Client, string, func()) {
	tb.Helper()
	dir := tb.TempDir()
	// We need the public base URL to point at the eventual server URL.
	// httptest.NewUnstartedServer lets us pin the listener first, look at
	// its addr, then construct the cfg with that PublicBaseURL.
	srv := httptest.NewUnstartedServer(nil)
	publicURL := "http://" + srv.Listener.Addr().String()

	cfg, err := config.ParseFlags(flag.NewFlagSet("xet", flag.ContinueOnError), []string{
		"-listen", "127.0.0.1:0",
		"-admin-listen", "127.0.0.1:0",
		"-cache-dir", filepath.Join(dir, "cache"),
		"-public-base-url", publicURL,
	})
	if err != nil {
		tb.Fatal(err)
	}
	store, err := cache.NewStore(cfg.CacheDir, "none")
	if err != nil {
		tb.Fatal(err)
	}
	h := proxy.NewHandler(cfg, store, fake, logx.New("error"))
	srv.Config.Handler = h
	srv.Start()

	tr := &http.Transport{
		DisableCompression:  true,
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
	}
	client := &http.Client{
		Transport: tr,
		// Don't auto-follow; we want to inspect the 302 + Location.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return client, publicURL, func() {
		tr.CloseIdleConnections()
		srv.Close()
	}
}

// TestXetRedirectStripsXetLinksAndCachesByContentHash is the central
// guarantee that "We need to handle the xet, that is a hard requirement"
// is satisfied:
//
//  1. The 302 we return for the resolve URL must NOT carry any
//     rel="xet-auth" or rel="xet-reconstruction-info" Link relations,
//     so a python huggingface_hub client falls back to following the
//     regular Location and stays inside the proxy.
//  2. Following that rewritten Location to /_p/cas-bridge.xethub.hf.co/...
//     must stream the full body and tee every byte to disk.
//  3. A SECOND resolve must warm-hit the redirect cache (the 302 itself
//     is cached) AND the cas-bridge body cache: ZERO new upstream calls
//     on either host.  This is the critical optimisation that lets a
//     911-range parallel download against warm proxy avoid one HF API call
//     per range task -- which was the binding bottleneck before
//     30x caching landed.
func TestXetRedirectStripsXetLinksAndCachesByContentHash(t *testing.T) {
	const sha = "63bed80836ee0758c8fd4f8975d59bb0b864263ee2753547c358e8a37cde8758"
	body := bytes.Repeat([]byte("x"), 256*1024)
	fake := &xetUpstream{body: body, hash: sha}
	client, base, stop := newXetProxyServer(t, fake)
	defer stop()

	// (1) First resolve - inspect the 302.
	resolveURL := base + "/openai-community/gpt2/resolve/main/model.safetensors"
	resp1, err := client.Get(resolveURL)
	if err != nil {
		t.Fatalf("resolve #1: %v", err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusFound {
		t.Fatalf("expected 302, got %d", resp1.StatusCode)
	}
	loc1 := resp1.Header.Get("Location")
	if loc1 == "" {
		t.Fatal("missing Location on resolve response")
	}
	if !startsWith(loc1, base+"/_p/cas-bridge.xethub.hf.co/") {
		t.Fatalf("Location not rewritten to /_p/: %s", loc1)
	}
	for _, lv := range resp1.Header.Values("Link") {
		if containsAny(lv, []string{"xet-auth", "xet-reconstruction-info"}) {
			t.Fatalf("expected xet-* Link relations to be stripped, got: %s", lv)
		}
	}

	// (2) Follow the rewritten Location - fully drain and verify body.
	got1 := drainOK(t, client, loc1)
	if !bytes.Equal(got1, body) {
		t.Fatalf("body mismatch on cold cas-bridge fetch")
	}
	if cas := fake.casCount.Load(); cas != 1 {
		t.Fatalf("cold: expected exactly 1 cas-bridge upstream fetch, got %d", cas)
	}

	// (3) Second resolve - MUST warm-hit the cached 30x and serve the
	// cached Location without an upstream HF roundtrip.
	resolveCountBefore := fake.resolveCount.Load()
	resp2, err := client.Get(resolveURL)
	if err != nil {
		t.Fatalf("resolve #2: %v", err)
	}
	_ = resp2.Body.Close()
	loc2 := resp2.Header.Get("Location")
	if loc2 == "" {
		t.Fatal("resolve #2: missing Location on cached 302")
	}
	if loc1 != loc2 {
		t.Fatalf("redirect cache should replay the same Location:\n  loc1=%s\n  loc2=%s", loc1, loc2)
	}
	if delta := fake.resolveCount.Load() - resolveCountBefore; delta != 0 {
		t.Fatalf("HARD: warm resolve should pull ZERO new bytes from huggingface.co; "+
			"resolveCount delta = %d", delta)
	}

	// (4) Following the cached Location must still serve the body, and
	// must NOT issue another cas-bridge upstream call (warm body cache).
	got2 := drainOK(t, client, loc2)
	if !bytes.Equal(got2, body) {
		t.Fatalf("body mismatch on warm cas-bridge fetch")
	}
	if cas := fake.casCount.Load(); cas != 1 {
		t.Fatalf("HARD: warm Xet hit should pull ZERO new bytes from cas-bridge; "+
			"casCount went 1 -> %d", cas)
	}
}

// TestResolveMetadataReplayedOnPBodyResponses pins compatibility with
// huggingface_hub >= 0.34 (the 2.x line, incl. the `hf` CLI): the client
// follows the whole /resolve/ -> 302 -> CDN chain within ONE request and
// validates the resolve metadata (X-Repo-Commit, X-Linked-Etag,
// X-Linked-Size) on the FINAL body response.  Pulsys rewrites the CDN
// Location to /_p/<cdn-host>/... and serves the body itself, but the CDN
// response does not carry that metadata -- without replaying the cached
// redirect's preserved headers on the /_p/ response, `hf download`
// aborts with:
//
//	Error: Local entry not found. Response from .../resolve/<sha>/<file>
//	is missing the 'X-Repo-Commit' header, so it does not seem to be
//	served by a Hugging Face Hub endpoint.
//
// The metadata must be present on the /_p/ body response COLD (fetched
// from the CDN), WARM (replayed from the body cache), on ranged 206
// serves, and through the SHA-pinned /resolve/<sha>/<file> rev alias.
func TestResolveMetadataReplayedOnPBodyResponses(t *testing.T) {
	const sha = "bb5ff7e71536bbce6378f6d4bb523a77f1e9455965702d18bec33f599d5851f7"
	const commit = "7ae557604adf67be50417f59c2c2f167def9a775"
	body := bytes.Repeat([]byte("m"), 128*1024)
	fake := &xetUpstream{body: body, hash: sha, commit: commit}
	client, base, stop := newXetProxyServer(t, fake)
	defer stop()

	const repo = "/Qwen/Qwen2.5-0.5B-Instruct"
	resolveURL := base + repo + "/resolve/main/model.safetensors"

	assertMeta := func(tb testing.TB, hdr http.Header, where string) {
		tb.Helper()
		if got := hdr.Get("X-Repo-Commit"); got != commit {
			tb.Fatalf("%s: X-Repo-Commit=%q, want %q", where, got, commit)
		}
		if got := hdr.Get("X-Linked-Size"); got != strconv.Itoa(len(body)) {
			tb.Fatalf("%s: X-Linked-Size=%q, want %d", where, got, len(body))
		}
		if hdr.Get("X-Linked-Etag") == "" {
			tb.Fatalf("%s: missing X-Linked-Etag", where)
		}
	}

	// (1) Cold resolve -> 302.  The redirect itself must carry the
	// metadata (hub 1.x reads it here).
	resp1, err := client.Get(resolveURL)
	if err != nil {
		t.Fatalf("resolve #1: %v", err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusFound {
		t.Fatalf("resolve #1: expected 302, got %d", resp1.StatusCode)
	}
	assertMeta(t, resp1.Header, "cold resolve 302")
	loc := resp1.Header.Get("Location")
	if loc == "" {
		t.Fatal("resolve #1: missing Location")
	}

	// (2) COLD /_p/ body: hub 2.x reads the metadata off THIS response.
	bodyResp, err := client.Get(loc)
	if err != nil {
		t.Fatalf("cold /_p/ body: %v", err)
	}
	got1, _ := io.ReadAll(bodyResp.Body)
	_ = bodyResp.Body.Close()
	if bodyResp.StatusCode != http.StatusOK {
		t.Fatalf("cold /_p/ body: expected 200, got %d", bodyResp.StatusCode)
	}
	if !bytes.Equal(got1, body) {
		t.Fatal("cold /_p/ body: body mismatch")
	}
	assertMeta(t, bodyResp.Header, "cold /_p/ body")

	// (3) WARM resolve -> cached 302 replay with metadata.
	resp2, err := client.Get(resolveURL)
	if err != nil {
		t.Fatalf("resolve #2: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusFound {
		t.Fatalf("resolve #2: expected 302, got %d", resp2.StatusCode)
	}
	assertMeta(t, resp2.Header, "warm resolve 302")

	// (4) WARM /_p/ body: replayed from the body cache with metadata.
	bodyResp2, err := client.Get(resp2.Header.Get("Location"))
	if err != nil {
		t.Fatalf("warm /_p/ body: %v", err)
	}
	got2, _ := io.ReadAll(bodyResp2.Body)
	_ = bodyResp2.Body.Close()
	if bodyResp2.StatusCode != http.StatusOK {
		t.Fatalf("warm /_p/ body: expected 200, got %d", bodyResp2.StatusCode)
	}
	if !bytes.Equal(got2, body) {
		t.Fatal("warm /_p/ body: body mismatch")
	}
	assertMeta(t, bodyResp2.Header, "warm /_p/ body")

	// (5) Ranged warm /_p/ request -> 206 from cache with metadata
	// (parallel chunk downloads take this path).
	reqRange, err := http.NewRequest(http.MethodGet, loc, nil)
	if err != nil {
		t.Fatal(err)
	}
	reqRange.Header.Set("Range", "bytes=0-1023")
	respRange, err := client.Do(reqRange)
	if err != nil {
		t.Fatalf("ranged /_p/: %v", err)
	}
	chunk, _ := io.ReadAll(respRange.Body)
	_ = respRange.Body.Close()
	if respRange.StatusCode != http.StatusPartialContent {
		t.Fatalf("ranged /_p/: expected 206, got %d", respRange.StatusCode)
	}
	if !bytes.Equal(chunk, body[:1024]) {
		t.Fatal("ranged /_p/: chunk mismatch")
	}
	assertMeta(t, respRange.Header, "warm ranged /_p/ 206")

	// (6) SHA-pinned resolve (hub 2.x requests /resolve/<sha>/<file>):
	// the rev alias replays the cached redirect, and the /_p/ warm
	// serve finds the redirect metadata through the alias hop.
	respSha, err := client.Get(base + repo + "/resolve/" + commit + "/model.safetensors")
	if err != nil {
		t.Fatalf("sha resolve: %v", err)
	}
	_ = respSha.Body.Close()
	if respSha.StatusCode != http.StatusFound {
		t.Fatalf("sha resolve: expected 302, got %d", respSha.StatusCode)
	}
	assertMeta(t, respSha.Header, "sha resolve 302 (rev alias)")
	bodyResp3, err := client.Get(respSha.Header.Get("Location"))
	if err != nil {
		t.Fatalf("sha /_p/ body: %v", err)
	}
	got3, _ := io.ReadAll(bodyResp3.Body)
	_ = bodyResp3.Body.Close()
	if bodyResp3.StatusCode != http.StatusOK {
		t.Fatalf("sha /_p/ body: expected 200, got %d", bodyResp3.StatusCode)
	}
	if !bytes.Equal(got3, body) {
		t.Fatal("sha /_p/ body: body mismatch")
	}
	assertMeta(t, bodyResp3.Header, "sha /_p/ warm body (alias hop)")

	// Everything after the first cold resolve must have been served
	// from cache: one resolve, one cas-bridge fetch, total.
	if cas := fake.casCount.Load(); cas != 1 {
		t.Fatalf("expected exactly 1 cas-bridge upstream fetch, got %d", cas)
	}
	if res := fake.resolveCount.Load(); res != 1 {
		t.Fatalf("expected exactly 1 huggingface.co upstream fetch, got %d", res)
	}
}

// TestHeadResolveMetadataFlowReplaysHeaders pins the hub 2.x HEAD
// metadata flow: hub issues HEAD /resolve/<rev>/<file> and follows the
// redirect chain in one request, reading the resolve metadata off the
// FINAL /_p/ response.  HEAD 30x responses must therefore be cached
// under the GET key (HEAD lookups already alias to GET keys), or the
// /_p/ HEAD follow finds no cached redirect and the response lacks
// X-Repo-Commit -- the cold-cache `hf download` failure:
//
//	Error: Local entry not found. Response from .../resolve/<sha>/<file>
//	is missing the 'X-Repo-Commit' header ...
func TestHeadResolveMetadataFlowReplaysHeaders(t *testing.T) {
	const sha = "63bed80836ee0758c8fd4f8975d59bb0b864263ee2753547c358e8a37cde8758"
	const commit = "607a30d783dfa663caf39e06633721c8d4cfcd7e"
	body := bytes.Repeat([]byte("h"), 64*1024)
	fake := &xetUpstream{body: body, hash: sha, commit: commit}
	client, base, stop := newXetProxyServer(t, fake)
	defer stop()

	resolveURL := base + "/openai-community/gpt2/resolve/main/model.safetensors"

	assertMeta := func(tb testing.TB, hdr http.Header, where string) {
		tb.Helper()
		if got := hdr.Get("X-Repo-Commit"); got != commit {
			tb.Fatalf("%s: X-Repo-Commit=%q, want %q", where, got, commit)
		}
		if got := hdr.Get("X-Linked-Size"); got != strconv.Itoa(len(body)) {
			tb.Fatalf("%s: X-Linked-Size=%q, want %d", where, got, len(body))
		}
		if hdr.Get("X-Linked-Etag") == "" {
			tb.Fatalf("%s: missing X-Linked-Etag", where)
		}
	}

	// (1) HEAD resolve -> 302 carrying the metadata.
	headReq, err := http.NewRequest(http.MethodHead, resolveURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp1, err := client.Do(headReq)
	if err != nil {
		t.Fatalf("HEAD resolve: %v", err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusFound {
		t.Fatalf("HEAD resolve: expected 302, got %d", resp1.StatusCode)
	}
	assertMeta(t, resp1.Header, "HEAD resolve 302")
	loc := resp1.Header.Get("Location")
	if loc == "" {
		t.Fatal("HEAD resolve: missing Location")
	}

	// (2) HEAD follow of the /_p/ Location -> 200 carrying the
	// metadata.  This is the exact request hub 2.x aborts on when the
	// HEAD-observed redirect is not cached.
	resp2, err := client.Head(loc)
	if err != nil {
		t.Fatalf("HEAD /_p/ follow: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /_p/ follow: expected 200, got %d", resp2.StatusCode)
	}
	assertMeta(t, resp2.Header, "HEAD /_p/ follow")

	// (3) A subsequent GET resolve warm-hits the redirect the HEAD
	// cached (filed under the GET key): no new upstream resolve.
	resp3, err := client.Get(resolveURL)
	if err != nil {
		t.Fatalf("GET resolve: %v", err)
	}
	_ = resp3.Body.Close()
	if resp3.StatusCode != http.StatusFound {
		t.Fatalf("GET resolve: expected 302, got %d", resp3.StatusCode)
	}
	assertMeta(t, resp3.Header, "GET resolve 302 (HEAD-cached redirect)")

	// (4) GET /_p/ body fetch -> 200 with metadata.
	resp4, err := client.Get(resp3.Header.Get("Location"))
	if err != nil {
		t.Fatalf("GET /_p/ body: %v", err)
	}
	got, _ := io.ReadAll(resp4.Body)
	_ = resp4.Body.Close()
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("GET /_p/ body: expected 200, got %d", resp4.StatusCode)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("GET /_p/ body: body mismatch")
	}
	assertMeta(t, resp4.Header, "GET /_p/ body")

	// One upstream resolve (the HEAD) and two upstream body fetches:
	// the HEAD /_p/ follow (HEADs are never teed to disk) plus the
	// cold GET /_p/ body fetch.
	if res := fake.resolveCount.Load(); res != 1 {
		t.Fatalf("expected exactly 1 huggingface.co upstream fetch, got %d", res)
	}
	if cas := fake.casCount.Load(); cas != 2 {
		t.Fatalf("expected exactly 2 cas-bridge upstream fetches, got %d", cas)
	}
}

// TestXetCasBridgeCacheIgnoresAuthorizationHeader pins the production-
// critical invariant that mixed clients (huggingface_hub strips
// Authorization across cross-origin redirects; Go's http.Client keeps it
// on same-host redirects) MUST land on the same cas-bridge cache slot.
//
// Before this was fixed, a warm parallel-range download against a cache
// populated by `hf download` caused 920 cas-bridge upstream fetches and a 15 GB
// re-download because the two clients hashed into different buckets:
//
//	Python -> key(GET, cas-bridge, /...sha, "", "")
//	Go     -> key(GET, cas-bridge, /...sha, "", "Bearer hf_xxx")
//
// The cas-bridge body is identified solely by the SHA in its path; the
// presign in the query authenticates the upstream fetch and the
// Authorization header is irrelevant to body identity.  The proxy must
// strip BOTH from the cache key for content-addressed hosts.
func TestXetCasBridgeCacheIgnoresAuthorizationHeader(t *testing.T) {
	const sha = "deadbeefcafef00d1234567890abcdef1234567890abcdef1234567890abcdef"
	body := bytes.Repeat([]byte("a"), 256*1024)
	fake := &xetUpstream{body: body, hash: sha}
	client, base, stop := newXetProxyServer(t, fake)
	defer stop()

	const token = "Bearer hf_synthetic_token_for_test"
	resolveURL := base + "/openai-community/gpt2/resolve/main/model.safetensors"

	doReq := func(t *testing.T, method, url string, auth bool) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			t.Fatalf("%s %s: %v", method, url, err)
		}
		if auth {
			req.Header.Set("Authorization", token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, url, err)
		}
		return resp
	}

	// Phase 1 (Python-style cold):
	//   - resolve carries Authorization (HF_TOKEN).
	//   - On the cross-origin redirect to cas-bridge, huggingface_hub
	//     STRIPS Authorization before following.
	resp1 := doReq(t, http.MethodGet, resolveURL, true)
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusFound {
		t.Fatalf("python resolve: status %d", resp1.StatusCode)
	}
	loc1 := resp1.Header.Get("Location")
	if loc1 == "" {
		t.Fatal("python resolve: missing Location")
	}
	resp1Body := doReq(t, http.MethodGet, loc1, false) // strips auth
	got1, _ := io.ReadAll(resp1Body.Body)
	resp1Body.Body.Close()
	if !bytes.Equal(got1, body) {
		t.Fatal("python cold body mismatch")
	}
	if cas := fake.casCount.Load(); cas != 1 {
		t.Fatalf("python cold: expected 1 cas-bridge fetch, got %d", cas)
	}

	// Phase 2 (Go-style warm):
	//   - resolve carries the SAME Authorization (HF_TOKEN, same bucket
	//     as phase 1 -> redirect cache hit).
	//   - On the redirect, Go's http.Client KEEPS Authorization because
	//     the proxy redirect lands on the same host:port.  Without the
	//     content-addressed auth-strip, this would bucket separately
	//     from phase 1 and trigger a fresh cas-bridge upstream fetch.
	resolveBefore := fake.resolveCount.Load()
	casBefore := fake.casCount.Load()

	resp2 := doReq(t, http.MethodGet, resolveURL, true)
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusFound {
		t.Fatalf("go resolve: status %d", resp2.StatusCode)
	}
	loc2 := resp2.Header.Get("Location")
	if loc2 == "" {
		t.Fatal("go resolve: missing Location")
	}
	resp2Body := doReq(t, http.MethodGet, loc2, true) // keeps auth
	defer resp2Body.Body.Close()
	if resp2Body.StatusCode != http.StatusOK {
		t.Fatalf("go cas-bridge: status %d", resp2Body.StatusCode)
	}
	got2, err := io.ReadAll(resp2Body.Body)
	if err != nil {
		t.Fatalf("go read: %v", err)
	}
	if !bytes.Equal(got2, body) {
		t.Fatal("go warm body mismatch")
	}

	if delta := fake.resolveCount.Load() - resolveBefore; delta != 0 {
		t.Fatalf("go warm resolve must hit redirect cache (same auth bucket); "+
			"resolveCount delta = %d", delta)
	}
	if delta := fake.casCount.Load() - casBefore; delta != 0 {
		t.Fatalf("go warm cas-bridge must hit body cache regardless of "+
			"Authorization header presence; casCount delta = %d "+
			"(this is the 920-fetch / 15 GB regression)", delta)
	}
}

// TestXetOriginPathRecordedOnBodyMeta is the storage-attribution
// invariant for the admin UI: when a /Org/Name/resolve/<rev>/<file>
// request redirects out to a content-addressed Xet body
// (cas-bridge.xethub.hf.co), the body's cache.Meta MUST record the
// originating user-facing path in OriginPath so disk usage can be
// attributed back to the model.  Without this the admin UI shows only
// the kilobytes of resolve-cache JSON for the model and silently drops
// the gigabytes of weight bytes -- the "11 MB Qwen2.5-0.5B" bug that
// motivated this attribution.
func TestXetOriginPathRecordedOnBodyMeta(t *testing.T) {
	const sha = "aeb713fdee2a083353a999d46771858f952744509d8af12868a1e95e9c45c7e3"
	const originReq = "/Qwen/Qwen2.5-0.5B/resolve/main/model.safetensors"
	body := bytes.Repeat([]byte("x"), 64*1024)
	fake := &xetUpstream{body: body, hash: sha}

	dir := t.TempDir()
	srv := httptest.NewUnstartedServer(nil)
	publicURL := "http://" + srv.Listener.Addr().String()
	cfg, err := config.ParseFlags(flag.NewFlagSet("xet-origin", flag.ContinueOnError), []string{
		"-listen", "127.0.0.1:0",
		"-admin-listen", "127.0.0.1:0",
		"-cache-dir", filepath.Join(dir, "cache"),
		"-public-base-url", publicURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := cache.NewStore(cfg.CacheDir, "none")
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = proxy.NewHandler(cfg, store, fake, logx.New("error"))
	srv.Start()
	defer srv.Close()

	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	client := &http.Client{
		Transport: tr,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp1, err := client.Get(publicURL + originReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusFound {
		t.Fatalf("status=%d", resp1.StatusCode)
	}
	loc := resp1.Header.Get("Location")
	if loc == "" {
		t.Fatal("missing Location")
	}

	resp2, err := client.Get(loc)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("body fetch status=%d loc=%s body=%s", resp2.StatusCode, loc, string(body))
	}
	if _, err := io.Copy(io.Discard, resp2.Body); err != nil {
		t.Fatal(err)
	}

	// Walk the cache and confirm the cas-bridge body recorded OriginPath.
	// The handler's cache commit runs AFTER the response body is fully
	// flushed to the client, so there's a brief window where the client
	// has already returned but commit() is still finalizing meta.json on
	// disk.  Poll briefly to avoid flake.
	var entries []cache.Meta
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, err = readCacheMetas(t, cfg.CacheDir)
		if err != nil {
			t.Fatal(err)
		}
		hasCas := false
		for _, m := range entries {
			if m.UpstreamHost == "cas-bridge.xethub.hf.co" && m.StatusCode == http.StatusOK {
				hasCas = true
				break
			}
		}
		if hasCas {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var found bool
	for _, m := range entries {
		if m.UpstreamHost != "cas-bridge.xethub.hf.co" {
			continue
		}
		if m.StatusCode != http.StatusOK {
			continue
		}
		found = true
		if m.OriginPath != originReq {
			t.Fatalf("OriginPath=%q want %q", m.OriginPath, originReq)
		}
		// And the rewritten Location given to the client must NOT have
		// leaked the __pulsys_origin param onward (it should be stripped
		// by the handler before forwarding, but the client sees it in
		// the rewritten URL).  Confirm the cache RawQuery has no origin
		// marker either -- the proxy strips it before any storage step.
		if strings.Contains(m.RawQuery, "__pulsys_origin") {
			t.Fatalf("RawQuery still carries origin marker: %q", m.RawQuery)
		}
	}
	if !found {
		var dump strings.Builder
		for _, m := range entries {
			fmt.Fprintf(&dump, "  host=%s status=%d path=%s origin=%s\n",
				m.UpstreamHost, m.StatusCode, m.Path, m.OriginPath)
		}
		walkPath := filepath.Join(cfg.CacheDir, "v1", "objects")
		dirs, _ := os.ReadDir(walkPath)
		for _, d := range dirs {
			files, _ := os.ReadDir(filepath.Join(walkPath, d.Name()))
			fmt.Fprintf(&dump, "  dir=%s files=", d.Name())
			for _, f := range files {
				fi, _ := f.Info()
				fmt.Fprintf(&dump, "%s(%d) ", f.Name(), fi.Size())
			}
			fmt.Fprintln(&dump)
		}
		t.Fatalf("no cas-bridge body meta found in cache; entries=%d:\n%s", len(entries), dump.String())
	}
}

func readCacheMetas(t *testing.T, cacheDir string) ([]cache.Meta, error) {
	t.Helper()
	root := filepath.Join(cacheDir, "v1", "objects")
	ents, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var out []cache.Meta
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(root, e.Name(), "meta.json"))
		if err != nil {
			continue
		}
		var m cache.Meta
		if err := json.Unmarshal(b, &m); err != nil {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func drainOK(tb testing.TB, c *http.Client, url string) []byte {
	tb.Helper()
	resp, err := c.Get(url)
	if err != nil {
		tb.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		tb.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		tb.Fatalf("read body %s: %v", url, err)
	}
	return b
}

func startsWith(s, prefix string) bool { return len(s) >= len(prefix) && s[:len(prefix)] == prefix }

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}
