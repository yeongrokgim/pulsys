// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Pulsys

package proxy

import "testing"

func TestParseContentRangeResponse(t *testing.T) {
	s, e, tot, ok := parseContentRangeResponse("bytes 0-9/100")
	if !ok || s != 0 || e != 10 || tot != 100 {
		t.Fatalf("got %d %d %d %v", s, e, tot, ok)
	}
	s, e, tot, ok = parseContentRangeResponse("bytes 100-199/*")
	if !ok || s != 100 || e != 200 || tot != -1 {
		t.Fatalf("got %d %d %d %v", s, e, tot, ok)
	}
}

// TestMergePreservedHeaders pins the precedence contract of
// mergePreservedHeaders: the origin resolve redirect's preserved
// headers (the authoritative resolve metadata) win over the body
// response's own set, empty authoritative values never shadow a
// present value, and the own set survives untouched when there is
// nothing authoritative to merge.
func TestMergePreservedHeaders(t *testing.T) {
	own := map[string]string{
		"X-Repo-Commit": "body-commit",
		"X-Xet-Hash":    "body-hash",
		"Accept-Ranges": "bytes",
	}
	authoritative := map[string]string{
		"X-Repo-Commit": "origin-commit", // conflict: origin must win
		"X-Linked-Etag": `"origin-etag"`, // only origin carries it
		"X-Linked-Size": "",              // empty: must not shadow anything
	}
	got := mergePreservedHeaders(own, authoritative)
	want := map[string]string{
		"X-Repo-Commit": "origin-commit",
		"X-Linked-Etag": `"origin-etag"`,
		"X-Xet-Hash":    "body-hash",
		"Accept-Ranges": "bytes",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %s: got %q, want %q", k, got[k], v)
		}
	}

	// Nothing authoritative -> own set returned as-is.
	same := mergePreservedHeaders(own, nil)
	if len(same) != len(own) {
		t.Fatalf("nil authoritative: got %v, want %v", same, own)
	}

	// Empty own set -> copy of the authoritative set.
	onlyOrigin := mergePreservedHeaders(nil, authoritative)
	if onlyOrigin["X-Repo-Commit"] != "origin-commit" || onlyOrigin["X-Linked-Etag"] != `"origin-etag"` {
		t.Fatalf("empty own: got %v", onlyOrigin)
	}
	if _, present := onlyOrigin["X-Linked-Size"]; present {
		t.Fatalf("empty own: empty authoritative value must be dropped, got %v", onlyOrigin)
	}
}
