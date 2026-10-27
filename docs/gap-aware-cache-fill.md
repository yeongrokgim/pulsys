# Gap-aware cache fill: design notes

Status: design proposal (not yet implemented).
Branch: `feat/gap-aware-cache-fill`, based on `fix/hf-hub-v2-resolve-headers`.

## Problem

A full-file (unranged) GET that arrives when the cache holds only part
of the body refetches the **entire file** from upstream, even though a
prefix (or most) of it is already on disk.

Example flow:

1. Client A fetches the first 25% of a file (`Range: bytes=0-…`).
2. Client B fetches 25%–50%.
3. Client C fetches the whole file (unranged GET).

Steps 1–2 are cached as spans of the SAME cache object (one directory,
one sparse `body` file, spans coalescing to `[0, 50%)`).  Step 3 today
is a cache miss (`Covers([0, total))` fails) → plain GET → the whole
file is re-downloaded and the tee overwrites the already-cached bytes.

Ranged requests do not have this problem: a chunk request for `[a,b)`
that misses fetches exactly `[a,b)` and tees it.  The waste exists only
on unranged full-file GETs.

## What clients actually do (huggingface_hub 2.3.0, verified in source)

- Per-file metadata: **HEAD** on the resolve URL (no body, no span).
- Body: **single-stream unranged GET**, read through a 10 MiB buffer
  (`iter_bytes`) — not HTTP Range requests.
- `Range` is sent **only to resume** an interrupted download
  (`_adjust_range_header` with the size of the local `.incomplete`
  file).
- `snapshot_download` parallelises across files (`max_workers=8`),
  never across byte ranges of one file.
- `hf_transfer` is removed in the 2.x line; the chunk-based path is the
  Xet protocol, which the proxy already diverts by stripping `xet-*`
  Link relations.
- Range-heavy HTTP clients that remain: the importer (parallel 16 MiB
  chunks through the loopback proxy), resume-after-interrupt, and
  direct part-readers (gguf streaming, safetensors remote mmap).

Consequence: partial spans arise mainly from **interrupted downloads**
(aborted GETs tee a prefix span `[0,k)`; the retry resumes with
`Range: bytes=n-`, n ≈ k).  When `n <= k` the coverage check already
passes and the resume is served warm.  When `n > k` a middle gap
`[k, n)` persists — and the next unranged GET refetches everything.

## Alternatives considered

### A. Gap-aware fetch on unranged miss (chosen)

On an unranged GET miss with partial coverage, fetch only the missing
byte range(s) upstream and serve the client a 200 spliced from disk
segments and live segment(s).

- Zero wasted upstream bytes; nothing speculative — the client proved
  it wants the whole file.
- Composes with the existing span model: the fetched gap is teed as
  another segment and `MergeSpans` folds everything into one span.

### B. Background full-file fill (Squid `range_offset_limit` / ATS
`background_fill` style)

Serve the requested range immediately and keep downloading the whole
object into the cache.

- Has precedent, but is a poor fit here: part-readers (gguf streaming,
  safetensors mmap) probe files they never fully load; auto-promoting
  every partial read to a full download amplifies upstream egress —
  the opposite of a pull-through cache — and silently consumes quota.
- Could be added later, config-gated (`--range-offset-limit`), for
  operators who want aggressive prewarm.

### C. Status quo

Chunk-based clients already converge to full coverage; only
mixed-client and interrupted-resume flows pay the refetch.

### D. Slice-based caching (nginx `slice` module style)

Fixed-size slices, each with its own cache key.  Rejected: destroys
the single-span zero-alloc fast paths (`MergeSpans`/`Covers` short
circuits, coreserver whole-object sendfile).

## Chosen design: single missing gap, anywhere

v1 handles one missing gap anywhere in the object — not only a
trailing gap — because the dominant real-world source of gaps is the
interrupted-resume middle gap described above.

Given merged spans and a known `total`, compute the missing intervals.
If there is **exactly one** gap `[g0, g1)`:

- upstream request: `Range: bytes=g0-(g1-1)` on the body URL (the
  cached redirect is guaranteed range-agnostic by `cacheableLocation`);
- client response: 200, `Content-Length: total`, body spliced as
    disk `[0, g0)` → net `[g0, g1)` (teed) → disk `[g1, total)`;
- the teed gap closes the span set to one span `[0, total)`.

Any anomaly falls back to today's full refetch:

- zero gaps (covered — cannot happen; `Covers` would have passed),
- two or more gaps,
- unknown `total` (`meta.Total == -1`),
- no prefix coverage at all (`g0 == 0` and no trailing disk segment —
  degenerates to a normal full fetch anyway),
- upstream answers 200 instead of the requested 206 (Range ignored),
- upstream errors.

### Invariants preserved

- **In-flight coordination**: the unranged GET still acquires the
  whole-object ticket `[0, Max]` before the gap analysis, so
  concurrent full-file requesters wait and re-check exactly as today;
  concurrent chunk requesters dedupe against the same ticket.
- **Resolve metadata (hub 2.x)**: the spliced 200 must replay
  `originExtra` (`originResolveHeaders`) — the client validated
  `X-Repo-Commit` on the final body response.
- **Quota / checkpointing**: the teed gap goes through
  `BeginSegment`/`SegmentWriter` unchanged (16 MiB partial-meta
  checkpoints, quota reservation for `g1 - g0` bytes).
- **Keys**: no new keys; the gap segment lands under the existing body
  key (content-addressed hosts keep the query stripped, auth never in
  the key).
- **Warm path**: after the fill, `spans == [[0, total)]` so the
  single-span fast paths (`MergeSpans`/`Covers`/coreserver sendfile)
  behave exactly as after a full cold download.

### Implementation sketch

- `internal/cache`: exported helper `MissingSpans(spans []Span, total
  int64) []Span` (merged, half-open) — pure function, unit-tested.
- `internal/proxy/gapfill.go`: `serveGapFill(...)` invoked from
  `forward` when `meth == GET && rangeHdr == "" && artifact` after the
  in-flight re-check misses; performs the analysis, the ranged
  upstream call, the three-segment splice, and the tee.
- `forward` unchanged otherwise; `tryServeFromCache` unchanged.

### Test plan

- Fake upstream: support open-ended and closed Range requests on the
  body path (extend `parseTestByteRange`).
- Probe/partial then full: seed spans via ranged requests, then an
  unranged GET; assert upstream received exactly one request with
  `Range: g0-(g1-1)` and zero bytes of already-covered ranges; assert
  the client received the full body with resolve metadata present.
- Middle gap (interrupted-resume shape): spans `[0,k)` + `[n,total)`
  via ranged requests; unranged GET fetches only `[k, n)`.
- Multi-gap → full refetch fallback (no Range sent upstream).
- Upstream ignores Range (returns 200) → full tee fallback.
- After a gap fill, a second unranged GET is a pure warm hit (zero
  upstream fetches), and coreserver-class sendfile semantics are
  intact (single span).
- Resolve metadata replayed on the spliced 200 (`X-Repo-Commit`,
  `X-Linked-Etag`, `X-Linked-Size`).
- Concurrent unranged GETs during a gap fill dedupe on the in-flight
  ticket (one upstream gap fetch, both clients served).

## Future work

- Multiple missing gaps: fetch each gap (parallel upstream requests),
  splice N segments.  Needs interleaved disk/net copying and
  bounded-memory reordering; only worth it if multi-gap objects show
  up in practice.
- `bytes=0-` full-range requests (206 response): same splice with
  different client framing.
- Config-gated background fill for operators who want it.
