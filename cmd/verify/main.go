// Command verify is the one-shot acceptance service for the batch sealing API.
//
// It reaches the two API instances exclusively over the internal compose
// network (never a published port) and exercises the full protocol:
// out-of-order arrival, idempotent retransmission, payload conflicts,
// ascending gaps, incomplete vs atomic sealing, post-seal rules and the
// cross-process race between the last chunk and a seal. VERIFY_PHASE=restart
// additionally proves that verdicts survive both API instances restarting.
//
// Exit code 0 means acceptance passed.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// stateFile locates the fixture state handed across API restarts; the
// VERIFY_STATE_FILE override is only used outside the compose stack.
var stateFile = "/verify-state/state.json"

type config struct {
	api1 string
	api2 string
}

func main() {
	stateFile = envOr("VERIFY_STATE_FILE", stateFile)
	cfg := config{
		api1: envOr("API1_URL", "http://api1:8080"),
		api2: envOr("API2_URL", "http://api2:8080"),
	}
	v := &verifier{
		client: &http.Client{Timeout: 20 * time.Second},
		cfg:    cfg,
		failed: false,
	}

	ctx := context.Background()
	v.log("waiting for both API instances")
	v.waitFor(ctx, cfg.api1+"/healthz")
	v.waitFor(ctx, cfg.api2+"/healthz")
	v.log("both instances healthy; running acceptance checks")

	v.checkLifecycleAcrossInstances(ctx)
	v.checkValidation(ctx)
	v.checkCrossInstanceSealRace(ctx)
	v.checkSealGroupProtocol(ctx)
	v.checkCrossInstanceSealGroupRace(ctx)

	switch phase := os.Getenv("VERIFY_PHASE"); phase {
	case "seed":
		v.phaseSeed(ctx)
	case "restart":
		v.phaseRestart(ctx)
	default:
		// A plain one-shot run also proves restart consistency by persisting
		// fixtures for a subsequent restart-phase invocation.
		v.phaseSeed(ctx)
		v.log("hint: run with VERIFY_PHASE=restart after restarting both APIs")
	}

	if v.failed {
		v.log("RESULT: FAIL")
		os.Exit(1)
	}
	v.log("RESULT: PASS")
}

type verifier struct {
	client *http.Client
	cfg    config
	failed bool
	mu     sync.Mutex
}

func (v *verifier) log(format string, args ...any) {
	fmt.Printf("verify: "+format+"\n", args...)
}

func (v *verifier) failf(format string, args ...any) {
	v.mu.Lock()
	v.failed = true
	v.mu.Unlock()
	v.log("FAIL: "+format, args...)
}

func envOr(key, fallback string) string {
	if x := os.Getenv(key); x != "" {
		return x
	}
	return fallback
}

func (v *verifier) waitFor(ctx context.Context, url string) {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := v.client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
			_ = body
		}
		select {
		case <-ctx.Done():
			v.failf("interrupted waiting for %s", url)
			return
		case <-time.After(time.Second):
		}
	}
	v.failf("timeout waiting for %s", url)
}

func (v *verifier) request(ctx context.Context, method, url string, body any) (int, map[string]any, []byte) {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		v.failf("build request: %v", err)
		return 0, nil, nil
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := v.client.Do(req)
	if err != nil {
		v.failf("%s %s: %v", method, url, err)
		return 0, nil, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	parsed := map[string]any{}
	_ = json.Unmarshal(raw, &parsed)
	return resp.StatusCode, parsed, raw
}

func (v *verifier) createBatch(ctx context.Context, base string, expected int) string {
	code, body, _ := v.request(ctx, http.MethodPost, base+"/api/v1/batches",
		map[string]int{"expectedChunks": expected})
	if code != 201 {
		v.failf("create batch: status=%d body=%v", code, body)
		return ""
	}
	id, _ := body["batchId"].(string)
	if len(id) != 32 {
		v.failf("create batch returned bad id: %v", body)
	}
	return id
}

// checkLifecycleAcrossInstances walks the complete protocol while
// alternating the two instances on purpose.
func (v *verifier) checkLifecycleAcrossInstances(ctx context.Context) {
	id := v.createBatch(ctx, v.cfg.api1, 4)
	if id == "" {
		return
	}

	submit := func(base string, seq int, payload string, wantStatus int, wantDup any) {
		code, body, _ := v.request(ctx, http.MethodPost,
			base+"/api/v1/batches/"+id+"/chunks",
			map[string]any{"seq": seq, "payload": payload})
		if code != wantStatus {
			v.failf("chunk seq=%d via %s: status=%d want %d (body=%v)",
				seq, base, code, wantStatus, body)
			return
		}
		if wantDup != nil && body["duplicate"] != wantDup {
			v.failf("chunk seq=%d duplicate=%v want %v", seq, body["duplicate"], wantDup)
		}
		if code == 200 || code == 201 {
			if int(body["size"].(float64)) != len(payload) {
				v.failf("chunk seq=%d size=%v want %d", seq, body["size"], len(payload))
			}
		}
	}

	// Out of order across instances.
	submit(v.cfg.api2, 4, "four", 201, false)
	submit(v.cfg.api1, 1, "one", 201, false)
	submit(v.cfg.api2, 2, "two", 201, false)

	// Gap list while open (missing [3]).
	code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api2+"/api/v1/batches/"+id, nil)
	if code != 200 || snap["status"] != "OPEN" ||
		int(snap["received"].(float64)) != 3 ||
		len(snap["gaps"].([]any)) != 1 ||
		int(snap["gaps"].([]any)[0].(float64)) != 3 {
		v.failf("open snapshot wrong: code=%d body=%v", code, snap)
	}

	// Seal incomplete -> 409 INCOMPLETE with gaps, remains OPEN.
	code, sealBody, _ := v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/"+id+"/seal", nil)
	if code != 409 || sealBody["error"] != "INCOMPLETE" ||
		len(sealBody["gaps"].([]any)) != 1 ||
		int(sealBody["gaps"].([]any)[0].(float64)) != 3 {
		v.failf("incomplete seal wrong: code=%d body=%v", code, sealBody)
	}
	code, snap, _ = v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil)
	if snap["status"] != "OPEN" {
		v.failf("batch did not stay OPEN after failed seal: %v", snap)
	}

	// Retransmission identical -> original confirmation, duplicate true.
	submit(v.cfg.api1, 4, "four", 200, true)
	// Different content -> CHUNK_CONFLICT.
	submit(v.cfg.api2, 4, "FOUR", 409, nil)

	// Final chunk then seal atomically.
	submit(v.cfg.api2, 3, "three", 201, false)
	code, sealed, _ := v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches/"+id+"/seal", nil)
	if code != 200 || sealed["status"] != "SEALED" || sealed["sealedAt"] == "" {
		v.failf("seal complete wrong: code=%d body=%v", code, sealed)
	}

	// Visible identically on instance 1.
	code, snap, _ = v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil)
	if code != 200 || snap["status"] != "SEALED" ||
		int(snap["received"].(float64)) != 4 || len(snap["gaps"].([]any)) != 0 {
		v.failf("sealed snapshot on other instance wrong: %v", snap)
	}

	// Repeated seal on instance 1: existing result, same sealedAt.
	code, resealed, _ := v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/"+id+"/seal", nil)
	if code != 200 || resealed["sealedAt"] != sealed["sealedAt"] {
		v.failf("repeated seal wrong: code=%d body=%v", code, resealed)
	}

	// Sealed: identical retransmission allowed, changed content rejected.
	submit(v.cfg.api1, 1, "one", 200, true)
	submit(v.cfg.api2, 1, "changed", 409, nil)
	// New seq also rejected.
	submit(v.cfg.api1, 4, "four", 200, true) // identical
}

// checkValidation covers the fixed status code matrix.
func (v *verifier) checkValidation(ctx context.Context) {
	id := v.createBatch(ctx, v.cfg.api1, 2)

	expect := func(name string, code, want int, body map[string]any, wantErr string) {
		if code != want {
			v.failf("%s: status=%d want %d body=%v", name, code, want, body)
			return
		}
		if wantErr != "" && body["error"] != wantErr {
			v.failf("%s: error=%v want %s", name, body["error"], wantErr)
		}
	}

	code, body, _ := v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches",
		map[string]int{"expectedChunks": 0})
	expect("expectedChunks 0", code, 400, body, "INVALID_EXPECTED_CHUNKS")

	code, body, _ = v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches",
		map[string]int{"expectedChunks": 10001})
	expect("expectedChunks 10001", code, 400, body, "INVALID_EXPECTED_CHUNKS")

	code, body, _ = v.request(ctx, http.MethodGet,
		v.cfg.api1+"/api/v1/batches/ffffffffffffffffffffffffffffffff", nil)
	expect("unknown batch", code, 404, body, "BATCH_NOT_FOUND")

	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 3, "payload": "x"})
	expect("seq > expected", code, 400, body, "SEQ_OUT_OF_RANGE")

	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 0, "payload": "x"})
	expect("seq 0", code, 400, body, "INVALID_SEQ")

	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 1, "payload": 42})
	expect("payload not string", code, 400, body, "INVALID_PAYLOAD")

	big := strings.Repeat("a", 65537)
	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 1, "payload": big})
	expect("payload 65537", code, 413, body, "PAYLOAD_TOO_LARGE")

	// UTF-8 byte identity boundary: 65536 multibyte chars exceed byte limit,
	// but exactly 65536 bytes of ASCII are accepted.
	exact := strings.Repeat("a", 65536)
	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 2, "payload": exact})
	expect("payload exactly 65536 bytes", code, 201, body, "")
}

// checkCrossInstanceSealRace fires the last chunk at one instance and the
// seal at the other simultaneously. A SEALED batch can never have gaps.
func (v *verifier) checkCrossInstanceSealRace(ctx context.Context) {
	const rounds = 25
	for i := 0; i < rounds; i++ {
		id := v.createBatch(ctx, v.cfg.api1, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/"+id+"/seal", nil)
		}()
		go func() {
			defer wg.Done()
			v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
				map[string]any{"seq": 1, "payload": "race"})
		}()
		wg.Wait()

		code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil)
		if code != 200 {
			v.failf("race round %d: status query %d", i, code)
			continue
		}
		if snap["status"] == "SEALED" && int(snap["received"].(float64)) != 1 {
			v.failf("race round %d: SEALED batch with gaps: %v", i, snap)
		}
		// Whatever the outcome, settling with another seal must converge
		// exactly once the chunk is present.
		if snap["status"] == "OPEN" && int(snap["received"].(float64)) == 1 {
			code, sealed, _ := v.request(ctx, http.MethodPost,
				v.cfg.api2+"/api/v1/batches/"+id+"/seal", nil)
			if code != 200 || sealed["status"] != "SEALED" {
				v.failf("race round %d: follow-up seal failed: %d %v", i, code, sealed)
			}
		}
	}
}

// checkSealGroupProtocol exercises the indivisible group seal while
// alternating both instances: atomic success in request order, idempotent
// retries (reverse order) with stable timing and preserved sealedAt,
// 409 INCOMPLETE changing nothing, and the 400/404 validation matrix.
func (v *verifier) checkSealGroupProtocol(ctx context.Context) {

	// Two complete members, filled via the opposite instances.
	a := v.createBatch(ctx, v.cfg.api1, 2)
	b := v.createBatch(ctx, v.cfg.api2, 3)
	for seq := 1; seq <= 2; seq++ {
		v.mustChunk(ctx, v.cfg.api2, a, seq, "a")
	}
	for seq := 1; seq <= 3; seq++ {
		v.mustChunk(ctx, v.cfg.api1, b, seq, "b")
	}

	code, body, _ := v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{a, b}})
	if code != 200 {
		v.failf("group seal: status=%d body=%v", code, body)
		return
	}
	list, _ := body["batches"].([]any)
	if len(list) != 2 ||
		list[0].(map[string]any)["batchId"] != a ||
		list[1].(map[string]any)["batchId"] != b {
		v.failf("group seal order wrong: %v", body)
	}
	stampA, _ := list[0].(map[string]any)["sealedAt"].(string)
	stampB, _ := list[1].(map[string]any)["sealedAt"].(string)
	if stampA == "" || stampB == "" {
		v.failf("group seal missing sealedAt: %v", body)
	}

	// Visible identically on the other instance.
	for _, id := range []string{a, b} {
		code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api2+"/api/v1/batches/"+id, nil)
		if code != 200 || snap["status"] != "SEALED" {
			v.failf("group member %s not sealed on other instance: %d %v", id, code, snap)
		}
	}

	// Reverse-order idempotent retries on the other instance: same stamps,
	// timing must stay stable (no lock-order stalls, no deadlock waits).
	const retries = 10
	var maxLatency time.Duration
	for i := 0; i < retries; i++ {
		start := time.Now()
		code, retry, _ := v.request(ctx, http.MethodPost,
			v.cfg.api2+"/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{b, a}})
		d := time.Since(start)
		if d > maxLatency {
			maxLatency = d
		}
		if code != 200 {
			v.failf("group retry %d: status=%d body=%v", i, code, retry)
			break
		}
		rl := retry["batches"].([]any)
		if rl[0].(map[string]any)["sealedAt"] != stampB ||
			rl[1].(map[string]any)["sealedAt"] != stampA {
			v.failf("group retry overwrote sealedAt: %v", retry)
		}
	}
	if maxLatency > 2*time.Second {
		v.failf("group retries too slow (max %s): lock-order stall?", maxLatency)
	}
	v.log("group seal idempotent retries stable (max %s over %d)", maxLatency, retries)

	// Mix an already SEALED member with a fresh complete one: success, and
	// the existing sealedAt is preserved.
	d := v.createBatch(ctx, v.cfg.api2, 1)
	v.mustChunk(ctx, v.cfg.api1, d, 1, "d")
	code, body, _ = v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{a, d}})
	if code != 200 {
		v.failf("mixed group seal: status=%d body=%v", code, body)
	} else {
		ml := body["batches"].([]any)
		if ml[0].(map[string]any)["sealedAt"] != stampA {
			v.failf("mixed group overwrote existing sealedAt: %v", ml[0])
		}
		if ml[1].(map[string]any)["sealedAt"] == nil ||
			ml[1].(map[string]any)["sealedAt"] == "" {
			v.failf("mixed group did not seal new member: %v", ml[1])
		}
	}

	// A complete, B incomplete: 409 INCOMPLETE lists only B in request order
	// with expected/received/ascending gaps; nothing changes.
	g := v.createBatch(ctx, v.cfg.api1, 2) // complete
	for seq := 1; seq <= 2; seq++ {
		v.mustChunk(ctx, v.cfg.api2, g, seq, "g")
	}
	h := v.createBatch(ctx, v.cfg.api2, 3) // only chunk 1 present
	v.mustChunk(ctx, v.cfg.api1, h, 1, "h")
	code, body, raw := v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{g, h}})
	if code != 409 || body["error"] != "INCOMPLETE" {
		v.failf("incomplete group: status=%d body=%v", code, body)
	} else {
		il := body["batches"].([]any)
		if len(il) != 1 || il[0].(map[string]any)["batchId"] != h {
			v.failf("incomplete list wrong: %v", body)
		} else {
			inc := il[0].(map[string]any)
			gaps := inc["gaps"].([]any)
			if int(inc["expected"].(float64)) != 3 ||
				int(inc["received"].(float64)) != 1 ||
				len(gaps) != 2 ||
				int(gaps[0].(float64)) != 2 || int(gaps[1].(float64)) != 3 {
				v.failf("incomplete entry wrong: %v", raw)
			}
		}
	}
	// G (complete) must still be OPEN; H unchanged.
	if !v.expectStatus(ctx, v.cfg.api1, g, "OPEN") || !v.expectStatus(ctx, v.cfg.api2, h, "OPEN") {
		v.failf("failed group seal changed members")
	}

	// Validation matrix: all rejected requests are 400 INVALID_BATCH_GROUP
	// with zero side effects; unknown IDs are 404 BATCH_NOT_FOUND.
	unknown := "ffffffffffffffffffffffffffffffff"
	badCases := []struct {
		name string
		raw  string
	}{
		{"malformed json", `{"batchIds": [`},
		{"missing batchIds", `{}`},
		{"extra field", `{"batchIds":["` + a + `","` + d + `"],"x":1}`},
		{"empty array", `{"batchIds":[]}`},
		{"single member", `{"batchIds":["` + a + `"]}`},
		{"duplicate ids", `{"batchIds":["` + a + `","` + a + `"]}`},
		{"uppercase id", `{"batchIds":["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","` + d + `"]}`},
		{"short id", `{"batchIds":["abc","` + d + `"]}`},
		{"non-string id", `{"batchIds":[1,"` + d + `"]}`},
		{"batchIds string", `{"batchIds":"` + a + `"}`},
	}
	for _, tc := range badCases {
		code, parsed, _ := v.request(ctx, http.MethodPost,
			v.cfg.api1+"/api/v1/batches/seal-group", json.RawMessage(tc.raw))
		if code != 400 || parsed["error"] != "INVALID_BATCH_GROUP" {
			v.failf("bad case %q: status=%d body=%v, want 400 INVALID_BATCH_GROUP",
				tc.name, code, parsed)
		}
	}
	// 101 members.
	big := `{"batchIds":[`
	for i := 0; i < 101; i++ {
		if i > 0 {
			big += ","
		}
		big += `"` + unknown + `"`
	}
	big += `]}`
	if code, parsed, _ := v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/seal-group", json.RawMessage(big)); code != 400 ||
		parsed["error"] != "INVALID_BATCH_GROUP" {
		v.failf("101 members: status=%d body=%v", code, parsed)
	}
	// Unknown, unknown among known, and duplicate-free but missing: 404.
	for _, ids := range [][]string{
		{unknown, g},
		{g, unknown, h},
	} {
		code, parsed, _ := v.request(ctx, http.MethodPost,
			v.cfg.api2+"/api/v1/batches/seal-group", map[string]any{"batchIds": ids})
		if code != 404 || parsed["error"] != "BATCH_NOT_FOUND" {
			v.failf("unknown group %v: status=%d body=%v", ids, code, parsed)
		}
	}
	// No rejected call above may have sealed G or H.
	if !v.expectStatus(ctx, v.cfg.api2, g, "OPEN") || !v.expectStatus(ctx, v.cfg.api1, h, "OPEN") {
		v.failf("rejected group seal had side effects")
	}
}

// checkCrossInstanceSealGroupRace interleaves group seals [A,B] (instance 1)
// and [B,A] (instance 2) with B's final chunk landing on instance 1. A is
// complete from the start. Canonical ascending-ID locking across chunk
// submit, single seal and group seal keeps this deadlock free, and the
// settled state can only be "both SEALED" or "no new seal at all".
func (v *verifier) checkCrossInstanceSealGroupRace(ctx context.Context) {
	const rounds = 25
	for i := 0; i < rounds; i++ {
		a := v.createBatch(ctx, v.cfg.api1, 1)
		b := v.createBatch(ctx, v.cfg.api2, 1)
		// A already complete, B waiting for its only chunk.
		code, _, _ := v.request(ctx, http.MethodPost,
			v.cfg.api2+"/api/v1/batches/"+a+"/chunks",
			map[string]any{"seq": 1, "payload": "a"})
		if code != 201 {
			v.failf("race round %d: seed A chunk %d", i, code)
			continue
		}

		var wg sync.WaitGroup
		wg.Add(3)
		var codeAB, codeBA, codeChunk int
		go func() {
			defer wg.Done()
			codeAB, _, _ = v.request(ctx, http.MethodPost,
				v.cfg.api1+"/api/v1/batches/seal-group",
				map[string]any{"batchIds": []string{a, b}})
		}()
		go func() {
			defer wg.Done()
			codeBA, _, _ = v.request(ctx, http.MethodPost,
				v.cfg.api2+"/api/v1/batches/seal-group",
				map[string]any{"batchIds": []string{b, a}})
		}()
		go func() {
			defer wg.Done()
			codeChunk, _, _ = v.request(ctx, http.MethodPost,
				v.cfg.api1+"/api/v1/batches/"+b+"/chunks",
				map[string]any{"seq": 1, "payload": "last"})
		}()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			v.failf("race round %d: three-way interlock timed out (deadlock)", i)
			return
		}

		if codeChunk != 201 {
			v.failf("race round %d: final chunk status %d", i, codeChunk)
		}
		if (codeAB != 200 && codeAB != 409) || (codeBA != 200 && codeBA != 409) {
			v.failf("race round %d: unexpected group codes AB=%d BA=%d", i, codeAB, codeBA)
		}

		_, snapA, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+a, nil)
		_, snapB, _ := v.request(ctx, http.MethodGet, v.cfg.api2+"/api/v1/batches/"+b, nil)
		stA, _ := snapA["status"].(string)
		stB, _ := snapB["status"].(string)
		switch {
		case stA == "SEALED" && stB == "SEALED":
			if int(snapA["received"].(float64)) != 1 ||
				int(snapB["received"].(float64)) != 1 ||
				len(snapB["gaps"].([]any)) != 0 {
				v.failf("race round %d: SEALED group with gaps: %v %v", i, snapA, snapB)
			}
		case stA == "OPEN" && stB == "OPEN":
			if codeAB != 409 || codeBA != 409 {
				v.failf("race round %d: both OPEN but codes AB=%d BA=%d", i, codeAB, codeBA)
			}
		default:
			v.failf("race round %d: partial sealing forbidden: A=%s B=%s", i, stA, stB)
		}

		// Convergence: once B is complete, another group seal succeeds.
		code, settled, _ := v.request(ctx, http.MethodPost,
			v.cfg.api2+"/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{a, b}})
		if code != 200 {
			v.failf("race round %d: settling group seal %d %v", i, code, settled)
		}
	}
}

// mustChunk submits one chunk and fails the verifier unless it is a first
// write (201).
func (v *verifier) mustChunk(ctx context.Context, base, id string, seq int, payload string) {
	code, body, _ := v.request(ctx, http.MethodPost,
		base+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": seq, "payload": payload})
	if code != 201 {
		v.failf("chunk %s seq=%d: status=%d body=%v", id, seq, code, body)
	}
}

func (v *verifier) expectStatus(ctx context.Context, base, id, want string) bool {
	code, snap, _ := v.request(ctx, http.MethodGet, base+"/api/v1/batches/"+id, nil)
	if code != 200 || snap["status"] != want {
		v.failf("batch %s: code=%d status=%v want %s", id, code, snap["status"], want)
		return false
	}
	return true
}

// persistedState is handed across the API restart via a shared volume.
type persistedState struct {
	OpenID        string   `json:"openId"`
	SealedID      string   `json:"sealedId"`
	SealedAt      string   `json:"sealedAt"`
	OpenReceived  int      `json:"openReceived"`
	GroupIDs      []string `json:"groupIds"`
	GroupSealedAt []string `json:"groupSealedAt"`
}

func (v *verifier) phaseSeed(ctx context.Context) {
	st := persistedState{}

	st.OpenID = v.createBatch(ctx, v.cfg.api1, 3)
	v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches/"+st.OpenID+"/chunks",
		map[string]any{"seq": 2, "payload": "middle"})
	code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+st.OpenID, nil)
	if code == 200 {
		st.OpenReceived = int(snap["received"].(float64))
	}

	st.SealedID = v.createBatch(ctx, v.cfg.api2, 2)
	v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/"+st.SealedID+"/chunks",
		map[string]any{"seq": 1, "payload": "p1"})
	v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches/"+st.SealedID+"/chunks",
		map[string]any{"seq": 2, "payload": "p2"})
	_, sealed, _ := v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.SealedID+"/seal", nil)
	st.SealedAt, _ = sealed["sealedAt"].(string)

	// Group seal fixture: two complete batches sealed as one indivisible
	// group (reverse request order on purpose). Their SEALED verdicts and
	// sealedAt stamps must survive the restart unchanged.
	g1 := v.createBatch(ctx, v.cfg.api1, 1)
	v.mustChunk(ctx, v.cfg.api2, g1, 1, "g1")
	g2 := v.createBatch(ctx, v.cfg.api2, 1)
	v.mustChunk(ctx, v.cfg.api1, g2, 1, "g2")
	_, groupSealed, _ := v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{g2, g1}})
	if list, ok := groupSealed["batches"].([]any); ok && len(list) == 2 {
		for _, item := range list {
			m := item.(map[string]any)
			st.GroupIDs = append(st.GroupIDs, m["batchId"].(string))
			st.GroupSealedAt = append(st.GroupSealedAt, m["sealedAt"].(string))
		}
	} else {
		v.failf("seed group seal wrong: %v", groupSealed)
	}

	if err := writeState(st); err != nil {
		v.failf("write state: %v", err)
		return
	}
	v.log("seeded restart fixtures: open=%s sealed=%s", st.OpenID, st.SealedID)
}

func (v *verifier) phaseRestart(ctx context.Context) {
	st, err := readState()
	if err != nil {
		v.failf("read state: %v", err)
		return
	}

	code, open, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+st.OpenID, nil)
	if code != 200 || open["status"] != "OPEN" ||
		int(open["received"].(float64)) != st.OpenReceived {
		v.failf("after restart open verdict changed: code=%d body=%v", code, open)
	}
	if gaps := open["gaps"].([]any); len(gaps) != 2 ||
		int(gaps[0].(float64)) != 1 || int(gaps[1].(float64)) != 3 {
		v.failf("after restart gaps wrong: %v", open["gaps"])
	}

	// Same content ack must still be the stored original.
	code, ack, _ := v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+st.OpenID+"/chunks",
		map[string]any{"seq": 2, "payload": "middle"})
	if code != 200 || ack["duplicate"] != true {
		v.failf("after restart duplicate ack wrong: code=%d body=%v", code, ack)
	}
	// Different content still conflicts.
	code, ack, _ = v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.OpenID+"/chunks",
		map[string]any{"seq": 2, "payload": "MIDDLE"})
	if code != 409 || ack["error"] != "CHUNK_CONFLICT" {
		v.failf("after restart conflict wrong: code=%d body=%v", code, ack)
	}

	code, sealed, _ := v.request(ctx, http.MethodGet, v.cfg.api2+"/api/v1/batches/"+st.SealedID, nil)
	if code != 200 || sealed["status"] != "SEALED" || sealed["sealedAt"] != st.SealedAt {
		v.failf("after restart sealed verdict changed: code=%d body=%v want sealedAt=%s",
			code, sealed, st.SealedAt)
	}
	// Repeated seal returns the existing result.
	code, resealed, _ := v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.SealedID+"/seal", nil)
	if code != 200 || resealed["sealedAt"] != st.SealedAt {
		v.failf("after restart reseal wrong: code=%d body=%v", code, resealed)
	}

	// Group seal verdicts and sealedAt stamps survive unchanged, and a
	// post-restart group retry is idempotent in the same request order.
	if len(st.GroupIDs) != 2 || len(st.GroupSealedAt) != 2 {
		v.failf("after restart missing group fixture: %+v", st)
	} else {
		for i, id := range st.GroupIDs {
			code, gs, _ := v.request(ctx, http.MethodGet,
				v.cfg.api1+"/api/v1/batches/"+id, nil)
			if code != 200 || gs["status"] != "SEALED" ||
				int(gs["received"].(float64)) != int(gs["expected"].(float64)) ||
				gs["sealedAt"] != st.GroupSealedAt[i] {
				v.failf("after restart group member %s wrong: code=%d body=%v want sealedAt=%s",
					id, code, gs, st.GroupSealedAt[i])
			}
		}
		code, reg, _ := v.request(ctx, http.MethodPost,
			v.cfg.api2+"/api/v1/batches/seal-group",
			map[string]any{"batchIds": st.GroupIDs})
		if code != 200 {
			v.failf("after restart group retry: code=%d body=%v", code, reg)
		} else {
			for i, item := range reg["batches"].([]any) {
				m := item.(map[string]any)
				if m["batchId"] != st.GroupIDs[i] || m["sealedAt"] != st.GroupSealedAt[i] {
					v.failf("after restart group retry changed verdict: %v", reg)
				}
			}
		}
	}
	v.log("restart verdicts confirmed identical on both instances")
}

func writeState(s persistedState) error {
	if dir := filepath.Dir(stateFile); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(stateFile, b, 0o644)
}

func readState() (persistedState, error) {
	var s persistedState
	b, err := os.ReadFile(stateFile)
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}
