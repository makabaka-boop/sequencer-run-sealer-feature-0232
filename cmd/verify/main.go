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

const (
	defaultStateFile = "/verify-state/state.json"
)

// stateFile is where restart fixtures are persisted; overridable for local
// runs outside the compose volume.
func stateFile() string {
	return envOr("VERIFY_STATE_FILE", defaultStateFile)
}

type config struct {
	api1 string
	api2 string
}

func main() {
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
	v.checkSealGroup(ctx)

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

// checkSealGroup exercises the indivisible group seal across both instances:
// atomic failure with per-member gap reports, the 400/404 matrix, then the
// atomic success with request-order response and a stable-sealedAt retry.
func (v *verifier) checkSealGroup(ctx context.Context) {
	a := v.createBatch(ctx, v.cfg.api1, 2)
	b := v.createBatch(ctx, v.cfg.api2, 3)
	if a == "" || b == "" {
		return
	}

	submit := func(base, id string, seq int, payload string) {
		code, body, _ := v.request(ctx, http.MethodPost, base+"/api/v1/batches/"+id+"/chunks",
			map[string]any{"seq": seq, "payload": payload})
		if code != 201 {
			v.failf("group fixture chunk %s/%d: status=%d body=%v", id, seq, code, body)
		}
	}
	submit(v.cfg.api1, a, 1, "a1")
	submit(v.cfg.api2, a, 2, "a2")
	submit(v.cfg.api2, b, 1, "b1")
	submit(v.cfg.api1, b, 3, "b3")
	// b is missing seq 2.

	sealGroup := func(base string, ids []string) (int, map[string]any) {
		code, body, _ := v.request(ctx, http.MethodPost, base+"/api/v1/batches/seal-group",
			map[string]any{"batchIds": ids})
		return code, body
	}
	mustOpen := func(id string) {
		code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil)
		if code != 200 || snap["status"] != "OPEN" || snap["sealedAt"] != nil {
			v.failf("member %s changed despite failed group seal: code=%d body=%v", id, code, snap)
		}
	}

	// Incomplete group: 409 INCOMPLETE listing only B with its gaps; the
	// whole group keeps its pre-call state (A is NOT sealed).
	code, body := sealGroup(v.cfg.api1, []string{a, b})
	if code != 409 || body["error"] != "INCOMPLETE" {
		v.failf("incomplete group seal: status=%d body=%v", code, body)
	} else if members, _ := body["batches"].([]any); len(members) != 1 {
		v.failf("incomplete group should list only B: %v", body)
	} else {
		m, _ := members[0].(map[string]any)
		gaps, _ := m["gaps"].([]any)
		if m["batchId"] != b || int(m["expected"].(float64)) != 3 ||
			int(m["received"].(float64)) != 2 || len(gaps) != 1 ||
			int(gaps[0].(float64)) != 2 {
			v.failf("incomplete member report wrong: %v", m)
		}
	}
	mustOpen(a)
	mustOpen(b)

	// 400/404 matrix; every rejection is side-effect free.
	ghost := "ffffffffffffffffffffffffffffffff"
	reject := func(name string, ids []string, wantStatus int, wantErr string) {
		code, body := sealGroup(v.cfg.api2, ids)
		if code != wantStatus || body["error"] != wantErr {
			v.failf("%s: status=%d body=%v want %d %s", name, code, body, wantStatus, wantErr)
		}
	}
	reject("single id", []string{a}, 400, "INVALID_BATCH_GROUP")
	reject("duplicate ids", []string{a, b, a}, 400, "INVALID_BATCH_GROUP")
	reject("bad id format", []string{a, "zz"}, 400, "INVALID_BATCH_GROUP")
	reject("unknown id", []string{a, ghost}, 404, "BATCH_NOT_FOUND")
	mustOpen(a)
	mustOpen(b)

	// Complete B and seal the group through the other instance: 200 with
	// snapshots in request order.
	submit(v.cfg.api2, b, 2, "b2")
	code, body = sealGroup(v.cfg.api2, []string{a, b})
	if code != 200 {
		v.failf("group seal: status=%d body=%v", code, body)
		return
	}
	members, _ := body["batches"].([]any)
	if len(members) != 2 {
		v.failf("group seal members: %v", body)
		return
	}
	m0, _ := members[0].(map[string]any)
	m1, _ := members[1].(map[string]any)
	if m0["batchId"] != a || m1["batchId"] != b ||
		m0["status"] != "SEALED" || m1["status"] != "SEALED" ||
		m0["sealedAt"] == nil || m1["sealedAt"] == nil {
		v.failf("group seal response wrong: %v", body)
		return
	}

	// Retry reversed through instance 1: idempotent, sealedAt stable.
	code, retry := sealGroup(v.cfg.api1, []string{b, a})
	rmembers, _ := retry["batches"].([]any)
	if code != 200 || len(rmembers) != 2 {
		v.failf("group retry: status=%d body=%v", code, retry)
		return
	}
	r0, _ := rmembers[0].(map[string]any)
	r1, _ := rmembers[1].(map[string]any)
	if r0["batchId"] != b || r1["batchId"] != a ||
		r0["sealedAt"] != m1["sealedAt"] || r1["sealedAt"] != m0["sealedAt"] {
		v.failf("group retry changed result: %v", retry)
	}
	v.log("seal-group acceptance passed on both instances")
}

// persistedState is handed across the API restart via a shared volume.
type persistedState struct {
	OpenID       string `json:"openId"`
	SealedID     string `json:"sealedId"`
	SealedAt     string `json:"sealedAt"`
	OpenReceived int    `json:"openReceived"`
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
	v.log("restart verdicts confirmed identical on both instances")
}

func writeState(s persistedState) error {
	if err := os.MkdirAll(filepath.Dir(stateFile()), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(stateFile(), b, 0o644)
}

func readState() (persistedState, error) {
	var s persistedState
	b, err := os.ReadFile(stateFile())
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}
