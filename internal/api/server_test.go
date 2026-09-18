package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"batchseal/internal/api"
	"batchseal/internal/store"
	"batchseal/internal/testsupport"
)

// Two in-process HTTP servers backed by independent connection pools on the
// same schema stand in for two API instances arbitrating via PostgreSQL.
func newAPIs(t *testing.T) (string, string, func()) {
	t.Helper()
	ctx := context.Background()
	url := testsupport.RequireURL(t)
	schema := testsupport.IsolatedSchema(ctx, t)

	open := func() (*store.Store, *httptest.Server) {
		st, err := store.New(ctx, testsupport.SchemaURL(url, schema))
		if err != nil {
			t.Fatal(err)
		}
		return st, httptest.NewServer(api.NewServer(st, nil).Handler())
	}
	s1, srv1 := open()
	s2, srv2 := open()

	cleanup := func() {
		srv1.Close()
		srv2.Close()
		s1.Close()
		s2.Close()
		testsupport.DropSchema(ctx, schema)
	}
	return srv1.URL, srv2.URL, cleanup
}

type client struct {
	http *http.Client
}

func newClient() *client {
	return &client{http: &http.Client{Timeout: 15 * time.Second}}
}

func (c *client) do(t *testing.T, method, url, raw string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if raw != "" {
		rdr = strings.NewReader(raw)
	} else if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil || raw != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	parsed := map[string]any{}
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("decode %q: %v", data, err)
		}
	}
	return resp.StatusCode, parsed
}

func (c *client) post(t *testing.T, url string, body any) (int, map[string]any) {
	return c.do(t, http.MethodPost, url, "", body)
}

func (c *client) postRaw(t *testing.T, url, raw string) (int, map[string]any) {
	return c.do(t, http.MethodPost, url, raw, nil)
}

func (c *client) get(t *testing.T, url string) (int, map[string]any) {
	return c.do(t, http.MethodGet, url, "", nil)
}

// postQuiet is the non-fatal variant for use inside race goroutines.
func (c *client) postQuiet(url string, body any) (int, map[string]any) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, url, rdr)
	if err != nil {
		return 0, nil
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	parsed := map[string]any{}
	_ = json.Unmarshal(data, &parsed)
	return resp.StatusCode, parsed
}

func TestHTTPLifecycleAcrossTwoInstances(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	code, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 3})
	if code != 201 {
		t.Fatalf("create: code=%d body=%v", code, body)
	}
	batchID, _ := body["batchId"].(string)

	// Out-of-order chunks alternating between the two instances.
	payloads := map[int]string{1: "one", 2: "two", 3: "three"}
	for _, seq := range []int{3, 1, 2} {
		base := u1
		if seq == 1 {
			base = u2
		}
		code, b := c.post(t, base+"/api/v1/batches/"+batchID+"/chunks",
			map[string]any{"seq": seq, "payload": payloads[seq]})
		if code != 201 || b["duplicate"] != false {
			t.Fatalf("chunk %d first write: code=%d body=%v", seq, code, b)
		}
	}

	// Retransmission on the other instance returns the original confirmation.
	code, b := c.post(t, u1+"/api/v1/batches/"+batchID+"/chunks",
		map[string]any{"seq": 3, "payload": "three"})
	if code != 200 || b["duplicate"] != true {
		t.Fatalf("retransmission: code=%d body=%v", code, b)
	}
	if int(b["size"].(float64)) != len("three") {
		t.Fatalf("size = %v", b["size"])
	}

	// Different content conflicts.
	code, b = c.post(t, u2+"/api/v1/batches/"+batchID+"/chunks",
		map[string]any{"seq": 3, "payload": "THREE"})
	if code != 409 || b["error"] != "CHUNK_CONFLICT" {
		t.Fatalf("conflict: code=%d body=%v", code, b)
	}

	// Seal on instance 1; the verdict is immediately visible on instance 2.
	code, b = c.post(t, u1+"/api/v1/batches/"+batchID+"/seal", nil)
	if code != 200 || b["status"] != "SEALED" {
		t.Fatalf("seal: code=%d body=%v", code, b)
	}
	firstSealTime, _ := b["sealedAt"].(string)

	code, b = c.get(t, u2+"/api/v1/batches/"+batchID)
	if code != 200 || b["status"] != "SEALED" ||
		int(b["expected"].(float64)) != 3 || int(b["received"].(float64)) != 3 {
		t.Fatalf("cross-instance status: code=%d body=%v", code, b)
	}

	// Repeated seal on instance 2 returns the existing result.
	code, b = c.post(t, u2+"/api/v1/batches/"+batchID+"/seal", nil)
	if code != 200 || b["sealedAt"] != firstSealTime {
		t.Fatalf("reseal: code=%d body=%v", code, b)
	}

	// Sealed: identical retransmission accepted, changed content rejected.
	code, _ = c.post(t, u2+"/api/v1/batches/"+batchID+"/chunks",
		map[string]any{"seq": 1, "payload": "one"})
	if code != 200 {
		t.Fatalf("identical after seal: %d", code)
	}
	code, b = c.post(t, u1+"/api/v1/batches/"+batchID+"/chunks",
		map[string]any{"seq": 1, "payload": "changed"})
	if code != 409 || b["error"] != "BATCH_SEALED" {
		t.Fatalf("changed after seal: code=%d body=%v", code, b)
	}
}

func TestHTTPIncompleteSealReportsGaps(t *testing.T) {
	u1, _, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 5})
	batchID := body["batchId"].(string)
	for _, seq := range []int{1, 3, 5} {
		c.post(t, u1+"/api/v1/batches/"+batchID+"/chunks",
			map[string]any{"seq": seq, "payload": "x"})
	}

	code, body := c.post(t, u1+"/api/v1/batches/"+batchID+"/seal", nil)
	if code != 409 || body["error"] != "INCOMPLETE" {
		t.Fatalf("seal incomplete: code=%d body=%v", code, body)
	}
	gaps := body["gaps"].([]any)
	if len(gaps) != 2 || int(gaps[0].(float64)) != 2 || int(gaps[1].(float64)) != 4 {
		t.Fatalf("gaps: %v", gaps)
	}
	if body["status"] != "OPEN" || int(body["received"].(float64)) != 3 {
		t.Fatalf("incomplete body wrong: %v", body)
	}

	code, body = c.get(t, u1+"/api/v1/batches/"+batchID)
	if code != 200 || body["status"] != "OPEN" {
		t.Fatalf("batch must remain OPEN after failed seal: %v", body)
	}
}

func TestHTTPValidationCodes(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 2})
	batchID := body["batchId"].(string)

	cases := []struct {
		name       string
		method     string
		url        string
		body       any
		wantStatus int
		wantError  string
	}{
		{"expected zero", http.MethodPost, u1 + "/api/v1/batches",
			map[string]int{"expectedChunks": 0}, 400, "INVALID_EXPECTED_CHUNKS"},
		{"expected too big", http.MethodPost, u1 + "/api/v1/batches",
			map[string]int{"expectedChunks": 10001}, 400, "INVALID_EXPECTED_CHUNKS"},
		{"malformed batch id", http.MethodGet, u1 + "/api/v1/batches/zzz", nil,
			404, "BATCH_NOT_FOUND"},
		{"missing batch", http.MethodGet, u1 + "/api/v1/batches/ffffffffffffffffffffffffffffffff", nil,
			404, "BATCH_NOT_FOUND"},
		{"chunk on missing batch", http.MethodPost,
			u2 + "/api/v1/batches/ffffffffffffffffffffffffffffffff/chunks",
			map[string]any{"seq": 1, "payload": "x"}, 404, "BATCH_NOT_FOUND"},
		{"seq out of range", http.MethodPost, u1 + "/api/v1/batches/" + batchID + "/chunks",
			map[string]any{"seq": 3, "payload": "x"}, 400, "SEQ_OUT_OF_RANGE"},
		{"payload not string", http.MethodPost, u1 + "/api/v1/batches/" + batchID + "/chunks",
			map[string]any{"seq": 1, "payload": 9}, 400, "INVALID_PAYLOAD"},
		{"seq not number", http.MethodPost, u2 + "/api/v1/batches/" + batchID + "/chunks",
			map[string]any{"seq": "1", "payload": "x"}, 400, "INVALID_SEQ"},
		{"missing field", http.MethodPost, u1 + "/api/v1/batches/" + batchID + "/chunks",
			map[string]any{"seq": 1}, 400, "INVALID_REQUEST"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var code int
			var b map[string]any
			if tc.method == http.MethodPost {
				code, b = c.post(t, tc.url, tc.body)
			} else {
				code, b = c.get(t, tc.url)
			}
			if code != tc.wantStatus || b["error"] != tc.wantError {
				t.Fatalf("got code=%d body=%v, want %d %s", code, b, tc.wantStatus, tc.wantError)
			}
		})
	}

	// 413: payload strictly over 65536 UTF-8 bytes.
	big := strings.Repeat("a", 65537)
	code, b := c.postRaw(t, u1+"/api/v1/batches/"+batchID+"/chunks",
		`{"seq":1,"payload":"`+big+`"}`)
	if code != 413 || b["error"] != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("oversize payload: code=%d body=%v", code, b)
	}

	// Exactly 65536 bytes is accepted.
	exact := strings.Repeat("a", 65536)
	code, _ = c.postRaw(t, u1+"/api/v1/batches/"+batchID+"/chunks",
		`{"seq":2,"payload":"`+exact+`"}`)
	if code != 201 {
		t.Fatalf("exact-limit payload should be accepted, got %d", code)
	}
}

// TestHTTPCrossInstanceSealRace drives the final chunk through one instance
// and the seal through the other concurrently. A SEALED batch must never be
// missing a chunk.
func TestHTTPCrossInstanceSealRace(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	const rounds = 20
	for range rounds {
		_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 1})
		batchID := body["batchId"].(string)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.post(t, u1+"/api/v1/batches/"+batchID+"/seal", nil) }()
		go func() {
			defer wg.Done()
			c.post(t, u2+"/api/v1/batches/"+batchID+"/chunks",
				map[string]any{"seq": 1, "payload": "only"})
		}()
		wg.Wait()

		code, snap := c.get(t, u1+"/api/v1/batches/"+batchID)
		if code != 200 {
			t.Fatalf("status: %d %v", code, snap)
		}
		if snap["status"] == "SEALED" && int(snap["received"].(float64)) != 1 {
			t.Fatalf("SEALED with missing chunk: %v", snap)
		}
	}
}

// TestHTTPRestartBothInstances closes both servers/stores and serves the same
// data with freshly created ones: every verdict must be identical.
func TestHTTPRestartBothInstances(t *testing.T) {
	ctx := context.Background()
	base := testsupport.RequireURL(t)
	schema := testsupport.IsolatedSchema(ctx, t)
	defer testsupport.DropSchema(ctx, schema)
	c := newClient()

	var batchOpen, batchSealed string
	var sealTime string
	{
		s1, err := store.New(ctx, testsupport.SchemaURL(base, schema))
		if err != nil {
			t.Fatal(err)
		}
		s2, err := store.New(ctx, testsupport.SchemaURL(base, schema))
		if err != nil {
			t.Fatal(err)
		}
		srv1 := httptest.NewServer(api.NewServer(s1, nil).Handler())
		srv2 := httptest.NewServer(api.NewServer(s2, nil).Handler())

		_, b := c.post(t, srv1.URL+"/api/v1/batches", map[string]int{"expectedChunks": 3})
		batchOpen = b["batchId"].(string)
		c.post(t, srv2.URL+"/api/v1/batches/"+batchOpen+"/chunks", map[string]any{"seq": 2, "payload": "x"})

		_, b = c.post(t, srv1.URL+"/api/v1/batches", map[string]int{"expectedChunks": 1})
		batchSealed = b["batchId"].(string)
		c.post(t, srv2.URL+"/api/v1/batches/"+batchSealed+"/chunks", map[string]any{"seq": 1, "payload": "z"})
		_, b = c.post(t, srv2.URL+"/api/v1/batches/"+batchSealed+"/seal", nil)
		sealTime, _ = b["sealedAt"].(string)

		// "Restart" both instances.
		srv1.Close()
		srv2.Close()
		s1.Close()
		s2.Close()
	}

	{
		s1, err := store.New(ctx, testsupport.SchemaURL(base, schema))
		if err != nil {
			t.Fatal(err)
		}
		s2, err := store.New(ctx, testsupport.SchemaURL(base, schema))
		if err != nil {
			t.Fatal(err)
		}
		srv1 := httptest.NewServer(api.NewServer(s1, nil).Handler())
		srv2 := httptest.NewServer(api.NewServer(s2, nil).Handler())
		defer func() { srv1.Close(); srv2.Close(); s1.Close(); s2.Close() }()

		code, open := c.get(t, srv1.URL+"/api/v1/batches/"+batchOpen)
		if code != 200 || open["status"] != "OPEN" ||
			int(open["received"].(float64)) != 1 ||
			len(open["gaps"].([]any)) != 2 {
			t.Fatalf("open batch changed over restart: %v", open)
		}
		gaps := open["gaps"].([]any)
		if int(gaps[0].(float64)) != 1 || int(gaps[1].(float64)) != 3 {
			t.Fatalf("gaps not ascending over restart: %v", gaps)
		}

		code, sealed := c.get(t, srv2.URL+"/api/v1/batches/"+batchSealed)
		if code != 200 || sealed["status"] != "SEALED" || sealed["sealedAt"] != sealTime {
			t.Fatalf("sealed verdict changed over restart: %v (want sealedAt %s)", sealed, sealTime)
		}

		// Duplicate ack after restart: still the original, 200 + duplicate.
		code, ack := c.post(t, srv1.URL+"/api/v1/batches/"+batchSealed+"/chunks",
			map[string]any{"seq": 1, "payload": "z"})
		if code != 200 || ack["duplicate"] != true {
			t.Fatalf("post-restart retransmission: code=%d body=%v", code, ack)
		}

		// Repeated seal after restart: existing result, same sealedAt.
		code, reseal := c.post(t, srv2.URL+"/api/v1/batches/"+batchSealed+"/seal", nil)
		if code != 200 || reseal["sealedAt"] != sealTime {
			t.Fatalf("reseal after restart: code=%d body=%v", code, reseal)
		}
	}
}

// TestHTTPSealGroupAcrossInstances seals a two-batch group through one
// instance and verifies the verdict, the request-order response and the
// idempotent retry (stable sealedAt) through the other.
func TestHTTPSealGroupAcrossInstances(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 2})
	a := body["batchId"].(string)
	_, body = c.post(t, u2+"/api/v1/batches", map[string]int{"expectedChunks": 2})
	b := body["batchId"].(string)

	// Chunks alternate instances and arrive out of order.
	for _, step := range []struct {
		base, id, payload string
		seq               int
	}{
		{u2, a, "a2", 2},
		{u1, a, "a1", 1},
		{u1, b, "b2", 2},
		{u2, b, "b1", 1},
	} {
		code, resp := c.post(t, step.base+"/api/v1/batches/"+step.id+"/chunks",
			map[string]any{"seq": step.seq, "payload": step.payload})
		if code != 201 {
			t.Fatalf("chunk %s/%d: code=%d body=%v", step.id, step.seq, code, resp)
		}
	}

	code, group := c.post(t, u1+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{a, b}})
	if code != 200 {
		t.Fatalf("seal group: code=%d body=%v", code, group)
	}
	members, _ := group["batches"].([]any)
	if len(members) != 2 {
		t.Fatalf("seal group members: %v", group)
	}
	m0, _ := members[0].(map[string]any)
	m1, _ := members[1].(map[string]any)
	if m0["batchId"] != a || m1["batchId"] != b {
		t.Fatalf("response not in request order: %v", group)
	}
	for _, m := range []map[string]any{m0, m1} {
		if m["status"] != "SEALED" || m["sealedAt"] == nil ||
			int(m["received"].(float64)) != 2 || len(m["gaps"].([]any)) != 0 {
			t.Fatalf("member not cleanly sealed: %v", m)
		}
	}
	sealedAtA, _ := m0["sealedAt"].(string)
	sealedAtB, _ := m1["sealedAt"].(string)

	// Verdict visible on the other instance.
	code, snap := c.get(t, u2+"/api/v1/batches/"+a)
	if code != 200 || snap["status"] != "SEALED" || snap["sealedAt"] != sealedAtA {
		t.Fatalf("cross-instance group verdict: code=%d body=%v", code, snap)
	}

	// Retry in reversed order via the other instance: idempotent success,
	// sealedAt timestamps unchanged.
	code, retry := c.post(t, u2+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{b, a}})
	if code != 200 {
		t.Fatalf("retry: code=%d body=%v", code, retry)
	}
	rmembers, _ := retry["batches"].([]any)
	r0, _ := rmembers[0].(map[string]any)
	r1, _ := rmembers[1].(map[string]any)
	if r0["batchId"] != b || r1["batchId"] != a ||
		r0["sealedAt"] != sealedAtB || r1["sealedAt"] != sealedAtA {
		t.Fatalf("retry changed result: %v", retry)
	}
}

// TestHTTPSealGroupIncompleteLeavesGroupUnchanged: A is complete, B is short
// one chunk. The group seal must fail with 409 INCOMPLETE listing only B, and
// both members keep their pre-call state (A is NOT sealed).
func TestHTTPSealGroupIncompleteLeavesGroupUnchanged(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 2})
	a := body["batchId"].(string)
	_, body = c.post(t, u2+"/api/v1/batches", map[string]int{"expectedChunks": 3})
	b := body["batchId"].(string)

	for _, seq := range []int{1, 2} {
		c.post(t, u1+"/api/v1/batches/"+a+"/chunks",
			map[string]any{"seq": seq, "payload": "x"})
	}
	for _, seq := range []int{1, 3} { // B misses seq 2
		c.post(t, u2+"/api/v1/batches/"+b+"/chunks",
			map[string]any{"seq": seq, "payload": "y"})
	}

	code, group := c.post(t, u1+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{a, b}})
	if code != 409 || group["error"] != "INCOMPLETE" {
		t.Fatalf("incomplete group: code=%d body=%v", code, group)
	}
	members, _ := group["batches"].([]any)
	if len(members) != 1 {
		t.Fatalf("incomplete list should hold only B: %v", group)
	}
	m, _ := members[0].(map[string]any)
	gaps, _ := m["gaps"].([]any)
	if m["batchId"] != b || int(m["expected"].(float64)) != 3 ||
		int(m["received"].(float64)) != 2 || len(gaps) != 1 ||
		int(gaps[0].(float64)) != 2 {
		t.Fatalf("incomplete member report wrong: %v", m)
	}

	// Both members unchanged: still OPEN, no sealedAt.
	for _, id := range []string{a, b} {
		code, snap := c.get(t, u2+"/api/v1/batches/"+id)
		if code != 200 || snap["status"] != "OPEN" || snap["sealedAt"] != nil {
			t.Fatalf("member %s changed despite failed group seal: %v", id, snap)
		}
	}

	// Completing B lets the retry seal the whole group.
	c.post(t, u1+"/api/v1/batches/"+b+"/chunks", map[string]any{"seq": 2, "payload": "y"})
	code, group = c.post(t, u2+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{a, b}})
	if code != 200 {
		t.Fatalf("retry after completing B: code=%d body=%v", code, group)
	}
}

// TestHTTPSealGroupValidation covers the fixed 400/404 matrix for the group
// endpoint and proves failed requests leave every member untouched.
func TestHTTPSealGroupValidation(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 1})
	a := body["batchId"].(string)
	_, body = c.post(t, u2+"/api/v1/batches", map[string]int{"expectedChunks": 1})
	b := body["batchId"].(string)
	ghost := "ffffffffffffffffffffffffffffffff"

	tooMany := make([]string, 0, 101)
	for i := 0; i < 101; i++ {
		tooMany = append(tooMany, fmt.Sprintf("%032x", i+1))
	}

	cases := []struct {
		name       string
		url        string
		body       any
		raw        string
		wantStatus int
		wantError  string
	}{
		{"empty array", u1 + "/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{}}, "", 400, "INVALID_BATCH_GROUP"},
		{"single id", u1 + "/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{a}}, "", 400, "INVALID_BATCH_GROUP"},
		{"101 ids", u2 + "/api/v1/batches/seal-group",
			map[string]any{"batchIds": tooMany}, "", 400, "INVALID_BATCH_GROUP"},
		{"duplicate ids", u1 + "/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{a, b, a}}, "", 400, "INVALID_BATCH_GROUP"},
		{"bad id format", u1 + "/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{a, "not-a-batch-id"}}, "", 400, "INVALID_BATCH_GROUP"},
		{"uppercase id", u2 + "/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{a, strings.ToUpper(b)}}, "", 400, "INVALID_BATCH_GROUP"},
		{"non-string element", u1 + "/api/v1/batches/seal-group",
			nil, `{"batchIds":["` + a + `",7]}`, 400, "INVALID_BATCH_GROUP"},
		{"batchIds not array", u1 + "/api/v1/batches/seal-group",
			nil, `{"batchIds":"` + a + `"}`, 400, "INVALID_BATCH_GROUP"},
		{"missing batchIds", u2 + "/api/v1/batches/seal-group",
			map[string]any{"ids": []string{a, b}}, "", 400, "INVALID_BATCH_GROUP"},
		{"malformed json", u1 + "/api/v1/batches/seal-group",
			nil, `{"batchIds":[`, 400, "INVALID_BATCH_GROUP"},
		{"unknown id", u1 + "/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{a, ghost}}, "", 404, "BATCH_NOT_FOUND"},
		{"unknown id reversed", u2 + "/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{ghost, b}}, "", 404, "BATCH_NOT_FOUND"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var code int
			var resp map[string]any
			if tc.raw != "" {
				code, resp = c.postRaw(t, tc.url, tc.raw)
			} else {
				code, resp = c.post(t, tc.url, tc.body)
			}
			if code != tc.wantStatus || resp["error"] != tc.wantError {
				t.Fatalf("got code=%d body=%v, want %d %s", code, resp, tc.wantStatus, tc.wantError)
			}
		})
	}

	// No failed request touched the members.
	for _, id := range []string{a, b} {
		code, snap := c.get(t, u1+"/api/v1/batches/"+id)
		if code != 200 || snap["status"] != "OPEN" || snap["sealedAt"] != nil {
			t.Fatalf("member %s changed by rejected group requests: %v", id, snap)
		}
	}
}

// TestHTTPSealGroupThreeWayRace fires the [A,B] group on one instance, the
// reversed [B,A] group on the other and B's final chunk concurrently. With a
// hard timeout as deadlock tripwire, the only legal outcomes are the whole
// group SEALED (one transaction, identical sealedAt) or no new seal at all.
func TestHTTPSealGroupThreeWayRace(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	const rounds = 20
	for i := 0; i < rounds; i++ {
		_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 1})
		a := body["batchId"].(string)
		_, body = c.post(t, u2+"/api/v1/batches", map[string]int{"expectedChunks": 1})
		b := body["batchId"].(string)
		c.post(t, u1+"/api/v1/batches/"+a+"/chunks", map[string]any{"seq": 1, "payload": "a"})

		type result struct {
			code int
			body map[string]any
		}
		resCh := make(chan result, 3)
		go func() {
			code, resp := c.postQuiet(u1+"/api/v1/batches/seal-group",
				map[string]any{"batchIds": []string{a, b}})
			resCh <- result{code, resp}
		}()
		go func() {
			code, resp := c.postQuiet(u2+"/api/v1/batches/seal-group",
				map[string]any{"batchIds": []string{b, a}})
			resCh <- result{code, resp}
		}()
		go func() {
			code, resp := c.postQuiet(u1+"/api/v1/batches/"+b+"/chunks",
				map[string]any{"seq": 1, "payload": "final"})
			resCh <- result{code, resp}
		}()

		var results []result
		timeout := time.After(30 * time.Second)
		for len(results) < 3 {
			select {
			case r := <-resCh:
				results = append(results, r)
			case <-timeout:
				t.Fatalf("round %d: three-way race deadlocked", i)
			}
		}
		for _, r := range results {
			if r.code == 0 {
				t.Fatalf("round %d: transport error in race", i)
			}
		}

		code, snapA := c.get(t, u1+"/api/v1/batches/"+a)
		if code != 200 {
			t.Fatalf("round %d: status A: %d", i, code)
		}
		code, snapB := c.get(t, u2+"/api/v1/batches/"+b)
		if code != 200 {
			t.Fatalf("round %d: status B: %d", i, code)
		}
		sealedA := snapA["status"] == "SEALED"
		sealedB := snapB["status"] == "SEALED"
		if sealedA != sealedB {
			t.Fatalf("round %d: group partially sealed: A=%v B=%v", i, snapA["status"], snapB["status"])
		}
		if sealedA {
			if int(snapA["received"].(float64)) != 1 || int(snapB["received"].(float64)) != 1 {
				t.Fatalf("round %d: SEALED with gaps: A=%v B=%v", i, snapA, snapB)
			}
			if snapA["sealedAt"] != snapB["sealedAt"] {
				t.Fatalf("round %d: members sealed by different transactions: %v vs %v",
					i, snapA["sealedAt"], snapB["sealedAt"])
			}
		}
	}
}
