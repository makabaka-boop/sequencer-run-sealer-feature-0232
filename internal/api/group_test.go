package api_test

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// createBatchHTTP creates a batch through the given instance.
func createBatchHTTP(t *testing.T, c *client, base string, expected int) string {
	t.Helper()
	code, body := c.post(t, base+"/api/v1/batches", map[string]int{"expectedChunks": expected})
	if code != 201 {
		t.Fatalf("create batch: code=%d body=%v", code, body)
	}
	id, _ := body["batchId"].(string)
	if len(id) != 32 {
		t.Fatalf("bad batch id: %v", body)
	}
	return id
}

func fillBatchHTTP(t *testing.T, c *client, base, id string, expected int) {
	t.Helper()
	for seq := 1; seq <= expected; seq++ {
		code, body := c.post(t, base+"/api/v1/batches/"+id+"/chunks",
			map[string]any{"seq": seq, "payload": "data"})
		if code != 201 {
			t.Fatalf("fill seq=%d: code=%d body=%v", seq, code, body)
		}
	}
}

func TestHTTPSealGroupSuccessAcrossInstances(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	a := createBatchHTTP(t, c, u1, 2)
	b := createBatchHTTP(t, c, u2, 3)
	// Fill through opposite instances.
	fillBatchHTTP(t, c, u2, a, 2)
	fillBatchHTTP(t, c, u1, b, 3)

	seal := func(order []string) (int, map[string]any, []map[string]any) {
		code, body := c.post(t, u1+"/api/v1/batches/seal-group",
			map[string]any{"batchIds": order})
		var list []map[string]any
		if raw, ok := body["batches"].([]any); ok {
			for _, item := range raw {
				list = append(list, item.(map[string]any))
			}
		}
		return code, body, list
	}

	code, body, list := seal([]string{a, b})
	if code != 200 {
		t.Fatalf("seal group: code=%d body=%v", code, body)
	}
	if len(list) != 2 || list[0]["batchId"] != a || list[1]["batchId"] != b {
		t.Fatalf("batches must follow request order: %v", list)
	}
	stampA, stampB := list[0]["sealedAt"].(string), list[1]["sealedAt"].(string)
	if stampA == "" || stampB == "" {
		t.Fatalf("sealedAt missing: %v", list)
	}

	// Both SEALED on the other instance immediately.
	for _, id := range []string{a, b} {
		code, snap := c.get(t, u2+"/api/v1/batches/"+id)
		if code != 200 || snap["status"] != "SEALED" {
			t.Fatalf("member %s not sealed cross-instance: %d %v", id, code, snap)
		}
	}

	// Retry in reverse order on instance 2: idempotent, same sealedAt, and
	// latency must stay stable (no lock-order stalls or deadlock timeouts).
	const retries = 10
	var maxLatency time.Duration
	for i := 0; i < retries; i++ {
		start := time.Now()
		code, body2 := c.post(t, u2+"/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{b, a}})
		d := time.Since(start)
		if d > maxLatency {
			maxLatency = d
		}
		if code != 200 {
			t.Fatalf("retry %d: code=%d body=%v", i, code, body2)
		}
		raw := body2["batches"].([]any)
		gotB := raw[0].(map[string]any)
		gotA := raw[1].(map[string]any)
		if gotB["batchId"] != b || gotA["batchId"] != a {
			t.Fatalf("retry order wrong: %v", raw)
		}
		if gotA["sealedAt"] != stampA || gotB["sealedAt"] != stampB {
			t.Fatalf("retry overwrote sealedAt: %v %v want %s %s",
				gotA["sealedAt"], gotB["sealedAt"], stampA, stampB)
		}
	}
	if maxLatency > 2*time.Second {
		t.Fatalf("idempotent retries too slow (max %s): lock-order stall?", maxLatency)
	}
	t.Logf("max retry latency over %d calls: %s", retries, maxLatency)
}

func TestHTTPSealGroupMixedSealedPreservesStamp(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	a := createBatchHTTP(t, c, u1, 1)
	fillBatchHTTP(t, c, u1, a, 1)
	code, sealed := c.post(t, u2+"/api/v1/batches/"+a+"/seal", nil)
	if code != 200 {
		t.Fatalf("pre-seal: %d %v", code, sealed)
	}
	stampA, _ := sealed["sealedAt"].(string)

	b := createBatchHTTP(t, c, u2, 1)
	fillBatchHTTP(t, c, u1, b, 1)

	code, body := c.post(t, u1+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{a, b}})
	if code != 200 {
		t.Fatalf("mixed group: code=%d body=%v", code, body)
	}
	list := body["batches"].([]any)
	if list[0].(map[string]any)["sealedAt"] != stampA {
		t.Fatalf("existing sealedAt not preserved: %v", list[0])
	}
	if list[1].(map[string]any)["sealedAt"] == nil ||
		list[1].(map[string]any)["sealedAt"] == "" {
		t.Fatalf("new member missing sealedAt: %v", list[1])
	}
}

func TestHTTPSealGroupIncompleteLeavesAllOpen(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	// A complete (2/2), B partial (1/3).
	a := createBatchHTTP(t, c, u1, 2)
	fillBatchHTTP(t, c, u2, a, 2)
	b := createBatchHTTP(t, c, u1, 3)
	code, _ := c.post(t, u2+"/api/v1/batches/"+b+"/chunks",
		map[string]any{"seq": 1, "payload": "x"})
	if code != 201 {
		t.Fatalf("seed chunk: %d", code)
	}

	code, body := c.post(t, u1+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{a, b}})
	if code != 409 || body["error"] != "INCOMPLETE" {
		t.Fatalf("want 409 INCOMPLETE, got %d %v", code, body)
	}
	list, ok := body["batches"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("only the incomplete member must be listed: %v", body)
	}
	inc := list[0].(map[string]any)
	if inc["batchId"] != b || inc["status"] != "OPEN" ||
		int(inc["expected"].(float64)) != 3 || int(inc["received"].(float64)) != 1 {
		t.Fatalf("incomplete entry wrong: %v", inc)
	}
	gaps := inc["gaps"].([]any)
	if len(gaps) != 2 || int(gaps[0].(float64)) != 2 || int(gaps[1].(float64)) != 3 {
		t.Fatalf("gaps wrong: %v", gaps)
	}

	// Nothing changed: A is still OPEN despite being complete, B is unchanged.
	code, snapA := c.get(t, u2+"/api/v1/batches/"+a)
	if code != 200 || snapA["status"] != "OPEN" {
		t.Fatalf("A must remain OPEN after failed group: %d %v", code, snapA)
	}
	code, snapB := c.get(t, u1+"/api/v1/batches/"+b)
	if code != 200 || snapB["status"] != "OPEN" ||
		int(snapB["received"].(float64)) != 1 || len(snapB["gaps"].([]any)) != 2 {
		t.Fatalf("B changed after failed group: %d %v", code, snapB)
	}

	// Complete B's missing pieces; group now succeeds, sealing A and B.
	for _, seq := range []int{2, 3} {
		c.post(t, u2+"/api/v1/batches/"+b+"/chunks",
			map[string]any{"seq": seq, "payload": "y"})
	}
	code, body = c.post(t, u2+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{b, a}})
	if code != 200 {
		t.Fatalf("follow-up group: %d %v", code, body)
	}
}

func TestHTTPSealGroupValidation(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	a := createBatchHTTP(t, c, u1, 1)
	b := createBatchHTTP(t, c, u2, 1)
	unknown := "ffffffffffffffffffffffffffffffff"

	// Existing members stay untouched across every rejected request.
	assertUntouched := func() {
		t.Helper()
		for _, id := range []string{a, b} {
			code, snap := c.get(t, u1+"/api/v1/batches/"+id)
			if code != 200 || snap["status"] != "OPEN" ||
				int(snap["received"].(float64)) != 0 {
				t.Fatalf("member %s had side effects: %v", id, snap)
			}
		}
	}

	expect400 := func(name, raw string) {
		t.Helper()
		code, body := c.postRaw(t, u1+"/api/v1/batches/seal-group", raw)
		if code != 400 || body["error"] != "INVALID_BATCH_GROUP" {
			t.Fatalf("%s: got %d %v, want 400 INVALID_BATCH_GROUP", name, code, body)
		}
		assertUntouched()
	}

	expect400("empty body", "")
	expect400("malformed json", `{"batchIds": [`)
	expect400("missing batchIds", `{}`)
	expect400("extra field", `{"batchIds":["`+a+`","`+b+`"],"x":1}`)
	expect400("empty array", `{"batchIds":[]}`)
	expect400("single member", `{"batchIds":["`+a+`"]}`)
	expect400("duplicate ids", `{"batchIds":["`+a+`","`+a+`"]}`)
	expect400("uppercase id", `{"batchIds":["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","`+b+`"]}`)
	expect400("short id", `{"batchIds":["abc","`+b+`"]}`)
	expect400("non-string id", `{"batchIds":[1,"`+b+`"]}`)
	expect400("batchIds not array", `{"batchIds":"`+a+`"}`)

	// 101 members exceeds the upper bound (checked before duplicates).
	body101 := `{"batchIds":[`
	for i := 0; i < 101; i++ {
		if i > 0 {
			body101 += ","
		}
		body101 += `"` + unknown + `"`
	}
	body101 += `]}`
	expect400("101 members", body101)

	// 100 validly-formed but unknown IDs pass validation and hit 404.
	body100 := `{"batchIds":[`
	for i := 0; i < 100; i++ {
		if i > 0 {
			body100 += ","
		}
		body100 += fmt.Sprintf(`"ffffff%026x"`, i)
	}
	body100 += `]}`
	code, body := c.postRaw(t, u2+"/api/v1/batches/seal-group", body100)
	if code != 404 || body["error"] != "BATCH_NOT_FOUND" {
		t.Fatalf("unknown members: got %d %v, want 404 BATCH_NOT_FOUND", code, body)
	}
	assertUntouched()

	// One unknown among real members: 404, no sealing of the known ones.
	code, body = c.post(t, u1+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{a, unknown, b}})
	if code != 404 || body["error"] != "BATCH_NOT_FOUND" {
		t.Fatalf("unknown among known: got %d %v", code, body)
	}
	assertUntouched()

	// Unknown first, known second — still 404 with no side effect.
	code, _ = c.post(t, u2+"/api/v1/batches/seal-group",
		map[string]any{"batchIds": []string{unknown, a}})
	if code != 404 {
		t.Fatalf("unknown first: got %d", code)
	}
	assertUntouched()
}

// TestHTTPSealGroupThreeWayInterleave drives group seals [A,B] and [B,A] on
// different instances at the same time as B's final chunk. A is complete from
// the start. Lock acquisition is ascending-ID everywhere, so this can never
// deadlock, and the settled state is binary: both batches SEALED (a group
// committed after the chunk landed) or both still OPEN with no new seal (every
// group lost the completeness verdict and rolled back).
func TestHTTPSealGroupThreeWayInterleave(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	const rounds = 25
	for r := 0; r < rounds; r++ {
		a := createBatchHTTP(t, c, u1, 1)
		b := createBatchHTTP(t, c, u2, 1)
		fillBatchHTTP(t, c, u2, a, 1) // A complete; B has zero chunks.

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		var codeAB, codeBA, codeChunk int
		go func() {
			defer wg.Done()
			<-start
			codeAB, _ = c.post(t, u1+"/api/v1/batches/seal-group",
				map[string]any{"batchIds": []string{a, b}})
		}()
		go func() {
			defer wg.Done()
			<-start
			codeBA, _ = c.post(t, u2+"/api/v1/batches/seal-group",
				map[string]any{"batchIds": []string{b, a}})
		}()
		go func() {
			defer wg.Done()
			<-start
			codeChunk, _ = c.post(t, u1+"/api/v1/batches/"+b+"/chunks",
				map[string]any{"seq": 1, "payload": "last"})
		}()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		close(start)
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatalf("round %d: three-way interleave timed out (deadlock?)", r)
		}

		if codeChunk != 201 {
			t.Fatalf("round %d: final chunk status %d", r, codeChunk)
		}
		for name, code := range map[string]int{"AB": codeAB, "BA": codeBA} {
			if code != 200 && code != 409 {
				t.Fatalf("round %d: group %s unexpected status %d", r, name, code)
			}
		}

		_, snapA := c.get(t, u1+"/api/v1/batches/"+a)
		_, snapB := c.get(t, u2+"/api/v1/batches/"+b)
		stA, _ := snapA["status"].(string)
		stB, _ := snapB["status"].(string)
		switch {
		case stA == "SEALED" && stB == "SEALED":
			// Whole group committed; at least one group call saw 200, and
			// neither sealed batch may report gaps.
			if int(snapA["received"].(float64)) != 1 ||
				int(snapB["received"].(float64)) != 1 ||
				len(snapB["gaps"].([]any)) != 0 {
				t.Fatalf("round %d: sealed with gaps: %v %v", r, snapA, snapB)
			}
		case stA == "OPEN" && stB == "OPEN":
			// No new seal happened; both groups must have reported 409.
			if codeAB != 409 || codeBA != 409 {
				t.Fatalf("round %d: both OPEN but group codes AB=%d BA=%d", r, codeAB, codeBA)
			}
		default:
			t.Fatalf("round %d: partial sealing forbidden: A=%s B=%s", r, stA, stB)
		}

		// Convergence: once B is complete, another group seal always
		// succeeds and preserves the first sealedAt if already sealed.
		code, body := c.post(t, u1+"/api/v1/batches/seal-group",
			map[string]any{"batchIds": []string{a, b}})
		if code != 200 {
			t.Fatalf("round %d: settling group seal failed: %d %v", r, code, body)
		}
	}
}

// TestHTTPSealGroupSingleSealInterleave mixes group seal with the legacy
// single-batch seal and B's final chunk across both instances.
func TestHTTPSealGroupSingleSealInterleave(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	const rounds = 25
	for r := 0; r < rounds; r++ {
		a := createBatchHTTP(t, c, u1, 1)
		b := createBatchHTTP(t, c, u2, 1)
		fillBatchHTTP(t, c, u2, a, 1)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		var codeSingle, codeGroup, codeChunk int
		go func() {
			defer wg.Done()
			<-start
			codeSingle, _ = c.post(t, u1+"/api/v1/batches/"+a+"/seal", nil)
		}()
		go func() {
			defer wg.Done()
			<-start
			codeGroup, _ = c.post(t, u2+"/api/v1/batches/seal-group",
				map[string]any{"batchIds": []string{b, a}})
		}()
		go func() {
			defer wg.Done()
			<-start
			codeChunk, _ = c.post(t, u2+"/api/v1/batches/"+b+"/chunks",
				map[string]any{"seq": 1, "payload": "z"})
		}()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		close(start)
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatalf("round %d: single/group interleave timed out", r)
		}

		if codeSingle != 200 && codeSingle != 409 {
			t.Fatalf("round %d: single seal code %d", r, codeSingle)
		}
		if codeGroup != 200 && codeGroup != 409 {
			t.Fatalf("round %d: group seal code %d", r, codeGroup)
		}
		if codeChunk != 201 {
			t.Fatalf("round %d: chunk code %d", r, codeChunk)
		}
		_, snapA := c.get(t, u1+"/api/v1/batches/"+a)
		_, snapB := c.get(t, u2+"/api/v1/batches/"+b)
		stA, _ := snapA["status"].(string)
		stB, _ := snapB["status"].(string)
		if stA == "SEALED" && stB == "OPEN" && codeGroup != 409 {
			t.Fatalf("round %d: A sealed alone but group did not report 409 (%d)", r, codeGroup)
		}
		if (stA == "SEALED") != (stB == "SEALED") {
			// A-only is allowed only when the group verdict ran while B was
			// missing its chunk; assert B was still OPEN at that time via the
			// group's 409 above. Here just ensure final state converges.
			code, body := c.post(t, u1+"/api/v1/batches/seal-group",
				map[string]any{"batchIds": []string{a, b}})
			if code != 200 {
				t.Fatalf("round %d: convergence failed: %d %v", r, code, body)
			}
		}
	}
}
