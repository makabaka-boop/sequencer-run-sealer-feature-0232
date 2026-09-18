package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"batchseal/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests require a real PostgreSQL reachable via TEST_DATABASE_URL
// (default matches the compose service). They skip when it is unavailable,
// e.g. when run without a database present.
func testURL() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/batchseal?sslmode=disable"
}

var (
	basePoolOnce sync.Once
	basePool     *pgxpool.Pool
	basePoolErr  error
)

func ensureBasePool() bool {
	basePoolOnce.Do(func() {
		c, err := pgxpool.New(context.Background(), testURL())
		if err == nil {
			err = c.Ping(context.Background())
		}
		basePool, basePoolErr = c, err
	})
	return basePoolErr == nil
}

// newStore points every test at an isolated schema inside the shared
// database. The returned function opens another store on the same schema,
// modelling a second API process arbitrating through the same database.
func newStore(ctx context.Context, t *testing.T) (s *store.Store, peer func() *store.Store) {
	t.Helper()

	if !ensureBasePool() {
		t.Skipf("real PostgreSQL not available at %q: %v", testURL(), basePoolErr)
	}

	schema := fmt.Sprintf("t_%d", time.Now().UnixNano())
	if _, err := basePool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = basePool.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
	})

	open := func() *store.Store {
		st, err := store.New(ctx, schemaURL(schema))
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return st
	}
	return open(), open
}

func schemaURL(schema string) string {
	sep := "?"
	if containsQ(testURL()) {
		sep = "&"
	}
	return testURL() + sep + "search_path=" + schema
}

func containsQ(u string) bool {
	for i := 0; i < len(u); i++ {
		if u[i] == '?' {
			return true
		}
	}
	return false
}

func mustBatch(ctx context.Context, t *testing.T, s *store.Store, expected int) *store.Batch {
	t.Helper()
	b, err := s.CreateBatch(ctx, expected)
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	return b
}

// TestConcurrentFirstBootMigrate points many brand-new stores at one fresh
// schema at the same time, modelling every API instance racing to apply the
// schema on an empty database. Exactly one migration must win the advisory
// lock; the rest must complete without a catalog duplicate-key error.
func TestConcurrentFirstBootMigrate(t *testing.T) {
	ctx := context.Background()
	if !ensureBasePool() {
		t.Skipf("real PostgreSQL not available at %q: %v", testURL(), basePoolErr)
	}
	schema := fmt.Sprintf("mig_%d", time.Now().UnixNano())
	if _, err := basePool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create empty schema: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = basePool.Exec(cctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			st, err := store.New(ctx, schemaURL(schema))
			if err != nil {
				errs[i] = err
				return
			}
			// Prove the migrated schema is usable, then release.
			_, err = st.CreateBatch(ctx, 1)
			if err != nil {
				errs[i] = err
			}
			st.Close()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent migrator %d failed: %v", i, err)
		}
	}
}

func TestCreateBatchValidatesRange(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	for _, n := range []int{0, -1, 10001} {
		if _, err := s.CreateBatch(ctx, n); err == nil {
			t.Fatalf("expected error for expectedChunks=%d", n)
		}
	}
}

func TestOutOfOrderThenSnapshot(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b := mustBatch(ctx, t, s, 4)

	for _, seq := range []int{4, 2, 1} {
		res, err := s.SubmitChunk(ctx, b.ID, seq, []byte{byte('a' + seq)})
		if err != nil {
			t.Fatalf("submit %d: %v", seq, err)
		}
		if !res.Created {
			t.Fatalf("seq %d should be a first write", seq)
		}
	}

	snap, err := s.Snapshot(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Received != 3 || len(snap.Gaps) != 1 || snap.Gaps[0] != 3 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestRetransmissionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b := mustBatch(ctx, t, s, 2)

	first, err := s.SubmitChunk(ctx, b.ID, 1, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.SubmitChunk(ctx, b.ID, 1, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || second.Created {
		t.Fatalf("created flags wrong: first=%+v second=%+v", first, second)
	}
	if !first.ReceivedAt.Equal(second.ReceivedAt) {
		t.Fatalf("original confirmation time changed: %v vs %v", first.ReceivedAt, second.ReceivedAt)
	}

	// Same seq, different bytes -> conflict.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("hellX")); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestUTF8ByteIdentity(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b := mustBatch(ctx, t, s, 1)

	// "é" as UTF-8 (0xC3 0xA9) versus Latin-1 (0xE9): same letter, different
	// bytes, so the retransmission must be a conflict.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("caf\xc3\xa9")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("caf\xe9")); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("different UTF-8 bytes should conflict, got %v", err)
	}
	// Identical bytes are the original acknowledgement.
	res, err := s.SubmitChunk(ctx, b.ID, 1, []byte("caf\xc3\xa9"))
	if err != nil || res.Created {
		t.Fatalf("identical retransmission wrong: res=%+v err=%v", res, err)
	}
}

func TestSeqRangeRejected(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b := mustBatch(ctx, t, s, 2)

	for _, seq := range []int{0, -1, 3, 10000} {
		if _, err := s.SubmitChunk(ctx, b.ID, seq, []byte("x")); !errors.Is(err, store.ErrSeqRange) {
			t.Fatalf("seq=%d: want ErrSeqRange, got %v", seq, err)
		}
	}
}

func TestSealIncompleteKeepsOpen(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b := mustBatch(ctx, t, s, 3)

	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.SealBatch(ctx, b.ID)
	if !errors.Is(err, store.ErrIncomplete) {
		t.Fatalf("want ErrIncomplete, got %v", err)
	}
	if snap.Status != store.StatusOpen {
		t.Fatalf("status changed to %s despite missing chunks", snap.Status)
	}
	if len(snap.Gaps) != 2 || snap.Gaps[0] != 2 || snap.Gaps[1] != 3 {
		t.Fatalf("gaps wrong: %v", snap.Gaps)
	}

	again, err := s.Snapshot(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != store.StatusOpen {
		t.Fatalf("batch did not remain OPEN: %s", again.Status)
	}
}

func TestSealCompleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b := mustBatch(ctx, t, s, 2)

	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitChunk(ctx, b.ID, 2, []byte("b")); err != nil {
		t.Fatal(err)
	}
	sealed, err := s.SealBatch(ctx, b.ID)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if sealed.Status != store.StatusSealed || sealed.SealedAt == nil {
		t.Fatalf("seal result wrong: %+v", sealed)
	}

	// Repeated seal returns the existing result with the same sealedAt.
	again, err := s.SealBatch(ctx, b.ID)
	if err != nil {
		t.Fatalf("reseal: %v", err)
	}
	if again.Status != store.StatusSealed || !again.SealedAt.Equal(*sealed.SealedAt) {
		t.Fatalf("reseal altered result: %+v vs %+v", sealed, again)
	}

	// Sealed batch rejects changed content, allows identical retransmission.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("changed")); !errors.Is(err, store.ErrSealed) {
		t.Fatalf("want ErrSealed, got %v", err)
	}
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("a")); err != nil {
		t.Fatalf("identical retransmission after seal rejected: %v", err)
	}
}

func TestDuplicateChunksDoNotInflateCount(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b := mustBatch(ctx, t, s, 1)

	for range 5 {
		if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := s.Snapshot(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Received != 1 || len(snap.Gaps) != 0 {
		t.Fatalf("count inflated by duplicates: %+v", snap)
	}
}

// TestSealRacesFinalChunk fires the seal and the last missing chunk
// concurrently from independent pools (simulating two API processes). The
// batch can never end up SEALED with a gap.
func TestSealRacesFinalChunk(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)
	other := newPeer()
	defer other.Close()

	const rounds = 30
	start := make(chan struct{})
	for i := 0; i < rounds; i++ {
		b := mustBatch(ctx, t, s, 1)

		var wg sync.WaitGroup
		wg.Add(2)
		var sealErr error
		go func() {
			defer wg.Done()
			<-start
			_, sealErr = s.SealBatch(ctx, b.ID)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = other.SubmitChunk(ctx, b.ID, 1, []byte("final"))
		}()
		close(start)
		wg.Wait()

		snap, err := s.Snapshot(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		switch snap.Status {
		case store.StatusOpen:
			if !errors.Is(sealErr, store.ErrIncomplete) {
				t.Fatalf("OPEN but seal error was %v", sealErr)
			}
			// OPEN is consistent whether the chunk is still missing or it
			// landed just after the seal verdict; SEALED-with-gaps is the
			// forbidden outcome.
			if snap.Received+len(snap.Gaps) != snap.ExpectedChunks {
				t.Fatalf("OPEN snapshot inconsistent: %+v", snap)
			}
			// If the chunk did land, another seal must now succeed atomically.
			if len(snap.Gaps) == 0 {
				again, err := s.SealBatch(ctx, b.ID)
				if err != nil || again.Status != store.StatusSealed {
					t.Fatalf("follow-up seal failed: snap=%+v err=%v", again, err)
				}
			}
		case store.StatusSealed:
			if len(snap.Gaps) != 0 || snap.Received != snap.ExpectedChunks {
				t.Fatalf("SEALED batch has gaps: %+v", snap)
			}
		default:
			t.Fatalf("unknown status %s", snap.Status)
		}
		start = make(chan struct{})
	}
}

// TestConflictingConcurrentSubmits hammers one seq with two payloads from two
// independent pools: exactly one payload must win, the other must see a
// conflict, and exactly one row must remain stored.
func TestConflictingConcurrentSubmits(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)
	other := newPeer()
	defer other.Close()

	const rounds = 30
	for range rounds {
		b := mustBatch(ctx, t, s, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		var err1, err2 error
		go func() { defer wg.Done(); _, err1 = s.SubmitChunk(ctx, b.ID, 1, []byte("AAAA")) }()
		go func() { defer wg.Done(); _, err2 = other.SubmitChunk(ctx, b.ID, 1, []byte("BBBB")) }()
		wg.Wait()

		conflicts := 0
		for _, e := range []error{err1, err2} {
			if errors.Is(e, store.ErrConflict) {
				conflicts++
			} else if e != nil {
				t.Fatalf("unexpected submit error: %v", e)
			}
		}
		if conflicts != 1 {
			t.Fatalf("expected exactly 1 conflict, got %d (err1=%v err2=%v)", conflicts, err1, err2)
		}
		snap, err := s.Snapshot(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Received != 1 {
			t.Fatalf("expected 1 stored chunk, got %d", snap.Received)
		}
	}
}

// TestSealGroupSuccessAndRetryStable seals a complete group and confirms the
// response follows request order and a retry (even reversed) is an idempotent
// success returning the original sealedAt for every member.
func TestSealGroupSuccessAndRetryStable(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)

	a := mustBatch(ctx, t, s, 2)
	b := mustBatch(ctx, t, s, 3)
	for seq, p := range map[int]string{1: "a1", 2: "a2"} {
		if _, err := s.SubmitChunk(ctx, a.ID, seq, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	for seq, p := range map[int]string{1: "b1", 2: "b2", 3: "b3"} {
		if _, err := s.SubmitChunk(ctx, b.ID, seq, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}

	snaps, err := s.SealGroup(ctx, []string{a.ID, b.ID})
	if err != nil {
		t.Fatalf("seal group: %v", err)
	}
	if len(snaps) != 2 || snaps[0].ID != a.ID || snaps[1].ID != b.ID {
		t.Fatalf("snapshots not in request order: %+v", snaps)
	}
	for _, snap := range snaps {
		if snap.Status != store.StatusSealed || snap.SealedAt == nil ||
			snap.Received != snap.ExpectedChunks || len(snap.Gaps) != 0 {
			t.Fatalf("member not cleanly sealed: %+v", snap)
		}
	}
	sealedAtA, sealedAtB := *snaps[0].SealedAt, *snaps[1].SealedAt

	// Retry in reversed order: idempotent, original sealedAt preserved.
	again, err := s.SealGroup(ctx, []string{b.ID, a.ID})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if again[0].ID != b.ID || again[1].ID != a.ID {
		t.Fatalf("retry snapshots not in request order: %+v", again)
	}
	if !again[0].SealedAt.Equal(sealedAtB) || !again[1].SealedAt.Equal(sealedAtA) {
		t.Fatalf("retry changed sealedAt: %v / %v", again[0].SealedAt, again[1].SealedAt)
	}
}

// TestSealGroupIncompleteLeavesWholeGroupUnchanged proves atomicity: with A
// complete and B short of chunks, the group seal fails and A is NOT sealed
// either — no per-member SealBatch loop leaking partial commits.
func TestSealGroupIncompleteLeavesWholeGroupUnchanged(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)

	a := mustBatch(ctx, t, s, 2) // will be complete
	b := mustBatch(ctx, t, s, 3) // will miss seq 2
	for seq, p := range map[int]string{1: "a1", 2: "a2"} {
		if _, err := s.SubmitChunk(ctx, a.ID, seq, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	for seq, p := range map[int]string{1: "b1", 3: "b3"} {
		if _, err := s.SubmitChunk(ctx, b.ID, seq, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}

	snaps, err := s.SealGroup(ctx, []string{a.ID, b.ID})
	if !errors.Is(err, store.ErrIncomplete) {
		t.Fatalf("want ErrIncomplete, got %v", err)
	}
	if len(snaps) != 2 || snaps[0].ID != a.ID || snaps[1].ID != b.ID {
		t.Fatalf("error snapshots not in request order: %+v", snaps)
	}
	if snaps[1].Received != 2 || len(snaps[1].Gaps) != 1 || snaps[1].Gaps[0] != 2 {
		t.Fatalf("incomplete member snapshot wrong: %+v", snaps[1])
	}

	// Neither member changed: A must still be OPEN with no sealedAt.
	for _, id := range []string{a.ID, b.ID} {
		snap, err := s.Snapshot(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Status != store.StatusOpen || snap.SealedAt != nil {
			t.Fatalf("member changed despite failed group seal: %+v", snap)
		}
	}

	// Completing B makes the retry seal the whole group.
	if _, err := s.SubmitChunk(ctx, b.ID, 2, []byte("b2")); err != nil {
		t.Fatal(err)
	}
	sealed, err := s.SealGroup(ctx, []string{a.ID, b.ID})
	if err != nil {
		t.Fatalf("retry after completing B: %v", err)
	}
	if sealed[0].Status != store.StatusSealed || sealed[1].Status != store.StatusSealed {
		t.Fatalf("group not sealed after completing B: %+v", sealed)
	}
}

// TestSealGroupUnknownIDHasNoSideEffects: a ghost id fails the whole request
// with ErrNotFound and the existing, complete member stays OPEN.
func TestSealGroupUnknownIDHasNoSideEffects(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)

	a := mustBatch(ctx, t, s, 1)
	if _, err := s.SubmitChunk(ctx, a.ID, 1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	ghost := "ffffffffffffffffffffffffffffffff" // valid format, never created

	if _, err := s.SealGroup(ctx, []string{a.ID, ghost}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	snap, err := s.Snapshot(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != store.StatusOpen || snap.SealedAt != nil {
		t.Fatalf("known member changed by unknown-id group: %+v", snap)
	}
}

// TestSealGroupKeepsExistingSealedAt: a member sealed individually before the
// group call is an idempotent success and keeps its original sealedAt while
// the remaining OPEN members seal.
func TestSealGroupKeepsExistingSealedAt(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)

	a := mustBatch(ctx, t, s, 1)
	b := mustBatch(ctx, t, s, 1)
	for _, id := range []string{a.ID, b.ID} {
		if _, err := s.SubmitChunk(ctx, id, 1, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	sealedA, err := s.SealBatch(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}

	snaps, err := s.SealGroup(ctx, []string{a.ID, b.ID})
	if err != nil {
		t.Fatalf("group with pre-sealed member: %v", err)
	}
	if !snaps[0].SealedAt.Equal(*sealedA.SealedAt) {
		t.Fatalf("group seal overwrote existing sealedAt: %v vs %v", snaps[0].SealedAt, sealedA.SealedAt)
	}
	if snaps[1].Status != store.StatusSealed || snaps[1].SealedAt == nil {
		t.Fatalf("open member not sealed by group: %+v", snaps[1])
	}
}

// TestSealGroupThreeWayRace fires the [A,B] group, the reversed [B,A] group
// and B's final chunk from three independent pools at the same time. The
// shared row-lock arbitration must serialise them without deadlock, and the
// only legal outcomes are the whole group SEALED (by exactly one transaction,
// so both members carry the same sealedAt) or no new seal at all.
func TestSealGroupThreeWayRace(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)
	p2 := newPeer()
	defer p2.Close()
	p3 := newPeer()
	defer p3.Close()

	const rounds = 25
	for i := 0; i < rounds; i++ {
		a := mustBatch(ctx, t, s, 1)
		b := mustBatch(ctx, t, s, 1)
		if _, err := s.SubmitChunk(ctx, a.ID, 1, []byte("a")); err != nil {
			t.Fatal(err)
		}

		opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		var wg sync.WaitGroup
		wg.Add(3)
		var errAB, errBA error
		go func() { defer wg.Done(); _, errAB = s.SealGroup(opCtx, []string{a.ID, b.ID}) }()
		go func() { defer wg.Done(); _, errBA = p2.SealGroup(opCtx, []string{b.ID, a.ID}) }()
		go func() { defer wg.Done(); _, _ = p3.SubmitChunk(opCtx, b.ID, 1, []byte("final")) }()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d: three-way seal race deadlocked", i)
		}
		cancel()

		for _, e := range []error{errAB, errBA} {
			if e != nil && !errors.Is(e, store.ErrIncomplete) {
				t.Fatalf("round %d: unexpected group error %v", i, e)
			}
		}

		snapA, err := s.Snapshot(ctx, a.ID)
		if err != nil {
			t.Fatal(err)
		}
		snapB, err := s.Snapshot(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		sealedA := snapA.Status == store.StatusSealed
		sealedB := snapB.Status == store.StatusSealed
		if sealedA != sealedB {
			t.Fatalf("round %d: group partially sealed: A=%s B=%s", i, snapA.Status, snapB.Status)
		}
		if sealedA {
			if len(snapA.Gaps) != 0 || len(snapB.Gaps) != 0 {
				t.Fatalf("round %d: SEALED with gaps: %+v %+v", i, snapA, snapB)
			}
			if !snapA.SealedAt.Equal(*snapB.SealedAt) {
				t.Fatalf("round %d: members sealed by different transactions: %v vs %v",
					i, snapA.SealedAt, snapB.SealedAt)
			}
		} else {
			// No new seal happened: both group calls must have reported
			// INCOMPLETE and nothing may have transitioned.
			if !errors.Is(errAB, store.ErrIncomplete) || !errors.Is(errBA, store.ErrIncomplete) {
				t.Fatalf("round %d: unsealed but errors were %v / %v", i, errAB, errBA)
			}
		}
	}
}

// TestSealGroupVsSingleSealRace: a group seal and a single-batch seal share
// the same row locks, so they serialise — the loser observes the winner's
// verdict idempotently and no sealedAt is ever overwritten.
func TestSealGroupVsSingleSealRace(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)
	other := newPeer()
	defer other.Close()

	const rounds = 25
	for i := 0; i < rounds; i++ {
		a := mustBatch(ctx, t, s, 1)
		b := mustBatch(ctx, t, s, 1)
		for _, id := range []string{a.ID, b.ID} {
			if _, err := s.SubmitChunk(ctx, id, 1, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var groupErr, singleErr error
		go func() { defer wg.Done(); _, groupErr = s.SealGroup(ctx, []string{a.ID, b.ID}) }()
		go func() { defer wg.Done(); _, singleErr = other.SealBatch(ctx, b.ID) }()
		wg.Wait()

		if groupErr != nil || singleErr != nil {
			t.Fatalf("round %d: group=%v single=%v", i, groupErr, singleErr)
		}
		snapA, err := s.Snapshot(ctx, a.ID)
		if err != nil {
			t.Fatal(err)
		}
		snapB, err := s.Snapshot(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snapA.Status != store.StatusSealed || snapB.Status != store.StatusSealed {
			t.Fatalf("round %d: expected both SEALED: %s / %s", i, snapA.Status, snapB.Status)
		}

		// A second group seal must not move either sealedAt.
		again, err := s.SealGroup(ctx, []string{a.ID, b.ID})
		if err != nil {
			t.Fatal(err)
		}
		if !again[0].SealedAt.Equal(*snapA.SealedAt) || !again[1].SealedAt.Equal(*snapB.SealedAt) {
			t.Fatalf("round %d: sealedAt moved on reseal", i)
		}
	}
}

// TestRestartConsistency closes every connection and opens a fresh store,
// modelling a full restart of all API instances: acknowledgements, gaps and
// sealing verdict must survive unchanged.
func TestRestartConsistency(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)

	open := mustBatch(ctx, t, s, 3)
	sealed := mustBatch(ctx, t, s, 2)

	if _, err := s.SubmitChunk(ctx, open.ID, 1, []byte("o1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitChunk(ctx, open.ID, 3, []byte("o3")); err != nil {
		t.Fatal(err)
	}
	openAck, err := s.SubmitChunk(ctx, open.ID, 1, []byte("o1"))
	if err != nil {
		t.Fatal(err)
	}

	for seq, p := range map[int]string{1: "s1", 2: "s2"} {
		if _, err := s.SubmitChunk(ctx, sealed.ID, seq, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	sealedSnap, err := s.SealBatch(ctx, sealed.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate full API restart: drop all pools and reconnect.
	s.Close()
	restarted := newPeer()
	defer restarted.Close()

	gotOpen, err := restarted.Snapshot(ctx, open.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotOpen.Status != store.StatusOpen || gotOpen.Received != 2 {
		t.Fatalf("open batch state not preserved after restart: %+v", gotOpen)
	}
	if len(gotOpen.Gaps) != 1 || gotOpen.Gaps[0] != 2 {
		t.Fatalf("gaps not preserved after restart: %+v", gotOpen)
	}

	// The retransmission acknowledgement after restart must still be the
	// original confirmation, not a new record.
	ack2, err := restarted.SubmitChunk(ctx, open.ID, 1, []byte("o1"))
	if err != nil {
		t.Fatal(err)
	}
	if ack2.Created || !ack2.ReceivedAt.Equal(openAck.ReceivedAt) {
		t.Fatalf("acknowledgement changed after restart: %+v vs %+v", ack2, openAck)
	}

	gotSealed, err := restarted.Snapshot(ctx, sealed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotSealed.Status != store.StatusSealed || !gotSealed.SealedAt.Equal(*sealedSnap.SealedAt) {
		t.Fatalf("sealed verdict changed after restart: %+v", gotSealed)
	}
	if _, err := restarted.SealBatch(ctx, sealed.ID); err != nil {
		t.Fatalf("repeated seal after restart should be idempotent: %v", err)
	}
}
