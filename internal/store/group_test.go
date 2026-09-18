package store_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"batchseal/internal/store"
)

// completeBatch creates a batch and fills every chunk 1..expected.
func completeBatch(ctx context.Context, t *testing.T, s *store.Store, expected int) *store.Batch {
	t.Helper()
	b := mustBatch(ctx, t, s, expected)
	for seq := 1; seq <= expected; seq++ {
		if _, err := s.SubmitChunk(ctx, b.ID, seq, []byte{byte('a' + seq%26)}); err != nil {
			t.Fatalf("submit %d: %v", seq, err)
		}
	}
	return b
}

func snapMap(snaps []*store.Snapshot) map[string]*store.Snapshot {
	m := make(map[string]*store.Snapshot, len(snaps))
	for _, sn := range snaps {
		m[sn.ID] = sn
	}
	return m
}

// TestSealGroupAtomicSuccess: two OPEN, complete members seal together; the
// response follows request order and both carry sealedAt.
func TestSealGroupAtomicSuccess(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	a := completeBatch(ctx, t, s, 2)
	b := completeBatch(ctx, t, s, 3)

	snaps, _, err := s.SealGroup(ctx, []string{a.ID, b.ID})
	if err != nil {
		t.Fatalf("seal group: %v", err)
	}
	if len(snaps) != 2 || snaps[0].ID != a.ID || snaps[1].ID != b.ID {
		t.Fatalf("snapshots not in request order: %v", snaps)
	}
	for _, sn := range snaps {
		if sn.Status != store.StatusSealed || sn.SealedAt == nil {
			t.Fatalf("member not sealed: %+v", sn)
		}
		if sn.Received != sn.ExpectedChunks || len(sn.Gaps) != 0 {
			t.Fatalf("sealed member inconsistent: %+v", sn)
		}
	}
}

// TestSealGroupReverseOrderRequestsLockAscending: request order [B,A] still
// returns snapshots in request order; the store locks in ascending ID order.
func TestSealGroupReverseOrderRequestsLockAscending(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	a := completeBatch(ctx, t, s, 1)
	b := completeBatch(ctx, t, s, 1)

	snaps, _, err := s.SealGroup(ctx, []string{b.ID, a.ID})
	if err != nil {
		t.Fatalf("seal group reversed: %v", err)
	}
	if len(snaps) != 2 || snaps[0].ID != b.ID || snaps[1].ID != a.ID {
		t.Fatalf("response must follow request order: %v", snaps)
	}
}

// TestSealGroupAlreadySealedIsIdempotent: an already SEALED member keeps its
// original sealedAt while a new OPEN member seals in the same group call.
func TestSealGroupAlreadySealedIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	a := completeBatch(ctx, t, s, 2)
	first, err := s.SealBatch(ctx, a.ID)
	if err != nil {
		t.Fatalf("pre-seal: %v", err)
	}
	b := completeBatch(ctx, t, s, 1)

	snaps, _, err := s.SealGroup(ctx, []string{a.ID, b.ID})
	if err != nil {
		t.Fatalf("seal group: %v", err)
	}
	byID := snapMap(snaps)
	if !byID[a.ID].SealedAt.Equal(*first.SealedAt) {
		t.Fatalf("existing sealedAt overwritten: %v want %v",
			byID[a.ID].SealedAt, first.SealedAt)
	}
	if byID[b.ID].Status != store.StatusSealed || byID[b.ID].SealedAt == nil {
		t.Fatalf("new member not sealed: %+v", byID[b.ID])
	}

	// Whole-group retry returns the same stamps again.
	again, _, err := s.SealGroup(ctx, []string{b.ID, a.ID})
	if err != nil {
		t.Fatalf("retry group: %v", err)
	}
	againByID := snapMap(again)
	if !againByID[a.ID].SealedAt.Equal(*first.SealedAt) {
		t.Fatalf("retry changed a sealedAt: %v", againByID[a.ID].SealedAt)
	}
	if !againByID[b.ID].SealedAt.Equal(*byID[b.ID].SealedAt) {
		t.Fatalf("retry changed b sealedAt: %v vs %v",
			againByID[b.ID].SealedAt, byID[b.ID].SealedAt)
	}
}

// TestSealGroupOneIncompleteChangesNothing: A complete, B missing a chunk ->
// 409 semantics (ErrIncomplete listing B only), and both rows are exactly as
// before the call.
func TestSealGroupOneIncompleteChangesNothing(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	a := completeBatch(ctx, t, s, 2)
	b := mustBatch(ctx, t, s, 3)
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("x")); err != nil {
		t.Fatal(err)
	}

	_, incomplete, err := s.SealGroup(ctx, []string{a.ID, b.ID})
	if !errors.Is(err, store.ErrIncomplete) {
		t.Fatalf("want ErrIncomplete, got %v", err)
	}
	if len(incomplete) != 1 || incomplete[0].ID != b.ID {
		t.Fatalf("incomplete list wrong: %v", incomplete)
	}
	if incomplete[0].ExpectedChunks != 3 || incomplete[0].Received != 1 ||
		len(incomplete[0].Gaps) != 2 || incomplete[0].Gaps[0] != 2 || incomplete[0].Gaps[1] != 3 {
		t.Fatalf("incomplete snapshot wrong: %+v", incomplete[0])
	}

	// Both members keep their pre-call state: A stays OPEN (not co-sealed),
	// B stays OPEN with its one chunk.
	snapA, err := s.Snapshot(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapA.Status != store.StatusOpen || snapA.Received != 2 {
		t.Fatalf("complete member must remain OPEN on failed group: %+v", snapA)
	}
	snapB, err := s.Snapshot(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapB.Status != store.StatusOpen || snapB.Received != 1 ||
		len(snapB.Gaps) != 2 || snapB.Gaps[0] != 2 || snapB.Gaps[1] != 3 {
		t.Fatalf("incomplete member changed: %+v", snapB)
	}

	// A is still independently sealable afterwards.
	sealedA, err := s.SealBatch(ctx, a.ID)
	if err != nil || sealedA.Status != store.StatusSealed {
		t.Fatalf("single seal after failed group: %v %+v", err, sealedA)
	}
}

// TestSealGroupMultipleIncompleteListsAllInRequestOrder.
func TestSealGroupMultipleIncompleteListsAllInRequestOrder(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	a := mustBatch(ctx, t, s, 2) // gaps [1,2]
	b := completeBatch(ctx, t, s, 1)
	c := mustBatch(ctx, t, s, 2)
	if _, err := s.SubmitChunk(ctx, c.ID, 1, []byte("x")); err != nil {
		t.Fatal(err)
	} // c gaps [2]

	_, incomplete, err := s.SealGroup(ctx, []string{a.ID, b.ID, c.ID})
	if !errors.Is(err, store.ErrIncomplete) {
		t.Fatalf("want ErrIncomplete, got %v", err)
	}
	if len(incomplete) != 2 || incomplete[0].ID != a.ID || incomplete[1].ID != c.ID {
		t.Fatalf("incomplete members must be listed in request order: %v", incomplete)
	}
	if len(incomplete[0].Gaps) != 2 || incomplete[1].Gaps[0] != 2 {
		t.Fatalf("gaps wrong: %+v %+v", incomplete[0], incomplete[1])
	}
}

// TestSealGroupUnknownBatchNotFoundNoSideEffect: an unknown ID yields
// ErrNotFound and the known members are untouched.
func TestSealGroupUnknownBatchNotFoundNoSideEffect(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	a := completeBatch(ctx, t, s, 1)
	unknown := "ffffffffffffffffffffffffffffffff"

	_, _, err := s.SealGroup(ctx, []string{a.ID, unknown})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	snap, err := s.Snapshot(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != store.StatusOpen {
		t.Fatalf("known member must stay OPEN on not-found group: %+v", snap)
	}

	// Unknown first, known second — same verdict.
	_, _, err = s.SealGroup(ctx, []string{unknown, a.ID})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// TestSealGroupThreeWayRace: [A,B] on one pool, [B,A] on another, and B's
// final chunk on the first pool fire together. Lock order is canonical
// (ascending ID) in every transaction, so this must never deadlock, and the
// only settled outcomes are: both sealed (group committed after the chunk)
// or no new seal at all (group rolled back; both remain OPEN).
func TestSealGroupThreeWayRace(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)
	other := newPeer()
	defer other.Close()

	const rounds = 40
	var gABErr, gBAErr error
	deadline := 60 * time.Second
	for i := 0; i < rounds; i++ {
		a := mustBatch(ctx, t, s, 1)
		b := mustBatch(ctx, t, s, 1)
		// A is already complete; B is waiting for its single chunk.

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			_, _, gABErr = s.SealGroup(ctx, []string{a.ID, b.ID})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _, gBAErr = other.SealGroup(ctx, []string{b.ID, a.ID})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = s.SubmitChunk(ctx, b.ID, 1, []byte("final"))
		}()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		close(start)
		select {
		case <-done:
		case <-time.After(deadline):
			t.Fatalf("round %d: three-way interlock deadlocked (timeout %s)", i, deadline)
		}

		snapA, errA := s.Snapshot(ctx, a.ID)
		snapB, errB := s.Snapshot(ctx, b.ID)
		if errA != nil || errB != nil {
			t.Fatalf("snapshots: %v %v", errA, errB)
		}
		// A never receives chunks concurrently: if it is SEALED it must be
		// complete. The atomicity requirement forbids one sealed / one open.
		switch {
		case snapA.Status == store.StatusSealed && snapB.Status == store.StatusSealed:
			if len(snapA.Gaps) != 0 || len(snapB.Gaps) != 0 {
				t.Fatalf("round %d: sealed group with gaps: %+v %+v", i, snapA, snapB)
			}
		case snapA.Status == store.StatusOpen && snapB.Status == store.StatusOpen:
			// The whole group lost to the missing chunk; no partial seal.
		default:
			t.Fatalf("round %d: partial sealing forbidden: A=%s B=%s",
				i, snapA.Status, snapB.Status)
		}
		// Group errors, when present, may only be INCOMPLETE.
		for _, e := range []error{gABErr, gBAErr} {
			if e != nil && !errors.Is(e, store.ErrIncomplete) {
				t.Fatalf("unexpected group error: %v", e)
			}
		}
	}
}

// TestSealGroupContendingReversedOrders fires many group seals over the same
// members, alternating request order, concurrently with chunk submits. The
// canonical ascending lock order must keep this free of deadlocks (SQLSTATE
// 40P01) and the settled group seals are always complete.
func TestSealGroupContendingReversedOrders(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)
	other := newPeer()
	defer other.Close()

	const members, rounds = 4, 30
	for r := 0; r < rounds; r++ {
		ids := make([]string, 0, members)
		for range members {
			ids = append(ids, mustBatch(ctx, t, s, 1).ID)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		// One chunk submitter per member.
		for _, id := range ids {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				<-start
				_, _ = other.SubmitChunk(ctx, id, 1, []byte("x"))
			}(id)
		}
		// Several group sealers on both pools with both request orders.
		for i := 0; i < 6; i++ {
			wg.Add(1)
			order := append([]string(nil), ids...)
			if i%2 == 1 {
				sort.Strings(order)
			} else {
				sort.Sort(sort.Reverse(sort.StringSlice(order)))
			}
			st := s
			if i%2 == 0 {
				st = other
			}
			go func(st *store.Store, order []string) {
				defer wg.Done()
				<-start
				_, _, _ = st.SealGroup(ctx, order)
			}(st, order)
		}

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		close(start)
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Fatalf("round %d: contending reverse-order groups deadlocked", r)
		}

		// Settle: all chunks are present, so a final group seal in either
		// order seals every member with no gaps.
		rev := append([]string(nil), ids...)
		sort.Sort(sort.Reverse(sort.StringSlice(rev)))
		snaps, _, err := s.SealGroup(ctx, rev)
		if err != nil {
			t.Fatalf("round %d: settling group: %v", r, err)
		}
		for _, snap := range snaps {
			if snap.Status != store.StatusSealed || len(snap.Gaps) != 0 || snap.SealedAt == nil {
				t.Fatalf("round %d: member %s inconsistent: %+v", r, snap.ID, snap)
			}
		}
	}
}

// TestSealGroupRacesSingleSeal: group seal [A,B] against single SealBatch(A)
// and the final chunk of B. Canonical lock order must keep this deadlock
// free, and sealedAt for A is whoever wins — never overwritten.
func TestSealGroupRacesSingleSeal(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)
	other := newPeer()
	defer other.Close()

	const rounds = 40
	for i := 0; i < rounds; i++ {
		a := mustBatch(ctx, t, s, 1)
		b := mustBatch(ctx, t, s, 1)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		var singleErr, groupErr error
		go func() {
			defer wg.Done()
			<-start
			_, singleErr = s.SealBatch(ctx, a.ID)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _, groupErr = other.SealGroup(ctx, []string{a.ID, b.ID})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = s.SubmitChunk(ctx, b.ID, 1, []byte("z"))
		}()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		close(start)
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Fatalf("round %d: deadlock against single seal", i)
		}

		snapA, _ := s.Snapshot(ctx, a.ID)
		snapB, _ := s.Snapshot(ctx, b.ID)
		if snapA.Status == store.StatusSealed && snapB.Status == store.StatusOpen {
			// Single seal committed first; group must then have seen B
			// incomplete and rolled back.
			if !errors.Is(groupErr, store.ErrIncomplete) {
				t.Fatalf("round %d: group should be INCOMPLETE, got %v", i, groupErr)
			}
		} else if snapA.Status == store.StatusOpen && snapB.Status == store.StatusOpen {
			if !errors.Is(singleErr, store.ErrIncomplete) {
				t.Fatalf("round %d: single seal error %v", i, singleErr)
			}
		} else if snapA.Status == store.StatusSealed && snapB.Status == store.StatusSealed {
			// Both consistent.
		} else {
			t.Fatalf("round %d: impossible combo A=%s B=%s", i, snapA.Status, snapB.Status)
		}
	}
}
