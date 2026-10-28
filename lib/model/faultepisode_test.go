// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/protocol"
)

func faultObs(source, path, errMsg string) faultObservation {
	return faultObservation{source: source, path: path, errMsg: errMsg}
}

func newTestFaultStore(kv db.KV, folder string) *faultEpisodeStore {
	return newFaultEpisodeRegistry(kv, events.NoopLogger).store(folder)
}

func activeEpisodes(t *testing.T, s *faultEpisodeStore) []FaultEpisode {
	t.Helper()
	eps, err := s.list(FaultEpisodeFilter{})
	if err != nil {
		t.Fatal(err)
	}
	return eps
}

func allEpisodes(t *testing.T, s *faultEpisodeStore) []FaultEpisode {
	t.Helper()
	eps, err := s.list(FaultEpisodeFilter{IncludeAcknowledged: true, IncludeResolved: true})
	if err != nil {
		t.Fatal(err)
	}
	return eps
}

// Repeated sightings of the same path with the same error while the
// episode is open must not pile up duplicates.
func TestFaultEpisodeDedup(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	if err := s.apply([]faultObservation{
		faultObs(faultSourceScan, "a", "boom"),
		faultObs(faultSourceScan, "b", "boom"),
	}, faultResolution{}); err != nil {
		t.Fatal(err)
	}
	if err := s.apply([]faultObservation{
		faultObs(faultSourceScan, "a", "boom"),
	}, faultResolution{}); err != nil {
		t.Fatal(err)
	}

	eps := allEpisodes(t, s)
	if len(eps) != 2 {
		t.Fatalf("expected 2 episodes, got %d", len(eps))
	}
	var a *FaultEpisode
	for i := range eps {
		if eps[i].Path == "a" {
			a = &eps[i]
		}
	}
	if a.Occurrences != 2 {
		t.Errorf("expected 2 occurrences, got %d", a.Occurrences)
	}
}

// The same path with a different kind of error is a different episode.
func TestFaultEpisodeDifferentErrorIsSeparate(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))
	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "different")}, faultResolution{}))

	if eps := allEpisodes(t, s); len(eps) != 2 {
		t.Fatalf("expected 2 episodes, got %d", len(eps))
	}
}

// Scan and pull faults on the same path are independent.
func TestFaultEpisodeSourceIsPartOfIdentity(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))
	must(t, s.apply([]faultObservation{faultObs(faultSourcePull, "a", "boom")}, faultResolution{}))

	if eps := allEpisodes(t, s); len(eps) != 2 {
		t.Fatalf("expected 2 episodes, got %d", len(eps))
	}
}

// A failed scan records the faults but closes nothing, because there is no
// recovery evidence.
func TestFaultEpisodeFailedScanKeepsOpen(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))
	// Another failed scan that reobserves the same fault.
	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))

	if n, _ := s.activeCount(); n != 1 {
		t.Fatalf("expected 1 active, got %d", n)
	}
}

// A successful full scan that does not reobserve the fault is batch
// validation and closes the episode.
func TestFaultEpisodeFullScanValidationCloses(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))
	must(t, s.apply(nil, faultResolution{scanValidated: true}))

	if n, _ := s.activeCount(); n != 0 {
		t.Fatalf("expected 0 active, got %d", n)
	}
	eps, err := s.list(FaultEpisodeFilter{IncludeResolved: true})
	must(t, err)
	if len(eps) != 1 || !eps[0].Resolved || eps[0].Resolution != faultResolutionValidated {
		t.Fatalf("expected one validated resolved episode, got %+v", eps)
	}
}

// A successful scan of a subtree only provides recovery evidence for
// episodes covered by that subtree.
func TestFaultEpisodeSubdirScanScope(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{
		faultObs(faultSourceScan, "a/x", "boom"),
		faultObs(faultSourceScan, "b/y", "boom"),
		faultObs(faultSourceScan, "a/z", "boom"),
	}, faultResolution{}))

	must(t, s.apply(nil, faultResolution{scanValidated: true, scanSubdirs: []string{"a"}}))

	eps := allEpisodes(t, s)
	resolved := map[string]bool{}
	for _, ep := range eps {
		resolved[ep.Path] = ep.Resolved
	}
	if !resolved["a/x"] || !resolved["a/z"] {
		t.Errorf("episodes under scanned subtree should be resolved: %v", resolved)
	}
	if resolved["b/y"] {
		t.Errorf("episode outside scanned subtree should stay open: %v", resolved)
	}
}

// A fault observed again in a run must never be resolved by that same
// run's validation evidence.
func TestFaultEpisodeObservedNotResolved(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	// Same apply both observes the fault and claims full validation.
	must(t, s.apply([]faultObservation{
		faultObs(faultSourcePull, "a", "boom"),
	}, faultResolution{pullValidated: true}))

	if n, _ := s.activeCount(); n != 1 {
		t.Fatalf("fault still observed must stay open, active=%d", n)
	}
}

// A zero-error pull is full validation for pull episodes.
func TestFaultEpisodePullValidationCloses(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{
		faultObs(faultSourcePull, "a", "syncing: boom"),
		faultObs(faultSourcePull, "b", "syncing: boom"),
	}, faultResolution{}))

	must(t, s.apply(nil, faultResolution{pullValidated: true}))

	if n, _ := s.activeCount(); n != 0 {
		t.Fatalf("expected 0 active, got %d", n)
	}
}

// When some items still fail, only items that were actually processed
// provide recovery evidence.
func TestFaultEpisodePullProcessedClosesOnlyProcessed(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{
		faultObs(faultSourcePull, "ok", "syncing: boom"),
		faultObs(faultSourcePull, "bad", "syncing: boom"),
	}, faultResolution{}))

	must(t, s.apply(nil, faultResolution{
		pullProcessed: map[string]struct{}{"ok": {}},
	}))

	eps := allEpisodes(t, s)
	state := map[string]bool{}
	for _, ep := range eps {
		state[ep.Path] = ep.Resolved
	}
	if !state["ok"] {
		t.Errorf("processed episode should be resolved")
	}
	if state["bad"] {
		t.Errorf("unconfirmed episode must stay open")
	}
}

// Acknowledged episodes disappear from the default listing but remain in
// history, and they are still closed when recovery is confirmed.
func TestFaultEpisodeAcknowledge(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{
		faultObs(faultSourceScan, "a", "boom"),
		faultObs(faultSourceScan, "b", "boom"),
	}, faultResolution{}))

	eps := activeEpisodes(t, s)
	if len(eps) != 2 {
		t.Fatalf("expected 2 active, got %d", len(eps))
	}
	must(t, s.acknowledge([]string{eps[0].ID}))

	if eps := activeEpisodes(t, s); len(eps) != 1 {
		t.Fatalf("expected 1 active after ack, got %d", len(eps))
	}
	eps, err := s.list(FaultEpisodeFilter{IncludeAcknowledged: true})
	must(t, err)
	if len(eps) != 2 {
		t.Fatalf("acknowledged episode should be queryable, got %d", len(eps))
	}
	if n, _ := s.activeCount(); n != 1 {
		t.Fatalf("active count should exclude acknowledged, got %d", n)
	}

	// Acknowledging without IDs acknowledges everything open.
	must(t, s.acknowledge(nil))
	if eps := activeEpisodes(t, s); len(eps) != 0 {
		t.Fatalf("expected no active after ack-all, got %d", len(eps))
	}

	// Confirmed recovery closes the acknowledged episode.
	must(t, s.apply(nil, faultResolution{scanValidated: true}))
	eps, err = s.list(FaultEpisodeFilter{IncludeResolved: true})
	must(t, err)
	if len(eps) != 2 {
		t.Fatalf("expected 2 resolved history entries, got %d", len(eps))
	}
	for _, ep := range eps {
		if !ep.Acknowledged || !ep.Resolved {
			t.Errorf("episode should be acknowledged and resolved: %+v", ep)
		}
	}
}

// After resolution, the same fault recurring starts a new episode rather
// than reviving the closed one.
func TestFaultEpisodeRecurrenceStartsNewEpisode(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))
	first := activeEpisodes(t, s)[0]

	must(t, s.apply(nil, faultResolution{scanValidated: true}))
	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))

	active := activeEpisodes(t, s)
	if len(active) != 1 || active[0].ID == first.ID || active[0].Occurrences != 1 {
		t.Fatalf("expected a new active episode, got %+v", active)
	}
	history, err := s.list(FaultEpisodeFilter{IncludeResolved: true})
	must(t, err)
	if len(history) != 2 {
		t.Fatalf("expected the closed episode to remain in history, got %d", len(history))
	}
}

// The ledger survives a "restart" (a new store over the same KV).
func TestFaultEpisodePersistsAcrossRestart(t *testing.T) {
	kv := newMemKV()

	s1 := newTestFaultStore(kv, "f")
	must(t, s1.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))
	first := activeEpisodes(t, s1)[0]
	must(t, s1.acknowledge([]string{first.ID}))

	s2 := newTestFaultStore(kv, "f")
	eps, err := s2.list(FaultEpisodeFilter{IncludeAcknowledged: true})
	must(t, err)
	if len(eps) != 1 || eps[0].ID != first.ID || !eps[0].Acknowledged {
		t.Fatalf("state did not survive reload: %+v", eps)
	}
	if n, _ := s2.activeCount(); n != 0 {
		t.Fatalf("acknowledged episode should not count active, got %d", n)
	}
}

// When the write fails the whole update fails and the previous state is
// left fully intact (no half record).
func TestFaultEpisodeWriteFailureIsAtomic(t *testing.T) {
	kv := newFailPutKV()
	s := newTestFaultStore(kv, "f")

	// First commit succeeds.
	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))

	kv.fail = true
	err := s.apply([]faultObservation{faultObs(faultSourceScan, "b", "boom")}, faultResolution{})
	if err == nil {
		t.Fatal("expected write failure")
	}
	kv.fail = false

	eps := allEpisodes(t, s)
	if len(eps) != 1 || eps[0].Path != "a" || eps[0].Occurrences != 1 {
		t.Fatalf("state must be unchanged after failed write, got %+v", eps)
	}

	// A later successful commit works normally.
	must(t, s.apply([]faultObservation{faultObs(faultSourceScan, "b", "boom")}, faultResolution{}))
	if eps := allEpisodes(t, s); len(eps) != 2 {
		t.Fatalf("expected 2 episodes, got %d", len(eps))
	}
}

// Resolved history is bounded.
func TestFaultEpisodeHistoryIsPruned(t *testing.T) {
	s := newTestFaultStore(newMemKV(), "f")

	for i := range maxResolvedFaultEpisodes + 5 {
		path := "p" + strconv.Itoa(i)
		must(t, s.apply([]faultObservation{faultObs(faultSourceScan, path, "boom")}, faultResolution{}))
		must(t, s.apply(nil, faultResolution{scanValidated: true}))
	}

	history, err := s.list(FaultEpisodeFilter{IncludeResolved: true})
	must(t, err)
	if len(history) != maxResolvedFaultEpisodes {
		t.Fatalf("expected %d retained resolved episodes, got %d", maxResolvedFaultEpisodes, len(history))
	}
}

func TestFaultEpisodeModelAPIAndEvent(t *testing.T) {
	w, fcfg := newDefaultCfgWrapper(t)
	m := newModel(t, w, myID, nil)
	t.Cleanup(m.evCancel)

	sub := m.evLogger.Subscribe(events.FaultEpisodes)
	defer sub.Unsubscribe()

	if _, err := m.FolderFaultEpisodes("does-not-exist", FaultEpisodeFilter{}); err == nil {
		t.Fatal("expected error for unknown folder")
	}
	if err := m.AcknowledgeFaultEpisodes("does-not-exist", nil); err == nil {
		t.Fatal("expected error for unknown folder")
	}

	store := m.faults.store(fcfg.ID)
	must(t, store.apply([]faultObservation{faultObs(faultSourceScan, "a", "boom")}, faultResolution{}))

	if n, err := m.ActiveFaultEpisodeCount(fcfg.ID); err != nil || n != 1 {
		t.Fatalf("active count: n=%d err=%v", n, err)
	}
	eps, err := m.FolderFaultEpisodes(fcfg.ID, FaultEpisodeFilter{})
	must(t, err)
	if len(eps) != 1 {
		t.Fatalf("expected one active episode, got %d", len(eps))
	}
	id := eps[0].ID

	must(t, m.AcknowledgeFaultEpisodes(fcfg.ID, []string{id}))
	if eps, err := m.FolderFaultEpisodes(fcfg.ID, FaultEpisodeFilter{}); err != nil || len(eps) != 0 {
		t.Fatalf("acknowledged episode hidden from default: %v %v", eps, err)
	}

	// Every change emits an event with the current counts.
	deadline := time.After(5 * time.Second)
	got := 0
	for got < 2 {
		select {
		case <-sub.C():
			got++
		case <-deadline:
			t.Fatalf("timed out waiting for fault events (got %d)", got)
		}
	}
}

// Episodes persist in the real database across model instances, i.e. a
// service restart does not lose them.
func TestFaultEpisodePersistsAcrossModelRestart(t *testing.T) {
	mdb, err := sqlite.Open(t.TempDir())
	must(t, err)
	t.Cleanup(func() { mdb.Close() })

	w1, fcfg := newDefaultCfgWrapper(t)
	ev1 := events.NewLogger()
	ctx1, cancel1 := context.WithCancel(t.Context())
	go ev1.Serve(ctx1)
	t.Cleanup(cancel1)
	m1 := NewModel(w1, myID, mdb, nil, ev1, protocol.NewKeyGenerator()).(*model)

	must(t, m1.faults.store(fcfg.ID).apply([]faultObservation{
		faultObs(faultSourceScan, "a", "boom"),
	}, faultResolution{}))

	// A brand new model over the same database, like after a restart.
	w2, _ := newDefaultCfgWrapper(t)
	ev2 := events.NewLogger()
	ctx2, cancel2 := context.WithCancel(t.Context())
	go ev2.Serve(ctx2)
	t.Cleanup(cancel2)
	m2 := NewModel(w2, myID, mdb, nil, ev2, protocol.NewKeyGenerator()).(*model)

	if n, err := m2.ActiveFaultEpisodeCount(fcfg.ID); err != nil || n != 1 {
		t.Fatalf("episode should survive restart: n=%d err=%v", n, err)
	}
	eps, err := m2.FolderFaultEpisodes(fcfg.ID, FaultEpisodeFilter{})
	must(t, err)
	if len(eps) != 1 || eps[0].Path != "a" || eps[0].Occurrences != 1 {
		t.Fatalf("unexpected episodes after restart: %+v", eps)
	}
}

// End-to-end wiring through a folder: the immediate in-memory error list
// keeps behaving as before while faults are recorded persistently.
func TestFaultEpisodeFolderWiring(t *testing.T) {
	w, fcfg := newDefaultCfgWrapper(t)
	m := newModel(t, w, myID, nil)
	t.Cleanup(m.evCancel)

	f := newFolder(m.model, ignore.New(fcfg.Filesystem()), fcfg, m.evLogger, m.folderIOLimiter, nil)

	// A scan produces a fault.
	f.clearScanErrors(nil)
	f.newScanError("foo", errors.New("permission denied"))
	if errs := f.Errors(); len(errs) != 1 || errs[0].Path != "foo" {
		t.Fatalf("immediate error list mismatch: %+v", errs)
	}
	// The scan did not finish successfully, so the episode is recorded
	// but stays open.
	must(t, f.commitScanFaults(nil, false))
	if n, _ := m.ActiveFaultEpisodeCount(fcfg.ID); n != 1 {
		t.Fatalf("expected 1 active, got %d", n)
	}

	// Another failed scan that keeps seeing the same fault dedups.
	f.clearScanErrors(nil)
	f.newScanError("foo", errors.New("permission denied"))
	if errs := f.Errors(); len(errs) != 1 {
		t.Fatalf("immediate list should contain one entry, got %+v", errs)
	}
	must(t, f.commitScanFaults(nil, false))
	eps := activeEpisodes(t, m.faults.store(fcfg.ID))
	if len(eps) != 1 || eps[0].Occurrences != 2 {
		t.Fatalf("expected one episode with 2 occurrences, got %+v", eps)
	}

	// A successful full scan resolves it; as in the existing behavior
	// the immediate list is cleared at the start of that scan.
	f.clearScanErrors(nil)
	if errs := f.Errors(); len(errs) != 0 {
		t.Fatalf("immediate list should be cleared for the new scan, got %+v", errs)
	}
	must(t, f.commitScanFaults(nil, true))
	if n, _ := m.ActiveFaultEpisodeCount(fcfg.ID); n != 0 {
		t.Fatalf("expected 0 active, got %d", n)
	}
}

// --- in-memory KV for tests ---

type memKV struct {
	mut  sync.RWMutex
	data map[string][]byte
}

func newMemKV() *memKV {
	return &memKV{data: make(map[string][]byte)}
}

func (k *memKV) GetKV(key string) ([]byte, error) {
	k.mut.RLock()
	defer k.mut.RUnlock()
	val, ok := k.data[key]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return append([]byte(nil), val...), nil
}

func (k *memKV) PutKV(key string, val []byte) error {
	k.mut.Lock()
	defer k.mut.Unlock()
	k.data[key] = append([]byte(nil), val...)
	return nil
}

func (k *memKV) DeleteKV(key string) error {
	k.mut.Lock()
	defer k.mut.Unlock()
	delete(k.data, key)
	return nil
}

func (k *memKV) PrefixKV(prefix string) (iter.Seq[db.KeyValue], func() error) {
	return func(yield func(db.KeyValue) bool) {
		k.mut.RLock()
		defer k.mut.RUnlock()
		for key, val := range k.data {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if !yield(db.KeyValue{Key: key, Value: append([]byte(nil), val...)}) {
				return
			}
		}
	}, func() error { return nil }
}

type failPutKV struct {
	*memKV
	fail bool
}

func newFailPutKV() *failPutKV {
	return &failPutKV{memKV: newMemKV()}
}

func (k *failPutKV) PutKV(key string, val []byte) error {
	if k.fail {
		return errors.New("injected write failure")
	}
	return k.memKV.PutKV(key, val)
}
