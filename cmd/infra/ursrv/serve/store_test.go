// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package serve

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/ur/contract"
)

var testBase = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func newTestStore(t *testing.T) *reportStore {
	t.Helper()
	dir := t.TempDir()
	return newReportStore(filepath.Join(dir, "reports.state.gz"), filepath.Join(dir, "reports.jsons.gz"), nil)
}

func applyBytes(t *testing.T, s *reportStore, bs []byte) *BatchResult {
	t.Helper()
	b, err := parseBatchBytes(bs)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	res, err := s.ApplyBatch(b)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return res
}

func expectApplyErr(t *testing.T, s *reportStore, bs []byte, want error) {
	t.Helper()
	b, perr := parseBatchBytes(bs)
	if perr != nil {
		if !errorIs(perr, want) {
			t.Fatalf("parse: want %v, got %v", want, perr)
		}
		return
	}
	if _, err := s.ApplyBatch(b); !errorIs(err, want) {
		t.Fatalf("apply: want %v, got %v", want, err)
	}
}

// activeIDs returns the visible installation IDs via the same path metrics
// use; it never prunes.
func activeIDs(t *testing.T, s *reportStore) map[string]struct{} {
	t.Helper()
	got := make(map[string]struct{})
	s.CollectActive(testBase.Add(24*time.Hour), 1000*24*time.Hour, func(r *contract.Report) {
		got[r.UniqueID] = struct{}{}
	})
	return got
}

func exportBytes(t *testing.T, s *reportStore) []byte {
	t.Helper()
	if err := s.WriteExport(); err != nil {
		t.Fatal(err)
	}
	bs, err := os.ReadFile(s.exportPath)
	if err != nil {
		t.Fatal(err)
	}
	return bs
}

func exportIDs(t *testing.T, s *reportStore) map[string]struct{} {
	t.Helper()
	gr, err := gzip.NewReader(bytes.NewReader(exportBytes(t, s)))
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(gr)
	got := make(map[string]struct{})
	for {
		var rep contract.Report
		if err := dec.Decode(&rep); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		got[rep.UniqueID] = struct{}{}
	}
	return got
}

func has(ids map[string]struct{}, uid string) bool {
	_, ok := ids[uid]
	return ok
}

// A newer observation wins; an older record never rolls a newer
// observation back; installations absent from a snapshot are untouched.
func TestReconcileNewerWins(t *testing.T) {
	s := newTestStore(t)

	if s.AddLive(testReport("a", testBase.Add(-2*time.Hour))) != liveAccepted {
		t.Fatal("a should be accepted")
	}
	if s.AddLive(testReport("b", testBase.Add(-100*time.Minute))) != liveAccepted {
		t.Fatal("b should be accepted")
	}

	// Regional snapshot v1: carries an older record for "a" (must be
	// ignored) and a previously unseen "c".
	bs := mustBatch(t, "eu", 1, testBase, []*contract.Report{
		testReport("a", testBase.Add(-24*time.Hour)),
		testReport("c", testBase.Add(-30*time.Minute)),
	}, nil)
	res := applyBytes(t, s, bs)
	if res.AcceptedRecords != 1 || res.IgnoredRecords != 1 {
		t.Fatalf("accepted=%d ignored=%d, want 1/1", res.AcceptedRecords, res.IgnoredRecords)
	}

	ids := activeIDs(t, s)
	for _, uid := range []string{"a", "b", "c"} {
		if !has(ids, uid) {
			t.Errorf("%q missing after batch", uid)
		}
	}
	if rep, _ := s.Lookup("a"); rep.Received != testBase.Add(-2*time.Hour) {
		t.Errorf("live report for a rolled back: %v", rep.Received)
	}
}

// The source-version fence stops a lower-version source from overwriting
// data already known at a higher version; later realtime reports survive
// replays; a genuinely newer package version with newer data wins.
func TestVersionFenceAndReplay(t *testing.T) {
	s := newTestStore(t)

	v5 := mustBatch(t, "eu", 5, testBase, []*contract.Report{
		testReport("x", testBase.Add(-3*time.Hour)),
		testReport("y", testBase.Add(-3*time.Hour)),
	}, nil)
	applyBytes(t, s, v5)

	// Realtime report after the snapshot.
	live := testReport("x", testBase.Add(-time.Minute))
	if s.AddLive(live) != liveReplaced {
		t.Fatal("x should be replaced by live")
	}

	// Another region on an older version must not overwrite x, even though
	// its package is new to us.
	v1us := mustBatch(t, "us", 1, testBase, []*contract.Report{
		testReport("x", testBase.Add(-2*time.Hour)),
	}, nil)
	res := applyBytes(t, s, v1us)
	if res.AcceptedRecords != 0 || res.IgnoredRecords != 1 {
		t.Fatalf("fence: accepted=%d ignored=%d", res.AcceptedRecords, res.IgnoredRecords)
	}
	if rep, _ := s.Lookup("x"); !rep.Received.Equal(live.Received) {
		t.Errorf("x rolled back by lower-version source: %v", rep.Received)
	}

	// Replaying the eu v5 package is a no-op and cannot roll x back.
	res = applyBytes(t, s, v5)
	if !res.Replayed {
		t.Fatal("same version should replay as no-op")
	}
	if rep, _ := s.Lookup("x"); !rep.Received.Equal(live.Received) {
		t.Errorf("x rolled back by replayed snapshot: %v", rep.Received)
	}

	// A newer package with a newer observation is accepted; a record older
	// than what we already hold (y at -3h) is still ignored.
	v6 := mustBatch(t, "eu", 6, testBase.Add(time.Hour), []*contract.Report{
		testReport("x", testBase.Add(30*time.Minute)),
		testReport("y", testBase.Add(-4*time.Hour)),
	}, nil)
	res = applyBytes(t, s, v6)
	if res.AcceptedRecords != 1 || res.IgnoredRecords != 1 {
		t.Fatalf("v6 accepted=%d ignored=%d", res.AcceptedRecords, res.IgnoredRecords)
	}
	if rep, _ := s.Lookup("x"); rep.Received != testBase.Add(30*time.Minute) {
		t.Errorf("x not updated by newer package: %v", rep.Received)
	}
}

// Realtime reports that arrive while a snapshot package is repeatedly
// re-sent are never rolled back, including after a restart.
func TestLiveSurvivesRepeatedReplays(t *testing.T) {
	s := newTestStore(t)
	v1 := mustBatch(t, "eu", 1, testBase, []*contract.Report{
		testReport("p", testBase.Add(-10*time.Hour)),
	}, nil)
	applyBytes(t, s, v1)

	live := testReport("p", testBase.Add(-time.Minute))
	s.AddLive(live)

	for range 5 {
		res := applyBytes(t, s, v1)
		if !res.Replayed {
			t.Fatal("replay must be a no-op")
		}
	}
	rep, ok := s.Lookup("p")
	if !ok || !rep.Received.Equal(live.Received) {
		t.Fatalf("live p lost: ok=%v", ok)
	}

	if err := s.Persist(); err != nil {
		t.Fatal(err)
	}
	restarted := newReportStore(s.statePath, s.exportPath, nil)
	fd, err := os.Open(s.statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.LoadState(fd); err != nil {
		t.Fatal(err)
	}
	fd.Close()

	applyBytes(t, restarted, v1)
	rep, ok = restarted.Lookup("p")
	if !ok || !rep.Received.Equal(live.Received) {
		t.Fatalf("live p lost after restart+replay: ok=%v", ok)
	}
}

// Explicit disable hides an installation everywhere; only a strictly
// higher-generation explicit enable brings it back, and reports alone
// never do.
func TestDisableEnableGenerations(t *testing.T) {
	s := newTestStore(t)
	s.AddLive(testReport("a", testBase.Add(-time.Hour)))
	s.AddLive(testReport("b", testBase.Add(-time.Hour)))

	// Disable a at generation 1.
	v1 := mustBatch(t, "eu", 1, testBase, []*contract.Report{
		testReport("b", testBase.Add(-30*time.Minute)),
	}, []InstallStateMarker{{UniqueID: "a", Enabled: false, Generation: 1}})
	res := applyBytes(t, s, v1)
	if res.Disabled != 1 {
		t.Fatalf("disabled=%d", res.Disabled)
	}
	if s.IsDisabled("a") != true {
		t.Fatal("a should be disabled")
	}
	if ids := exportIDs(t, s); has(ids, "a") {
		t.Error("a must be absent from export")
	}
	if ids := activeIDs(t, s); has(ids, "a") {
		t.Error("a must be absent from metrics")
	}

	// Realtime and batch reports cannot resurrect it.
	if s.AddLive(testReport("a", testBase)) != liveIgnoredDisabled {
		t.Error("live report for disabled install must be ignored")
	}
	v2 := mustBatch(t, "eu", 2, testBase.Add(time.Hour), []*contract.Report{
		testReport("a", testBase.Add(time.Hour)),
	}, nil)
	res = applyBytes(t, s, v2)
	if res.IgnoredRecords != 1 || !s.IsDisabled("a") {
		t.Fatalf("record resurrected disabled install: %+v", res)
	}

	// An equal/lower generation marker changes nothing.
	v3 := mustBatch(t, "eu", 3, testBase.Add(2*time.Hour), nil, []InstallStateMarker{
		{UniqueID: "a", Enabled: true, Generation: 1},
	})
	applyBytes(t, s, v3)
	if !s.IsDisabled("a") {
		t.Fatal("stale enable marker must be ignored")
	}

	// Same-batch enable+fresh record at higher generation restores it.
	v4 := mustBatch(t, "eu", 4, testBase.Add(3*time.Hour), []*contract.Report{
		testReport("a", testBase.Add(3*time.Hour)),
	}, []InstallStateMarker{{UniqueID: "a", Enabled: true, Generation: 2}})
	res = applyBytes(t, s, v4)
	if res.Enabled != 1 || res.AcceptedRecords != 1 {
		t.Fatalf("enable: %+v", res)
	}
	if s.IsDisabled("a") {
		t.Fatal("a should be enabled again")
	}
	if ids := exportIDs(t, s); !has(ids, "a") {
		t.Error("a should be back in export")
	}

	// A later disable beats a lower-gen re-enable.
	v5 := mustBatch(t, "eu", 5, testBase.Add(4*time.Hour), nil, []InstallStateMarker{
		{UniqueID: "a", Enabled: false, Generation: 5},
	})
	applyBytes(t, s, v5)
	v6 := mustBatch(t, "eu", 6, testBase.Add(5*time.Hour), nil, []InstallStateMarker{
		{UniqueID: "a", Enabled: true, Generation: 4},
	})
	applyBytes(t, s, v6)
	if !s.IsDisabled("a") {
		t.Fatal("lower-generation enable must not restore")
	}
}

// A disable marker for a never-seen installation still persists as a
// tombstone, and a record in the same batch cannot slip past it.
func TestTombstoneForUnknownInstall(t *testing.T) {
	s := newTestStore(t)
	v1 := mustBatch(t, "eu", 1, testBase, []*contract.Report{
		testReport("z", testBase.Add(-time.Minute)),
	}, []InstallStateMarker{{UniqueID: "z", Enabled: false, Generation: 1}})
	res := applyBytes(t, s, v1)
	if res.Disabled != 1 || res.IgnoredRecords != 1 {
		t.Fatalf("res=%+v", res)
	}
	if !s.IsDisabled("z") {
		t.Fatal("z should be tombstoned")
	}
	if ids := activeIDs(t, s); has(ids, "z") {
		t.Fatal("z must never have been visible")
	}
}

// A stale (lower source version) batch and a write failure leave the store
// exactly as it was.
func TestStaleAndWriteFailureAreAtomic(t *testing.T) {
	s := newTestStore(t)
	s.AddLive(testReport("a", testBase))
	v1 := mustBatch(t, "eu", 1, testBase, []*contract.Report{
		testReport("b", testBase),
	}, nil)
	applyBytes(t, s, v1)

	// Version zero is directly rejected.
	stale := &parsedBatch{
		batchManifest: batchManifest{
			Format: batchFormat, Source: "eu", Version: 0, Created: testBase,
			Records: batchRecordsName,
		},
	}
	if _, err := s.ApplyBatch(stale); !errors.Is(err, ErrStaleBatch) {
		t.Fatalf("want stale, got %v", err)
	}
	if ids := activeIDs(t, s); !has(ids, "a") || !has(ids, "b") {
		t.Fatalf("state changed by stale batch: %v", ids)
	}

	// Make persistence fail: state path in a nonexistent directory.
	bad := newReportStore(filepath.Join(t.TempDir(), "missing", "state.gz"), filepath.Join(t.TempDir(), "export.gz"), nil)
	bad.AddLive(testReport("a", testBase))
	v1bad := mustBatch(t, "eu", 1, testBase, []*contract.Report{
		testReport("b", testBase),
	}, nil)
	b, err := parseBatchBytes(v1bad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.ApplyBatch(b); err == nil {
		t.Fatal("write failure must error")
	}
	if ids := activeIDs(t, bad); has(ids, "b") || !has(ids, "a") {
		t.Fatalf("batch partially applied despite write failure: %v", ids)
	}
}

// Corrupt/conflicting archives never reach the store.
func TestRejectedArchiveNoEffect(t *testing.T) {
	s := newTestStore(t)
	s.AddLive(testReport("a", testBase))
	created := testBase
	good := mustBatch(t, "eu", 1, created, []*contract.Report{testReport("b", created)}, nil)
	tampered := rewriteZip(t, mustZipReader(t, good), batchManifestName, func(m []byte) []byte {
		return append(m, []byte(" garbage")...)
	}, true)
	expectApplyErr(t, s, tampered, ErrCorruptArchive)
	expectApplyErr(t, s, []byte("not zip"), ErrCorruptArchive)

	if ids := activeIDs(t, s); len(ids) != 1 || !has(ids, "a") {
		t.Fatalf("state changed: %v", ids)
	}
}

// Feeding the same inputs in different orders converges to the identical
// report set and export bytes, before and after restart.
func TestConvergenceAcrossOrdersAndRestart(t *testing.T) {
	build := func() (*reportStore, [][]byte) {
		s := newTestStore(t)
		s.AddLive(testReport("live", testBase.Add(-2*time.Hour)))

		eu1 := mustBatch(t, "eu", 1, testBase, []*contract.Report{
			testReport("shared", testBase.Add(-3*time.Hour)),
			testReport("only-eu", testBase.Add(-90*time.Minute)),
		}, nil)
		us2 := mustBatch(t, "us", 2, testBase, []*contract.Report{
			// newer observation for shared
			testReport("shared", testBase.Add(-15*time.Minute)),
			testReport("only-us", testBase.Add(-45*time.Minute)),
		}, nil)
		eu3 := mustBatch(t, "eu", 3, testBase.Add(time.Hour), nil, []InstallStateMarker{
			{UniqueID: "gone", Enabled: false, Generation: 1},
		})
		gone := testReport("gone", testBase.Add(-30*time.Minute))
		s.AddLive(gone)
		return s, [][]byte{eu1, us2, eu3}
	}

	s1, batches1 := build()
	s2, batches2 := build()

	// Two valid interleavings (each source's versions stay in order).
	for _, bs := range batches1 {
		applyBytes(t, s1, bs)
	}
	for _, bs := range [][]byte{batches2[1], batches2[0], batches2[2]} {
		applyBytes(t, s2, bs)
	}

	// An out-of-order stale package from a source is rejected wholesale.
	st3 := newTestStore(t)
	pb3, err := parseBatchBytes(batches1[2])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st3.ApplyBatch(pb3); err != nil {
		t.Fatal(err)
	}
	pb1, err := parseBatchBytes(batches1[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st3.ApplyBatch(pb1); !errors.Is(err, ErrStaleBatch) {
		t.Fatalf("out-of-order old version: want stale, got %v", err)
	}

	want := map[string]struct{}{}
	for _, uid := range []string{"live", "shared", "only-eu", "only-us"} {
		want[uid] = struct{}{}
	}
	if ids := activeIDs(t, s1); !sameSet(ids, want) {
		t.Fatalf("s1 = %v", ids)
	}
	if ids := activeIDs(t, s2); !sameSet(ids, want) {
		t.Fatalf("s2 = %v", ids)
	}
	if !s1.IsDisabled("gone") || !s2.IsDisabled("gone") {
		t.Fatal("tombstone missing")
	}
	if rep1, _ := s1.Lookup("shared"); rep1.Received != testBase.Add(-15*time.Minute) {
		t.Fatalf("s1 shared: %v", rep1.Received)
	}
	if rep2, _ := s2.Lookup("shared"); rep2.Received != testBase.Add(-15*time.Minute) {
		t.Fatalf("s2 shared: %v", rep2.Received)
	}

	b1 := exportBytes(t, s1)
	b2 := exportBytes(t, s2)
	if !bytes.Equal(b1, b2) {
		t.Fatal("export differs between apply orders")
	}

	// Restart both from their persisted state and replay everything; the
	// set and bytes must remain identical.
	if err := s1.Persist(); err != nil {
		t.Fatal(err)
	}
	r1 := newReportStore(s1.statePath, s1.exportPath, nil)
	fd, err := os.Open(s1.statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := r1.LoadState(fd); err != nil {
		t.Fatal(err)
	}
	fd.Close()
	for _, bs := range batches1 {
		applyBytes(t, r1, bs)
	}
	if b := exportBytes(t, r1); !bytes.Equal(b, b1) {
		t.Fatal("export changed after restart and full replay")
	}
	if ids := activeIDs(t, r1); !sameSet(ids, want) {
		t.Fatalf("r1 = %v", ids)
	}
}

// Legacy gzip JSON-lines dumps bootstrap the store and upgrade cleanly.
func TestLegacyBootstrap(t *testing.T) {
	dir := t.TempDir()
	exportPath := filepath.Join(dir, "reports.jsons.gz")

	fd, err := os.Create(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	gw := gzip.NewWriter(fd)
	enc := json.NewEncoder(gw)
	for _, uid := range []string{"old1", "old2"} {
		if err := enc.Encode(testReport(uid, testBase.Add(-time.Hour))); err != nil {
			t.Fatal(err)
		}
	}
	gw.Close()
	fd.Close()

	s := newReportStore(statePathFor(exportPath), exportPath, nil)
	rfd, err := os.Open(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.LoadLegacy(rfd)
	rfd.Close()
	if err != nil || n != 2 {
		t.Fatalf("load legacy: n=%d err=%v", n, err)
	}
	if ids := activeIDs(t, s); !has(ids, "old1") || !has(ids, "old2") {
		t.Fatalf("legacy reports missing: %v", ids)
	}

	// Upgraded state persists and round-trips.
	if err := s.Persist(); err != nil {
		t.Fatal(err)
	}
	r2 := newReportStore(s.statePath, s.exportPath, nil)
	sfd, err := os.Open(s.statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.LoadState(sfd); err != nil {
		t.Fatal(err)
	}
	sfd.Close()
	if ids := activeIDs(t, r2); !has(ids, "old1") || !has(ids, "old2") {
		t.Fatalf("round trip lost reports: %v", ids)
	}
}

// Old visible reports are pruned; tombstones survive pruning.
func TestCollectActiveRetention(t *testing.T) {
	s := newTestStore(t)
	s.AddLive(testReport("fresh", testBase.Add(-time.Minute)))
	s.AddLive(testReport("stale", testBase.Add(-48*time.Hour)))
	applyBytes(t, s, mustBatch(t, "eu", 1, testBase.Add(-48*time.Hour), nil, []InstallStateMarker{
		{UniqueID: "dead", Enabled: false, Generation: 1},
	}))

	var seen []string
	s.CollectActive(testBase, 24*time.Hour, func(r *contract.Report) { seen = append(seen, r.UniqueID) })
	if len(seen) != 1 || seen[0] != "fresh" {
		t.Fatalf("seen=%v", seen)
	}
	if _, ok := s.Lookup("stale"); ok {
		t.Fatal("stale report should be pruned")
	}
	if !s.IsDisabled("dead") {
		t.Fatal("tombstone must survive pruning")
	}
}

// The HTTP import endpoint applies valid batches, replays idempotently,
// rejects stale versions with 409 and junk with 400.
func TestImportHandler(t *testing.T) {
	s := newTestStore(t)
	srv := &server{store: s}

	v1 := mustBatch(t, "eu", 1, testBase, []*contract.Report{
		testReport("a", testBase.Add(-time.Minute)),
	}, nil)
	v2 := mustBatch(t, "eu", 2, testBase.Add(time.Hour), []*contract.Report{
		testReport("a", testBase.Add(time.Hour)),
	}, nil)

	post := func(body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/import", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		srv.handleImport(rec, req)
		return rec
	}

	if rec := post(v2); rec.Code != http.StatusOK {
		t.Fatalf("apply v2: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(v1); rec.Code != http.StatusConflict {
		t.Fatalf("older version: want 409, got %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(v2); rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`"Replayed":true`)) {
		t.Fatalf("replay: want 200 replayed, got %d %s", rec.Code, rec.Body.String())
	}
	if rec := post([]byte("garbage")); rec.Code != http.StatusBadRequest {
		t.Fatalf("junk: want 400, got %d", rec.Code)
	}
	if rec := post(mustBatch(t, "us", 1, testBase, nil, nil)); rec.Code != http.StatusOK {
		t.Fatalf("other source: %d %s", rec.Code, rec.Body.String())
	}

	if ids := activeIDs(t, s); !has(ids, "a") {
		t.Fatalf("a missing: %v", ids)
	}
}

func sameSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}
