// Copyright © 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *relayStore {
	t.Helper()
	dir := t.TempDir()
	s, err := newRelayStore(
		filepath.Join(dir, "leases.json"),
		filepath.Join(dir, "published.json"),
		filepath.Join(dir, "known_relays"),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.stop)
	return s
}

func testRelay(id, host string) *relay {
	rawURL := "relay://" + host + "/?id=" + url.QueryEscape(id)
	uri, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return &relay{URL: rawURL, uri: uri}
}

func mustAdmit(t *testing.T, s *relayStore, id string, r *relay, times int) {
	t.Helper()
	for range times {
		if err := s.admit(id, r, time.Hour); err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
	}
}

func leaseOf(s *relayStore, id string) *lease {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leases[id]
}

func onDiskGeneration(t *testing.T, s *relayStore) (uint64, uint64) {
	t.Helper()
	ledger, err := loadLeasesFile(s.leasesPath)
	if err != nil {
		t.Fatal("read ledger:", err)
	}
	published, err := loadPublishedFile(s.publishedPath)
	if err != nil {
		t.Fatal("read published list:", err)
	}
	return ledger.Generation, published.Generation
}

// A brand new identity needs three consecutive successful reports before
// anything is published. Every report is its own committed generation and
// both files carry the same generation.
func TestLeasePendingThenPublish(t *testing.T) {
	s := newTestStore(t)
	r := testRelay("id1", "192.0.2.10:22067")

	for i := 1; i <= 2; i++ {
		if err := s.admit("id1", r, time.Hour); err != nil {
			t.Fatal(err)
		}
		_, dyn := s.snapshot()
		if len(dyn) != 0 {
			t.Fatalf("pending relay published after %d reports", i)
		}
		l := leaseOf(s, "id1")
		if l == nil || l.pendingOK != i || l.pendingURL != r.URL || l.activeURL != "" {
			t.Fatalf("bad pending state after %d reports: %+v", i, l)
		}
		if lg, pg := onDiskGeneration(t, s); lg != uint64(i) || pg != uint64(i) {
			t.Fatalf("disk generations ledger=%d list=%d, want %d", lg, pg, i)
		}
	}

	if err := s.admit("id1", r, time.Hour); err != nil {
		t.Fatal(err)
	}
	gen, dyn := s.snapshot()
	if gen != 3 || len(dyn) != 1 || dyn[0].URL != r.URL {
		t.Fatalf("unexpected published state gen=%d dyn=%v", gen, dyn)
	}
	l := leaseOf(s, "id1")
	if l.activeURL != r.URL || l.pendingURL != "" || l.pendingOK != 0 {
		t.Fatalf("bad active lease: %+v", l)
	}
	if lg, pg := onDiskGeneration(t, s); lg != 3 || pg != 3 {
		t.Fatalf("disk generations ledger=%d list=%d, want 3", lg, pg)
	}
}

// A candidate address does not replace the active one until the third
// consecutive success; failures and changing candidates reset the count.
func TestLeaseSwitchGating(t *testing.T) {
	s := newTestStore(t)
	old := testRelay("id1", "192.0.2.20:22067")
	mustAdmit(t, s, "id1", old, 3)

	new1 := testRelay("id1", "192.0.2.21:22067")
	mustAdmit(t, s, "id1", new1, 2)
	if _, dyn := s.snapshot(); len(dyn) != 1 || dyn[0].URL != old.URL {
		t.Fatalf("old entry must stay published while candidate is vetted, got %v", dyn)
	}
	if l := leaseOf(s, "id1"); l.pendingOK != 2 {
		t.Fatalf("pendingOK=%d, want 2", l.pendingOK)
	}

	// A failure resets the consecutive count, old entry survives.
	if err := s.reportFailure("id1", new1.URL); err != nil {
		t.Fatal(err)
	}
	if l := leaseOf(s, "id1"); l.pendingOK != 0 || l.activeURL != old.URL {
		t.Fatalf("failure did not reset pending state: %+v", l)
	}
	if _, dyn := s.snapshot(); len(dyn) != 1 || dyn[0].URL != old.URL {
		t.Fatalf("old entry changed after failed check, got %v", dyn)
	}

	// A failure for a URL that is not the current candidate is a no-op
	// (no generation change).
	genBefore, _ := s.snapshot()
	if err := s.reportFailure("id1", "relay://192.0.2.99:22067/?id=id1"); err != nil {
		t.Fatal(err)
	}
	if gen, _ := s.snapshot(); gen != genBefore {
		t.Fatalf("irrelevant failure bumped generation %d -> %d", genBefore, gen)
	}

	// One more success at count one, then a different candidate resets.
	if err := s.admit("id1", new1, time.Hour); err != nil {
		t.Fatal(err)
	}
	new2 := testRelay("id1", "192.0.2.22:22067")
	if err := s.admit("id1", new2, time.Hour); err != nil {
		t.Fatal(err)
	}
	if l := leaseOf(s, "id1"); l.pendingURL != new2.URL || l.pendingOK != 1 || l.activeURL != old.URL {
		t.Fatalf("candidate change did not reset state: %+v", l)
	}

	// Third consecutive success at the new candidate atomically switches.
	mustAdmit(t, s, "id1", new2, 2)
	if _, dyn := s.snapshot(); len(dyn) != 1 || dyn[0].URL != new2.URL {
		t.Fatalf("candidate not promoted: %v", dyn)
	}
	if l := leaseOf(s, "id1"); l.activeURL != new2.URL || l.pendingURL != "" {
		t.Fatalf("active URL not replaced: %+v", l)
	}
	if lg, pg := onDiskGeneration(t, s); lg != pg {
		t.Fatalf("disk generations disagree: ledger=%d list=%d", lg, pg)
	}
}

// A heartbeat at the active address extends the lease and drops any
// pending candidate.
func TestLeaseHeartbeat(t *testing.T) {
	s := newTestStore(t)
	r := testRelay("id1", "192.0.2.30:22067")
	mustAdmit(t, s, "id1", r, 3)
	first := leaseOf(s, "id1").expiresAt

	candidate := testRelay("id1", "192.0.2.31:22067")
	if err := s.admit("id1", candidate, time.Hour); err != nil {
		t.Fatal(err)
	}
	if l := leaseOf(s, "id1"); l.pendingURL != candidate.URL {
		t.Fatalf("candidate not pending: %+v", l)
	}

	time.Sleep(5 * time.Millisecond)
	if err := s.admit("id1", r, time.Hour); err != nil {
		t.Fatal(err)
	}
	l := leaseOf(s, "id1")
	if !l.expiresAt.After(first) {
		t.Fatalf("lease not extended: %v <= %v", l.expiresAt, first)
	}
	if l.pendingURL != "" || l.pendingOK != 0 || l.activeURL != r.URL {
		t.Fatalf("heartbeat did not clear candidate: %+v", l)
	}
}

// Failed health reports at the pending address extend nothing and never
// publish anything.
func TestLeaseFailureBeforePublish(t *testing.T) {
	s := newTestStore(t)
	r := testRelay("id1", "192.0.2.40:22067")
	if err := s.admit("id1", r, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := s.reportFailure("id1", r.URL); err != nil {
		t.Fatal(err)
	}
	l := leaseOf(s, "id1")
	if l.pendingOK != 0 || l.activeURL != "" {
		t.Fatalf("unexpected lease after failure: %+v", l)
	}
	if _, dyn := s.snapshot(); len(dyn) != 0 {
		t.Fatalf("relay must not be published on failure: %v", dyn)
	}
}

// If either state file fails to write, the whole admission is a no-op in
// both memory and disk.
func TestLeaseCommitFailureRollback(t *testing.T) {
	s := newTestStore(t)
	genBefore, dynBefore := s.snapshot()
	origPublished := s.publishedPath

	// Make the published-list rename fail by turning the target path into
	// an existing directory (rename over a directory fails).
	if err := os.Remove(s.publishedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(s.publishedPath, 0o755); err != nil {
		t.Fatal(err)
	}

	r := testRelay("id1", "192.0.2.50:22067")
	if err := s.admit("id1", r, time.Hour); err == nil {
		t.Fatal("admit should fail when the list cannot be written")
	}

	genAfter, dynAfter := s.snapshot()
	if genAfter != genBefore || len(dynAfter) != len(dynBefore) {
		t.Fatalf("in-memory state changed despite commit failure: gen %d -> %d, dyn %v -> %v", genBefore, genAfter, dynBefore, dynAfter)
	}
	if l := leaseOf(s, "id1"); l != nil {
		t.Fatalf("lease appeared in memory despite commit failure: %+v", l)
	}
	ledger, err := loadLeasesFile(s.leasesPath)
	if err != nil {
		t.Fatal("read ledger:", err)
	}
	if ledger.Generation != genBefore {
		t.Fatalf("ledger generation changed despite failure: %d, want %d", ledger.Generation, genBefore)
	}
	// The list file may be missing (it was removed to create the blocker
	// directory); it must never carry a newer generation than the ledger.
	if published, err := loadPublishedFile(origPublished); err == nil && published.Generation > genBefore {
		t.Fatalf("published list advanced despite ledger failure: %d, want <= %d", published.Generation, genBefore)
	}
}

// Restart: the ledger is authoritative. Active entries, pending counts and
// lease deadlines are restored; a list file that is ahead gets rebuilt to
// match the ledger.
func TestLeaseRestartReconcile(t *testing.T) {
	dir := t.TempDir()
	leasesPath := filepath.Join(dir, "leases.json")
	publishedPath := filepath.Join(dir, "published.json")

	s, err := newRelayStore(leasesPath, publishedPath, filepath.Join(dir, "known_relays"), nil)
	if err != nil {
		t.Fatal(err)
	}
	r := testRelay("id1", "192.0.2.60:22067")
	if err := s.admit("id1", r, time.Hour); err != nil {
		t.Fatal(err)
	}
	s.stop()

	// Pending state (including the count) survives a restart.
	s2, err := newRelayStore(leasesPath, publishedPath, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if l := leaseOf(s2, "id1"); l == nil || l.pendingOK != 1 || l.activeURL != "" {
		t.Fatalf("pending state not restored: %+v", l)
	}
	if gen, dyn := s2.snapshot(); gen != 1 || len(dyn) != 0 {
		t.Fatalf("unexpected state after restart: gen=%d dyn=%v", gen, dyn)
	}
	if err := s2.admit("id1", r, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s2.admit("id1", r, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, dyn := s2.snapshot(); len(dyn) != 1 || dyn[0].URL != r.URL {
		t.Fatalf("pending relay should publish with 2 post-restart reports, got %v", dyn)
	}
	s2.stop()

	// Simulate a crash that left the list one generation ahead of the
	// ledger: reconciliation must roll it back.
	ahead := publishedFile{
		Generation: 42,
		Relays:     []publishedEntry{{ID: "ghost", URL: "relay://192.0.2.66:22067/?id=ghost"}},
	}
	if err := writeJSONAtomic(publishedPath, ahead); err != nil {
		t.Fatal(err)
	}

	s3, err := newRelayStore(leasesPath, publishedPath, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s3.stop)
	gen, dyn := s3.snapshot()
	if gen != 3 || len(dyn) != 1 || dyn[0].URL != r.URL {
		t.Fatalf("reconciled state wrong: gen=%d dyn=%v", gen, dyn)
	}
	if lg, pg := onDiskGeneration(t, s3); lg != 3 || pg != 3 {
		t.Fatalf("disk not reconciled: ledger=%d list=%d", lg, pg)
	}
}

// Expired leases are dropped on load and removed by a committed eviction at
// runtime.
func TestLeaseExpiry(t *testing.T) {
	s := newTestStore(t)
	r := testRelay("id1", "192.0.2.70:22067")
	mustAdmit(t, s, "id1", r, 3)
	shortEviction := 30 * time.Millisecond
	if err := s.admit("id1", r, shortEviction); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * shortEviction)
	s.expire("id1")

	if l := leaseOf(s, "id1"); l != nil {
		t.Fatalf("lease still present after expiry: %+v", l)
	}
	if _, dyn := s.snapshot(); len(dyn) != 0 {
		t.Fatalf("expired relay still published: %v", dyn)
	}
	if lg, pg := onDiskGeneration(t, s); lg != pg {
		t.Fatalf("disk generations disagree after expiry: %d vs %d", lg, pg)
	}
}

// Expired entries are garbage collected at startup and bump the committed
// generation.
func TestLeaseExpiredDroppedOnLoad(t *testing.T) {
	dir := t.TempDir()
	leasesPath := filepath.Join(dir, "leases.json")
	publishedPath := filepath.Join(dir, "published.json")

	s, err := newRelayStore(leasesPath, publishedPath, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := testRelay("id1", "192.0.2.80:22067")
	if err := s.admit("id1", r, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	s.stop()
	time.Sleep(30 * time.Millisecond)

	s2, err := newRelayStore(leasesPath, publishedPath, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s2.stop)
	if l := leaseOf(s2, "id1"); l != nil {
		t.Fatalf("expired lease restored: %+v", l)
	}
	if gen, _ := s2.snapshot(); gen != 2 {
		t.Fatalf("generation not bumped after expiry cleanup, gen=%d", gen)
	}
}

// Explicit revocation is an atomic offline commit; revoking an unknown
// identity is reported as such.
func TestLeaseRevoke(t *testing.T) {
	s := newTestStore(t)
	r := testRelay("id1", "192.0.2.90:22067")
	mustAdmit(t, s, "id1", r, 3)

	if existed, err := s.revoke("unknown"); err != nil || existed {
		t.Fatalf("revoke unknown: existed=%v err=%v, want false/nil", existed, err)
	}
	existed, err := s.revoke("id1")
	if err != nil || !existed {
		t.Fatalf("revoke: existed=%v err=%v, want true/nil", existed, err)
	}
	if l := leaseOf(s, "id1"); l != nil {
		t.Fatalf("revoked lease still present: %+v", l)
	}
	if _, dyn := s.snapshot(); len(dyn) != 0 {
		t.Fatalf("revoked relay still published: %v", dyn)
	}
	if lg, pg := onDiskGeneration(t, s); lg != pg {
		t.Fatalf("disk generations disagree after revoke: %d vs %d", lg, pg)
	}
}

// The legacy line-per-URL known relays file is imported once into active
// leases; reopening must not import it again.
func TestLeaseLegacyMigration(t *testing.T) {
	dir := t.TempDir()
	leasesPath := filepath.Join(dir, "leases.json")
	publishedPath := filepath.Join(dir, "published.json")
	legacyPath := filepath.Join(dir, "known_relays")

	content := "relay://192.0.2.100:22067/?id=id1\nrelay://192.0.2.101:22067/?id=id2\n\n"
	if err := os.WriteFile(legacyPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := newRelayStore(leasesPath, publishedPath, legacyPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.stop)
	gen, dyn := s.snapshot()
	if gen != 1 || len(dyn) != 2 {
		t.Fatalf("migration state wrong: gen=%d dyn=%v", gen, dyn)
	}
	for _, r := range dyn {
		if id := identityForURI(r.uri); id != "id1" && id != "id2" {
			t.Errorf("unexpected migrated identity %q", id)
		}
	}
	if lg, pg := onDiskGeneration(t, s); lg != 1 || pg != 1 {
		t.Fatalf("migration not persisted: ledger=%d list=%d", lg, pg)
	}
	s.stop()

	// Remove a relay from the ledger by revoking, then reopen: the legacy
	// file must not re-import it.
	existed, err := s.revoke("id1")
	if err != nil || !existed {
		t.Fatalf("revoke: %v %v", existed, err)
	}
	s2, err := newRelayStore(leasesPath, publishedPath, legacyPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s2.stop)
	if _, dyn := s2.snapshot(); len(dyn) != 1 || identityForURI(dyn[0].uri) != "id2" {
		t.Fatalf("legacy file re-imported after migration: %v", dyn)
	}
}

// Switching one identity never changes another identity's active URL or
// lease deadline.
func TestLeaseOtherIdentitiesUnaffected(t *testing.T) {
	s := newTestStore(t)
	a := testRelay("idA", "192.0.2.110:22067")
	b := testRelay("idB", "192.0.2.120:22067")
	mustAdmit(t, s, "idA", a, 3)
	mustAdmit(t, s, "idB", b, 3)

	before := leaseOf(s, "idB")
	aNew := testRelay("idA", "192.0.2.111:22067")
	mustAdmit(t, s, "idA", aNew, 3)

	after := leaseOf(s, "idB")
	if after.activeURL != before.activeURL || !after.expiresAt.Equal(before.expiresAt) {
		t.Fatalf("idB changed while idA switched:\nbefore=%+v\nafter =%+v", before, after)
	}
	_, dyn := s.snapshot()
	hosts := map[string]bool{}
	for _, r := range dyn {
		hosts[r.uri.Host] = true
	}
	if !hosts["192.0.2.111:22067"] || !hosts["192.0.2.120:22067"] || hosts["192.0.2.110:22067"] {
		t.Fatalf("unexpected published hosts after switch: %v", hosts)
	}
}
