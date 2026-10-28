// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/syncthing/syncthing/internal/blob"
	"github.com/syncthing/syncthing/internal/gen/discosrv"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/tlsutil"
)

func testAddrs(tc *testClock, addresses ...string) []*discosrv.DatabaseAddress {
	addrs := make([]*discosrv.DatabaseAddress, len(addresses))
	for i, a := range addresses {
		addrs[i] = &discosrv.DatabaseAddress{Address: a, Expires: tc.Now().Add(time.Hour).UnixNano()}
	}
	return addrs
}

// The core generation rules: retirement creates a sticky tombstone,
// announcements and old events can't resurrect it, and only a strictly
// higher generation re-enables it.
func TestRetirementGeneration(t *testing.T) {
	db := newInMemoryStore(t.TempDir(), 0, nil)
	tc := &testClock{time.Now()}
	db.clock = tc
	id := protocol.EmptyDeviceID

	addrs := testAddrs(tc, "tcp://1.2.3.4:5")
	if err := db.merge(&id, addrs, tc.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	rec, err := db.get(&id)
	if err != nil || len(rec.Addresses) != 1 {
		t.Fatalf("announcement should be visible, got %v %v", rec, err)
	}

	// Retire bumps the generation and clears all addresses.
	tomb, err := db.retire(&id)
	if err != nil {
		t.Fatal(err)
	}
	if tomb.Generation != 1 || !tomb.Retired {
		t.Fatalf("unexpected tombstone: %+v", tomb)
	}
	rec, err = db.get(&id)
	if err != nil || len(rec.Addresses) != 0 || !rec.Retired {
		t.Fatalf("retired device should look not-found, got %+v", rec)
	}

	// A late device announcement can't resurrect it.
	if err := db.merge(&id, testAddrs(tc, "tcp://9.9.9.9:9"), tc.Now().UnixNano()); !errors.Is(err, errDeviceRetired) {
		t.Fatalf("merge should return errDeviceRetired, got %v", err)
	}
	if rec, _ := db.get(&id); len(rec.Addresses) != 0 {
		t.Fatal("announcement resurrected retired device")
	}

	// Same-generation events are sticky: neither addresses nor a
	// non-retired flag at the retired generation win.
	if err := db.apply(&id, testAddrs(tc, "tcp://9.9.9.9:9"), tc.Now().UnixNano(), 1, false); err != nil {
		t.Fatal(err)
	}
	if err := db.apply(&id, nil, tc.Now().UnixNano(), 1, true); err != nil {
		t.Fatal(err)
	}
	if rec, _ := db.get(&id); !rec.Retired {
		t.Fatal("tombstone should be sticky within the generation")
	}

	// An older-generation tombstone arriving late must not win over a
	// newer state.
	//
	// Explicit re-enable at generation 2: higher generation, no
	// addresses (the device must announce again).
	enabled, err := db.enable(&id)
	if err != nil {
		t.Fatal(err)
	}
	if enabled.Generation != 2 || enabled.Retired {
		t.Fatalf("unexpected enabled record: %+v", enabled)
	}
	rec, _ = db.get(&id)
	if rec.Retired || rec.Generation != 2 || len(rec.Addresses) != 0 {
		t.Fatalf("device should be re-enabled with no addresses, got %+v", rec)
	}

	// Fresh announcements are accepted again...
	if err := db.merge(&id, testAddrs(tc, "tcp://5.6.7.8:1"), tc.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	// ...without changing the generation.
	rec, _ = db.get(&id)
	if rec.Retired || rec.Generation != 2 || len(rec.Addresses) != 1 {
		t.Fatalf("announcement after re-enable failed: %+v", rec)
	}

	// ...and an old gen-1 tombstone replayed late does not re-retire.
	if err := db.apply(&id, nil, tc.Now().UnixNano(), 1, true); err != nil {
		t.Fatal(err)
	}
	if rec, _ := db.get(&id); rec.Retired || len(rec.Addresses) != 1 {
		t.Fatalf("stale tombstone resurrected: %+v", rec)
	}

	// Enabling a device that isn't retired is an error.
	other := protocol.GlobalDeviceID
	if _, err := db.enable(&other); !errors.Is(err, errNotRetired) {
		t.Fatalf("enable of unknown device should be errNotRetired, got %v", err)
	}

	// Retiring the re-enabled device bumps the generation again...
	tomb2, err := db.retire(&id)
	if err != nil {
		t.Fatal(err)
	}
	if tomb2.Generation != 3 {
		t.Fatalf("re-retiring a live device should bump to generation 3, got %d", tomb2.Generation)
	}
	// ...but retiring an already retired device is idempotent (used to
	// retry a failed backup upload).
	tomb3, err := db.retire(&id)
	if err != nil {
		t.Fatal(err)
	}
	if tomb3.Generation != 3 {
		t.Fatalf("idempotent retire should keep generation 3, got %d", tomb3.Generation)
	}
}

// Colliding retirements at the same generation resolve in the safe
// direction: a live record receiving a same-generation tombstone adopts
// the tombstone.
func TestApplySameGenerationCollision(t *testing.T) {
	db := newInMemoryStore(t.TempDir(), 0, nil)
	tc := &testClock{time.Now()}
	db.clock = tc
	id := protocol.EmptyDeviceID

	if err := db.apply(&id, testAddrs(tc, "tcp://1.1.1.1:1"), tc.Now().UnixNano(), 5, false); err != nil {
		t.Fatal(err)
	}
	if err := db.apply(&id, nil, tc.Now().UnixNano(), 5, true); err != nil {
		t.Fatal(err)
	}
	rec, _ := db.get(&id)
	if !rec.Retired || rec.Generation != 5 || len(rec.Addresses) != 0 {
		t.Fatalf("same-generation tombstone should win, got %+v", rec)
	}
}

// Retirement and re-enable survive a restart via the snapshot.
func TestRetirementPersistence(t *testing.T) {
	dir := t.TempDir()
	id := protocol.EmptyDeviceID

	db := newInMemoryStore(dir, 0, nil)
	tc := &testClock{time.Now()}
	db.clock = tc
	if err := db.merge(&id, testAddrs(tc, "tcp://1.2.3.4:5"), tc.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.retire(&id); err != nil {
		t.Fatal(err)
	}

	db2 := newInMemoryStore(dir, 0, nil)
	db2.clock = tc
	rec, _ := db2.get(&id)
	if !rec.Retired || rec.Generation != 1 {
		t.Fatalf("tombstone not restored from snapshot: %+v", rec)
	}
	if err := db2.merge(&id, testAddrs(tc, "tcp://9.9.9.9:9"), tc.Now().UnixNano()); !errors.Is(err, errDeviceRetired) {
		t.Fatalf("restored tombstone should reject announcements, got %v", err)
	}

	if _, err := db2.enable(&id); err != nil {
		t.Fatal(err)
	}
	db3 := newInMemoryStore(dir, 0, nil)
	db3.clock = tc
	rec, _ = db3.get(&id)
	if rec.Retired || rec.Generation != 2 {
		t.Fatalf("re-enable not persisted: %+v", rec)
	}
}

// Tombstones are not garbage collected with the one week expiry.
func TestTombstoneSurvivesExpiry(t *testing.T) {
	db := newInMemoryStore(t.TempDir(), 0, nil)
	tc := &testClock{time.Now()}
	db.clock = tc
	id := protocol.EmptyDeviceID
	stale := protocol.GlobalDeviceID

	if _, err := db.retire(&id); err != nil {
		t.Fatal(err)
	}
	if err := db.merge(&stale, testAddrs(tc, "tcp://1.1.1.1:1"), tc.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}

	tc.wind(8 * 24 * time.Hour)
	db.expireAndCalculateStatistics()

	if rec, _ := db.get(&id); !rec.Retired {
		t.Fatal("tombstone should survive the one week cleanup")
	}
	if rec, _ := db.get(&stale); rec.Seen != 0 || len(rec.Addresses) != 0 {
		t.Fatal("ordinary stale record should have been dropped")
	}
}

// Journal events replay into state; a torn trailing write from a crash
// is ignored rather than failing startup.
func TestJournalReplay(t *testing.T) {
	dir := t.TempDir()
	id := protocol.EmptyDeviceID

	db := newInMemoryStore(dir, 0, nil)
	tc := &testClock{time.Now()}
	db.clock = tc
	tomb := &discosrv.DatabaseRecord{
		Seen:       tc.Now().UnixNano(),
		Generation: 1,
		Retired:    true,
	}
	db.mu.Lock()
	if err := db.appendJournalLocked(id, tomb); err != nil {
		db.mu.Unlock()
		t.Fatal(err)
	}
	db.mu.Unlock()

	// Append a torn frame that can never have been committed.
	jf, err := os.OpenFile(filepath.Join(dir, journalFile), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jf.Write([]byte{0x00, 0x00, 0x00, 0x09, 'x'}); err != nil {
		t.Fatal(err)
	}
	jf.Close()

	db2 := newInMemoryStore(dir, 0, nil)
	db2.clock = tc
	rec, _ := db2.get(&id)
	if !rec.Retired || rec.Generation != 1 {
		t.Fatalf("journal tombstone should be replayed despite torn tail, got %+v", rec)
	}
}

// A corrupt current snapshot falls back to the previous one.
func TestSnapshotCorruptionFallback(t *testing.T) {
	dir := t.TempDir()
	id := protocol.EmptyDeviceID

	db := newInMemoryStore(dir, 0, nil)
	tc := &testClock{time.Now()}
	db.clock = tc
	first := "tcp://1.1.1.1:1"
	if err := db.merge(&id, testAddrs(tc, first), tc.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := db.write(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Second write turns the first snapshot into records.db.prev.
	second := "tcp://2.2.2.2:2"
	if err := db.merge(&id, testAddrs(tc, second), tc.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := db.write(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Corrupt the tail of the current snapshot.
	main := filepath.Join(dir, snapshotFile)
	data, err := os.ReadFile(main)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(data) - 8; i < len(data); i++ {
		data[i] ^= 0xff
	}
	if err := os.WriteFile(main, data, 0o600); err != nil {
		t.Fatal(err)
	}

	db2 := newInMemoryStore(dir, 0, nil)
	db2.clock = tc
	rec, _ := db2.get(&id)
	if len(rec.Addresses) != 1 {
		t.Fatalf("should fall back to previous snapshot, got %+v", rec)
	}
	if rec.Addresses[0].Address != first {
		t.Fatalf("expected address from previous snapshot %q, got %q", first, rec.Addresses[0].Address)
	}
}

// A failed snapshot write leaves the previous snapshot in place.
func TestSnapshotWriteFailureKeepsPrevious(t *testing.T) {
	dir := t.TempDir()
	id := protocol.EmptyDeviceID

	db := newInMemoryStore(dir, 0, nil)
	tc := &testClock{time.Now()}
	db.clock = tc
	if err := db.merge(&id, testAddrs(tc, "tcp://1.1.1.1:1"), tc.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := db.write(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Make the directory read-only so the temp snapshot can't be
	// created.
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := db.write(context.Background()); err == nil {
		t.Fatal("write should fail in a read-only directory")
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	db2 := newInMemoryStore(dir, 0, nil)
	db2.clock = tc
	if rec, _ := db2.get(&id); len(rec.Addresses) != 1 {
		t.Fatalf("previous snapshot should remain usable, got %+v", rec)
	}
}

// Snapshots written by older versions (no magic or CRC) still load.
func TestLegacySnapshotFormat(t *testing.T) {
	dir := t.TempDir()
	id := protocol.EmptyDeviceID

	rec := &discosrv.ReplicationRecord{
		Key:  id[:],
		Seen: time.Now().UnixNano(),
		Addresses: []*discosrv.DatabaseAddress{
			{Address: "tcp://old.old:1", Expires: time.Now().Add(time.Hour).UnixNano()},
		},
	}
	payload, err := proto.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := os.Create(filepath.Join(dir, snapshotFile))
	if err != nil {
		t.Fatal(err)
	}
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(payload)))
	if _, err := fd.Write(l[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := fd.Write(payload); err != nil {
		t.Fatal(err)
	}
	fd.Close()

	db := newInMemoryStore(dir, 0, nil)
	got, err := db.get(&id)
	if err != nil || len(got.Addresses) != 1 || got.Addresses[0].Address != "tcp://old.old:1" {
		t.Fatalf("legacy snapshot should load, got %+v err=%v", got, err)
	}
}

// Retirement synchronously advances the backup; failed uploads surface
// as errors and can be retried without bumping the generation.
func TestRetirementBackup(t *testing.T) {
	dir := t.TempDir()
	id := protocol.EmptyDeviceID
	blobs := newFakeBlobStore()

	db := newInMemoryStore(dir, 0, blobs)
	tc := &testClock{time.Now()}
	db.clock = tc
	if err := db.merge(&id, testAddrs(tc, "tcp://1.2.3.4:5"), tc.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := db.write(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Failing backup: retire reports failure but is active in memory
	// and in the local snapshot.
	blobs.failUpload = true
	rec, err := db.retire(&id)
	if err == nil {
		t.Fatal("retire should fail when the backup can't be advanced")
	}
	if rec == nil || !rec.Retired {
		t.Fatal("in-memory tombstone should still be installed")
	}
	if got, _ := db.get(&id); !got.Retired {
		t.Fatal("retired state should be served despite backup failure")
	}
	backup := blobs.mustObject(t, db.objKey)
	if snapshotHasRetired(backup, id) {
		t.Fatal("failed upload must not have replaced the backup")
	}

	// Retrying succeeds without changing the generation, and the new
	// backup contains the tombstone.
	blobs.failUpload = false
	rec, err = db.retire(&id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Generation != 1 {
		t.Fatalf("retry should keep generation, got %d", rec.Generation)
	}
	backup = blobs.mustObject(t, db.objKey)
	if !snapshotHasRetired(backup, id) {
		t.Fatal("backup should contain the tombstone")
	}

	// A fresh instance bootstrapping from the backup is retired.
	db2 := newInMemoryStore(t.TempDir(), 0, blobs)
	if got, _ := db2.get(&id); !got.Retired || got.Generation != 1 {
		t.Fatalf("restored backup should be retired, got %+v", got)
	}
}

// snapshotHasRetired parses a v2 snapshot and reports whether it
// contains a retired record for the given device.
func snapshotHasRetired(data []byte, id protocol.DeviceID) bool {
	if len(data) < len(snapshotMagic) || string(data[:len(snapshotMagic)]) != snapshotMagic {
		return false
	}
	r := bytes.NewReader(data[len(snapshotMagic):])
	var buf []byte
	for {
		payload, err := readCRCFrame(r, buf)
		if err == io.EOF {
			return false
		} else if err != nil {
			return false
		}
		buf = payload
		rec := &discosrv.ReplicationRecord{}
		if proto.Unmarshal(payload, rec) != nil {
			return false
		}
		if bytes.Equal(rec.Key, id[:]) {
			return rec.Retired && rec.Generation > 0
		}
	}
}

// End-to-end HTTP behavior through the real handler.
func TestRetireHTTP(t *testing.T) {
	db := newInMemoryStore(t.TempDir(), 0, nil)
	api := newAPISrv("127.0.0.1:0", tls.Certificate{}, db, nil, true, true, 1000, 1000)
	srv := httptest.NewServer(http.HandlerFunc(api.handler))
	defer srv.Close()

	kf := t.TempDir() + "/cert"
	crt, err := tlsutil.NewCertificate(kf+".crt", kf+".key", "localhost", 7, true)
	if err != nil {
		t.Fatal(err)
	}
	certBs, err := os.ReadFile(kf + ".crt")
	if err != nil {
		t.Fatal(err)
	}
	certString := string(strings.ReplaceAll(string(regexp.MustCompile(`---[^\n]+---\n`).ReplaceAll(certBs, nil)), "\n", " "))
	devID := protocol.NewDeviceID(crt.Certificate[0]).String()

	announce := func() int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v2/?device="+devID, strings.NewReader(`{"addresses":["tcp://10.10.10.10:42000"]}`))
		req.Header.Set("X-Forwarded-Tls-Client-Cert", certString)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	lookup := func() int {
		resp, err := http.Get(srv.URL + "/v2/?device=" + devID)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	admin := func(action string) int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v2/"+action+"?device="+devID, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := announce(); code != http.StatusNoContent {
		t.Fatalf("announce: %d", code)
	}
	if code := lookup(); code != http.StatusOK {
		t.Fatalf("lookup before retire: %d", code)
	}
	if code := admin("retire"); code != http.StatusNoContent {
		t.Fatalf("retire: %d", code)
	}
	if code := lookup(); code != http.StatusNotFound {
		t.Fatalf("lookup after retire: %d", code)
	}
	if code := announce(); code != http.StatusNoContent {
		t.Fatalf("announce while retired: %d", code)
	}
	if code := lookup(); code != http.StatusNotFound {
		t.Fatalf("late announcement must not resurrect: %d", code)
	}
	if code := admin("enable"); code != http.StatusNoContent {
		t.Fatalf("enable: %d", code)
	}
	if code := lookup(); code != http.StatusNotFound {
		t.Fatalf("lookup before re-announce: %d", code)
	}
	if code := announce(); code != http.StatusNoContent {
		t.Fatalf("announce after enable: %d", code)
	}
	if code := lookup(); code != http.StatusOK {
		t.Fatalf("lookup after re-announce: %d", code)
	}
	if code := admin("enable"); code != http.StatusNotFound {
		t.Fatalf("enable of a live device: %d", code)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/retire?device="+devID, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET on retire endpoint: %d", resp.StatusCode)
	}
}

// fakeBlobStore is an in-memory blob.Store for tests.
type fakeBlobStore struct {
	mu         sync.Mutex
	objects    map[string][]byte
	failUpload bool
}

func newFakeBlobStore() *fakeBlobStore {
	return &fakeBlobStore{objects: make(map[string][]byte)}
}

func (s *fakeBlobStore) Upload(_ context.Context, key string, r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failUpload {
		return errors.New("simulated upload failure")
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.objects[key] = b
	return nil
}

func (s *fakeBlobStore) Download(_ context.Context, key string, w blob.Writer) error {
	s.mu.Lock()
	b, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return os.ErrNotExist
	}
	_, err := w.WriteAt(b, 0)
	return err
}

func (s *fakeBlobStore) LatestKey(_ context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.objects {
		return key, nil
	}
	return "", errors.New("no objects")
}

func (s *fakeBlobStore) mustObject(t *testing.T, key string) []byte {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	if !ok {
		t.Fatalf("no backup object %q", key)
	}
	return b
}
