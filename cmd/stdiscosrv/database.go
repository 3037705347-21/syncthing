// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bufio"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	"google.golang.org/protobuf/proto"

	"github.com/syncthing/syncthing/internal/blob"
	"github.com/syncthing/syncthing/internal/gen/discosrv"
	"github.com/syncthing/syncthing/internal/protoutil"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

const (
	snapshotFile = "records.db"
	snapshotTmp  = "records.db.tmp"
	snapshotPrev = "records.db.prev"
	restoreTmp   = "records.db.restore.tmp"
	journalFile  = "events.log"

	// snapshotMagic prefixes the CRC framed snapshot format. Legacy
	// snapshots (no magic, unframed records) are still read on upgrade.
	snapshotMagic = "STDSNDB2"

	// A single frame larger than this is corruption rather than data.
	maxFrameSize = 1 << 20
)

var (
	errCorruptSnapshot = errors.New("corrupt snapshot")
	errEmptySnapshot   = errors.New("empty snapshot")
	errDeviceRetired   = errors.New("device is retired")
	errNotRetired      = errors.New("device is not retired")
)

type clock interface {
	Now() time.Time
}

type defaultClock struct{}

func (defaultClock) Now() time.Time {
	return time.Now()
}

type database interface {
	put(key *protocol.DeviceID, rec *discosrv.DatabaseRecord) error
	merge(key *protocol.DeviceID, addrs []*discosrv.DatabaseAddress, seen int64) error
	apply(key *protocol.DeviceID, addrs []*discosrv.DatabaseAddress, seen int64, generation uint64, retired bool) error
	get(key *protocol.DeviceID) (*discosrv.DatabaseRecord, error)
	retire(key *protocol.DeviceID) (*discosrv.DatabaseRecord, error)
	enable(key *protocol.DeviceID) (*discosrv.DatabaseRecord, error)
}

type inMemoryStore struct {
	m   *xsync.MapOf[protocol.DeviceID, *discosrv.DatabaseRecord]
	dir string

	// mu serializes everything that changes durable state: journal
	// appends, snapshot rotations and backup uploads.
	mu            sync.Mutex
	journal       *os.File
	backupDirty   bool
	flushInterval time.Duration

	blobs  blob.Store
	objKey string
	clock  clock
}

func newInMemoryStore(dir string, flushInterval time.Duration, blobs blob.Store) *inMemoryStore {
	hn, err := os.Hostname()
	if err != nil {
		hn = rand.String(8)
	}
	s := &inMemoryStore{
		m:             xsync.NewMapOf[protocol.DeviceID, *discosrv.DatabaseRecord](),
		dir:           dir,
		flushInterval: flushInterval,
		blobs:         blobs,
		objKey:        hn + ".db",
		clock:         defaultClock{},
	}

	s.journal = openJournal(dir)
	s.removeStaleTempFiles()
	s.load()
	s.expireAndCalculateStatistics()
	return s
}

func (s *inMemoryStore) put(key *protocol.DeviceID, rec *discosrv.DatabaseRecord) error {
	t0 := time.Now()
	s.m.Store(*key, rec)
	databaseOperations.WithLabelValues(dbOpPut, dbResSuccess).Inc()
	databaseOperationSeconds.WithLabelValues(dbOpPut).Observe(time.Since(t0).Seconds())
	return nil
}

// merge applies a plain device announcement. It never changes the
// deletion generation and can never bring a retired device back.
func (s *inMemoryStore) merge(key *protocol.DeviceID, addrs []*discosrv.DatabaseAddress, seen int64) error {
	t0 := time.Now()

	if oldRec, ok := s.m.Load(*key); ok && oldRec.Retired {
		// A tombstone wins over announcements and legacy replication
		// messages regardless of their timestamps.
		databaseOperations.WithLabelValues(dbOpMerge, dbResRetired).Inc()
		databaseOperationSeconds.WithLabelValues(dbOpMerge).Observe(time.Since(t0).Seconds())
		return errDeviceRetired
	}

	newRec := &discosrv.DatabaseRecord{
		Addresses: addrs,
		Seen:      seen,
	}
	if oldRec, ok := s.m.Load(*key); ok {
		newRec = merge(newRec, oldRec)
		newRec.Generation = oldRec.Generation
	}
	s.m.Store(*key, newRec)

	databaseOperations.WithLabelValues(dbOpMerge, dbResSuccess).Inc()
	databaseOperationSeconds.WithLabelValues(dbOpMerge).Observe(time.Since(t0).Seconds())

	return nil
}

// apply applies an explicit retire/re-enable event carrying a deletion
// generation. Stale generations are ignored, tombstones are sticky
// within a generation, and only a strictly higher generation can
// override the current state.
func (s *inMemoryStore) apply(key *protocol.DeviceID, addrs []*discosrv.DatabaseAddress, seen int64, generation uint64, retired bool) error {
	t0 := time.Now()

	if generation == 0 {
		// Not an admin event, treat it as a plain announcement.
		return s.merge(key, addrs, seen)
	}

	oldRec, ok := s.m.Load(*key)
	switch {
	case !ok:
		if retired {
			addrs = nil
		}
		s.m.Store(*key, &discosrv.DatabaseRecord{
			Addresses:  addrs,
			Seen:       seen,
			Generation: generation,
			Retired:    retired,
		})
	case generation < oldRec.Generation:
		// Old event replayed late, ignore it.
	case generation == oldRec.Generation:
		switch {
		case oldRec.Retired:
			// Tombstone is sticky within the generation; address
			// messages arriving at the retired generation are dropped.
		case retired:
			// A peer retired at the same generation we know as live
			// (independent retirements colliding): adopt the
			// tombstone, the safe direction.
			s.m.Store(*key, &discosrv.DatabaseRecord{
				Seen:       max(oldRec.Seen, seen),
				Generation: generation,
				Retired:    true,
			})
		default:
			// Both live at this generation, union the addresses.
			newRec := merge(&discosrv.DatabaseRecord{
				Addresses:  addrs,
				Seen:       seen,
				Generation: generation,
			}, oldRec)
			newRec.Generation = generation
			s.m.Store(*key, newRec)
		}
	default:
		// A newer generation replaces everything wholesale.
		if retired {
			addrs = nil
		}
		s.m.Store(*key, &discosrv.DatabaseRecord{
			Addresses:  addrs,
			Seen:       seen,
			Generation: generation,
			Retired:    retired,
		})
	}

	databaseOperations.WithLabelValues(dbOpApply, dbResSuccess).Inc()
	databaseOperationSeconds.WithLabelValues(dbOpApply).Observe(time.Since(t0).Seconds())
	return nil
}

func (s *inMemoryStore) get(key *protocol.DeviceID) (*discosrv.DatabaseRecord, error) {
	t0 := time.Now()
	defer func() {
		databaseOperationSeconds.WithLabelValues(dbOpGet).Observe(time.Since(t0).Seconds())
	}()

	rec, ok := s.m.Load(*key)
	if !ok {
		databaseOperations.WithLabelValues(dbOpGet, dbResNotFound).Inc()
		return &discosrv.DatabaseRecord{}, nil
	}

	// A retired device looks not-found to every query, forever.
	if rec.Retired {
		databaseOperations.WithLabelValues(dbOpGet, dbResRetired).Inc()
		return &discosrv.DatabaseRecord{
			Seen:       rec.Seen,
			Generation: rec.Generation,
			Retired:    true,
		}, nil
	}

	naddresses, changed := expire(rec.Addresses, s.clock.Now())
	if changed {
		rec = &discosrv.DatabaseRecord{
			Addresses:  naddresses,
			Seen:       rec.Seen,
			Generation: rec.Generation,
		}
	}
	databaseOperations.WithLabelValues(dbOpGet, dbResSuccess).Inc()
	return rec, nil
}

// retire writes a durable tombstone for the device at a strictly higher
// generation, snapshots the state and pushes the backup before
// returning. Retiring an already retired device is idempotent and is
// used to retry a previously failed backup upload.
func (s *inMemoryStore) retire(key *protocol.DeviceID) (*discosrv.DatabaseRecord, error) {
	t0 := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if oldRec, ok := s.m.Load(*key); ok && oldRec.Retired {
		err := s.writeLocked(context.Background())
		if err == nil {
			databaseOperations.WithLabelValues(dbOpRetire, dbResSuccess).Inc()
		}
		databaseOperationSeconds.WithLabelValues(dbOpRetire).Observe(time.Since(t0).Seconds())
		return oldRec, err
	}

	generation := uint64(1)
	if oldRec, ok := s.m.Load(*key); ok {
		generation = oldRec.Generation + 1
	}
	rec := &discosrv.DatabaseRecord{
		Seen:       s.clock.Now().UnixNano(),
		Generation: generation,
		Retired:    true,
	}

	if err := s.appendJournalLocked(*key, rec); err != nil {
		return nil, err
	}
	s.m.Store(*key, rec)

	if err := s.writeLocked(context.Background()); err != nil {
		return rec, err
	}

	databaseOperations.WithLabelValues(dbOpRetire, dbResSuccess).Inc()
	databaseOperationSeconds.WithLabelValues(dbOpRetire).Observe(time.Since(t0).Seconds())
	return rec, nil
}

// enable clears a tombstone. It requires an existing retired record and
// bumps the generation so that no old announcement or replication
// message can be mistaken for the re-enable.
func (s *inMemoryStore) enable(key *protocol.DeviceID) (*discosrv.DatabaseRecord, error) {
	t0 := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	oldRec, ok := s.m.Load(*key)
	if !ok || !oldRec.Retired {
		return nil, errNotRetired
	}

	rec := &discosrv.DatabaseRecord{
		Seen:       s.clock.Now().UnixNano(),
		Generation: oldRec.Generation + 1,
		Retired:    false,
	}

	if err := s.appendJournalLocked(*key, rec); err != nil {
		return nil, err
	}
	s.m.Store(*key, rec)

	if err := s.writeLocked(context.Background()); err != nil {
		return rec, err
	}

	databaseOperations.WithLabelValues(dbOpEnable, dbResSuccess).Inc()
	databaseOperationSeconds.WithLabelValues(dbOpEnable).Observe(time.Since(t0).Seconds())
	return rec, nil
}

// replicateAdminEvents re-publishes every retire/re-enable event known
// to this instance. It's used at startup to reconverge replicas that
// were disconnected when the event was first published.
func (s *inMemoryStore) replicateAdminEvents(r replicator) {
	s.m.Range(func(key protocol.DeviceID, rec *discosrv.DatabaseRecord) bool {
		if rec.Generation == 0 {
			return true
		}
		r.send(&discosrv.ReplicationRecord{
			Key:        key[:],
			Seen:       rec.Seen,
			Generation: rec.Generation,
			Retired:    rec.Retired,
		})
		return true
	})
}

func (s *inMemoryStore) Serve(ctx context.Context) error {
	if s.flushInterval <= 0 {
		<-ctx.Done()
		return nil
	}

	t := time.NewTimer(s.flushInterval)
	defer t.Stop()

loop:
	for {
		select {
		case <-t.C:
			slog.InfoContext(ctx, "Calculating statistics")
			s.expireAndCalculateStatistics()
			slog.InfoContext(ctx, "Flushing database")
			if err := s.write(ctx); err != nil {
				slog.ErrorContext(ctx, "Failed to write database", "error", err)
			}
			slog.InfoContext(ctx, "Finished flushing database")
			t.Reset(s.flushInterval)

		case <-ctx.Done():
			// We're done.
			break loop
		}
	}

	return s.write(ctx)
}

func (s *inMemoryStore) expireAndCalculateStatistics() {
	now := s.clock.Now()
	cutoff24h := now.Add(-24 * time.Hour).UnixNano()
	cutoff1w := now.Add(-7 * 24 * time.Hour).UnixNano()
	current, currentIPv4, currentIPv6, currentIPv6GUA, last24h, last1w, retired := 0, 0, 0, 0, 0, 0, 0

	n := 0
	s.m.Range(func(key protocol.DeviceID, rec *discosrv.DatabaseRecord) bool {
		if n%1000 == 0 {
			runtime.Gosched()
		}
		n++

		addresses, changed := expire(rec.Addresses, now)
		if changed {
			rec = &discosrv.DatabaseRecord{
				Addresses:  addresses,
				Seen:       rec.Seen,
				Generation: rec.Generation,
				Retired:    rec.Retired,
			}
			s.m.Store(key, rec)
		}

		switch {
		case rec.Retired:
			// Tombstones are kept indefinitely.
			retired++
		case len(rec.Addresses) > 0:
			current++
			seenIPv4, seenIPv6, seenIPv6GUA := false, false, false
			for _, addr := range rec.Addresses {
				// We do fast and loose matching on strings here instead of
				// parsing the address and the IP and doing "proper" checks,
				// to keep things fast and generate less garbage.
				if strings.Contains(addr.Address, "[") {
					seenIPv6 = true
					if strings.Contains(addr.Address, "[2") {
						seenIPv6GUA = true
					}
				} else {
					seenIPv4 = true
				}
				if seenIPv4 && seenIPv6 && seenIPv6GUA {
					break
				}
			}
			if seenIPv4 {
				currentIPv4++
			}
			if seenIPv6 {
				currentIPv6++
			}
			if seenIPv6GUA {
				currentIPv6GUA++
			}
		case rec.Seen > cutoff24h:
			last24h++
		case rec.Seen > cutoff1w:
			last1w++
		default:
			// drop the record if it's older than a week
			s.m.Delete(key)
		}
		return true
	})

	databaseKeys.WithLabelValues("current").Set(float64(current))
	databaseKeys.WithLabelValues("currentIPv4").Set(float64(currentIPv4))
	databaseKeys.WithLabelValues("currentIPv6").Set(float64(currentIPv6))
	databaseKeys.WithLabelValues("currentIPv6GUA").Set(float64(currentIPv6GUA))
	databaseKeys.WithLabelValues("last24h").Set(float64(last24h))
	databaseKeys.WithLabelValues("last1w").Set(float64(last1w))
	databaseKeys.WithLabelValues("retired").Set(float64(retired))
	databaseStatisticsSeconds.Set(time.Since(now).Seconds())
}

func (s *inMemoryStore) write(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(ctx)
}

// writeLocked persists the current state: a CRC framed snapshot is
// written to a temp file, fsynced and atomically rotated into place with
// the previous snapshot retained, then the journal is truncated and the
// backup is uploaded. A failure at any point leaves the previous
// snapshot (and backup object) intact and usable.
func (s *inMemoryStore) writeLocked(ctx context.Context) (err error) {
	t0 := time.Now()
	defer func() {
		if err == nil {
			databaseWriteSeconds.Set(time.Since(t0).Seconds())
			databaseLastWritten.Set(float64(t0.Unix()))
		}
	}()

	dbf := filepath.Join(s.dir, snapshotFile)
	tmp := filepath.Join(s.dir, snapshotTmp)
	prev := filepath.Join(s.dir, snapshotPrev)

	fd, err := os.Create(tmp)
	if err != nil {
		return err
	}
	abort := func() {
		_ = fd.Close()
		_ = os.Remove(tmp)
	}

	bw := bufio.NewWriterSize(fd, 1<<20)
	if _, err := bw.WriteString(snapshotMagic); err != nil {
		abort()
		return err
	}

	var frame, payload []byte
	var rangeErr error
	now := s.clock.Now()
	cutoff1w := now.Add(-7 * 24 * time.Hour).UnixNano()
	n := 0
	s.m.Range(func(key protocol.DeviceID, value *discosrv.DatabaseRecord) bool {
		if n%1000 == 0 {
			runtime.Gosched()
		}
		n++

		if !value.Retired && value.Seen < cutoff1w {
			// drop ordinary records older than a week; keep tombstones
			return true
		}
		rec := &discosrv.ReplicationRecord{
			Key:        key[:],
			Addresses:  value.Addresses,
			Seen:       value.Seen,
			Generation: value.Generation,
			Retired:    value.Retired,
		}
		size := proto.Size(rec)
		if size > len(payload) {
			payload = make([]byte, size)
		}
		m, err := protoutil.MarshalTo(payload, rec)
		if err != nil {
			rangeErr = err
			return false
		}
		frame = marshalCRCFrame(frame[:0], payload[:m])
		if _, err := bw.Write(frame); err != nil {
			rangeErr = err
			return false
		}
		return true
	})
	if rangeErr != nil {
		abort()
		return rangeErr
	}

	if err := bw.Flush(); err != nil {
		abort()
		return err
	}
	if err := fd.Sync(); err != nil {
		abort()
		return err
	}
	if err := fd.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// Rotate: current snapshot becomes the previous one, new snapshot
	// becomes current. Between the two renames the previous file is the
	// usable fallback; the journal always covers any gap.
	if _, statErr := os.Stat(dbf); statErr == nil {
		_ = os.Remove(prev)
		if err := os.Rename(dbf, prev); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		syncDir(s.dir)
	}
	if err := os.Rename(tmp, dbf); err != nil {
		return err
	}
	syncDir(s.dir)

	if info, err := os.Lstat(dbf); err == nil {
		slog.Info("Saved database", "name", dbf, "size", info.Size(), "modtime", info.ModTime())
	} else {
		slog.Warn("Failed to stat database after save", "error", err)
	}

	// The snapshot now contains everything the journal did, so the
	// journal can be truncated. Replaying the same events after a crash
	// in between here is idempotent due to generation comparisons.
	if s.journal != nil {
		if err := s.journal.Truncate(0); err != nil {
			slog.Warn("Failed to truncate database journal", "error", err)
		} else {
			_ = s.journal.Sync()
		}
	}

	// Upload to backup storage. An object upload either replaces the old
	// object atomically or leaves it untouched on failure.
	if err := s.uploadBackupLocked(ctx, dbf); err != nil {
		s.backupDirty = true
		slog.Error("Failed to upload database to backup storage; will retry", "error", err)
		return err
	}
	s.backupDirty = false

	return nil
}

func (s *inMemoryStore) uploadBackupLocked(ctx context.Context, dbf string) error {
	if s.blobs == nil {
		return nil
	}
	fd, err := os.Open(dbf)
	if err != nil {
		return err
	}
	defer fd.Close()
	return s.blobs.Upload(ctx, s.objKey, fd)
}

// load restores the durable state on startup. It tries the current
// snapshot, then the previous one, then backup storage, and finally
// replays the journal on top of whatever it found.
func (s *inMemoryStore) load() int {
	source := "empty"

	n, err := s.readSnapshotFile(filepath.Join(s.dir, snapshotFile))
	switch {
	case err == nil:
		source = "snapshot"
	default:
		slog.Warn("Failed to read database snapshot", "error", err)

		// Discard anything partially loaded from the broken snapshot
		// so a fallback can't mix with it.
		s.m = xsync.NewMapOf[protocol.DeviceID, *discosrv.DatabaseRecord]()

		if n2, err2 := s.readSnapshotFile(filepath.Join(s.dir, snapshotPrev)); err2 == nil {
			slog.Warn("Recovered previous database snapshot", "records", n2)
			if err := os.Rename(filepath.Join(s.dir, snapshotPrev), filepath.Join(s.dir, snapshotFile)); err == nil {
				syncDir(s.dir)
			}
			n = n2
			source = "previous snapshot"
			err = nil
		} else if s.blobs != nil {
			slog.Warn("Previous snapshot unusable, trying backup storage", "error", err2)
			s.m = xsync.NewMapOf[protocol.DeviceID, *discosrv.DatabaseRecord]()
			if err3 := s.restoreFromBackup(); err3 == nil {
				n2, err4 := s.readSnapshotFile(filepath.Join(s.dir, snapshotFile))
				if err4 == nil {
					n = n2
					source = "backup storage"
					err = nil
				} else {
					slog.Error("Downloaded database snapshot is unusable", "error", err4)
				}
			} else {
				slog.Error("Failed to restore database from backup storage", "error", err3)
			}
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errEmptySnapshot) {
		slog.Error("Failed to read database", "error", err)
	}

	jn := s.replayJournal()
	n += jn

	slog.Info("Loaded database", "records", n, "source", source, "journalEvents", jn, "backupDirty", s.backupDirty)
	return n
}

func (s *inMemoryStore) readSnapshotFile(path string) (int, error) {
	fd, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	var magic [len(snapshotMagic)]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, errEmptySnapshot
		}
		return 0, err
	}

	if string(magic[:]) == snapshotMagic {
		return s.readFramedSnapshot(br)
	}

	// Legacy snapshot: length prefixed protobuf records without magic
	// or CRC. Reparse from the beginning.
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	return s.readLegacySnapshot(fd)
}

func (s *inMemoryStore) readFramedSnapshot(r io.Reader) (int, error) {
	var buf []byte
	nr := 0
	for {
		payload, err := readCRCFrame(r, buf)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nr, err
		}
		buf = payload
		if err := s.storeReplicationRecord(payload); err != nil {
			return nr, err
		}
		nr++
	}
	return nr, nil
}

func (s *inMemoryStore) readLegacySnapshot(r io.Reader) (int, error) {
	br := bufio.NewReader(r)
	var buf []byte
	nr := 0
	for {
		var n uint32
		if err := binary.Read(br, binary.BigEndian, &n); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nr, err
		}
		if n > maxFrameSize {
			return nr, errCorruptSnapshot
		}
		if int(n) > len(buf) {
			buf = make([]byte, n)
		}
		if _, err := io.ReadFull(br, buf[:n]); err != nil {
			return nr, err
		}
		if err := s.storeReplicationRecord(buf[:n]); err != nil {
			return nr, err
		}
		nr++
	}
	return nr, nil
}

// validateSnapshotFile reads and verifies a snapshot file without
// loading it into memory.
func validateSnapshotFile(path string) error {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	var magic [len(snapshotMagic)]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return errEmptySnapshot
	}
	if string(magic[:]) != snapshotMagic {
		return errCorruptSnapshot
	}

	var buf []byte
	for {
		payload, err := readCRCFrame(br, buf)
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		buf = payload
		rec := &discosrv.ReplicationRecord{}
		if err := proto.Unmarshal(payload, rec); err != nil {
			return err
		}
		if _, err := deviceIDFromRecordKey(rec.Key); err != nil {
			return err
		}
	}
}

func (s *inMemoryStore) storeReplicationRecord(payload []byte) error {
	rec := &discosrv.ReplicationRecord{}
	if err := proto.Unmarshal(payload, rec); err != nil {
		return err
	}
	key, err := deviceIDFromRecordKey(rec.Key)
	if err != nil {
		slog.Error("Got bad device ID while reading database", "error", err)
		return nil
	}

	slices.SortFunc(rec.Addresses, Cmp)
	rec.Addresses, _ = expire(slices.CompactFunc(rec.Addresses, Equal), s.clock.Now())
	s.m.Store(key, &discosrv.DatabaseRecord{
		Addresses:  rec.Addresses,
		Seen:       rec.Seen,
		Generation: rec.Generation,
		Retired:    rec.Retired,
	})
	return nil
}

func (s *inMemoryStore) restoreFromBackup() error {
	latestKey, err := s.blobs.LatestKey(context.Background())
	if err != nil {
		return err
	}

	dst := filepath.Join(s.dir, restoreTmp)
	fd, err := os.Create(dst)
	if err != nil {
		return err
	}
	if err := s.blobs.Download(context.Background(), latestKey, fd); err != nil {
		_ = fd.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := fd.Close(); err != nil {
		_ = os.Remove(dst)
		return err
	}

	// Verify the download before it can replace any local state.
	if err := validateSnapshotFile(dst); err != nil {
		_ = os.Remove(dst)
		return err
	}

	if err := os.Rename(dst, filepath.Join(s.dir, snapshotFile)); err != nil {
		_ = os.Remove(dst)
		return err
	}
	syncDir(s.dir)
	slog.Info("Restored database from backup storage", "key", latestKey)
	return nil
}

func (s *inMemoryStore) removeStaleTempFiles() {
	for _, name := range []string{snapshotTmp, restoreTmp} {
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !os.IsNotExist(err) {
			slog.Warn("Failed to remove stale temp file", "file", name, "error", err)
		}
	}
}

// appendJournalLocked durably appends a retire/re-enable event. The
// event is on disk before the in-memory state is touched, so a crash at
// any later point can be recovered by replaying the journal.
func (s *inMemoryStore) appendJournalLocked(key protocol.DeviceID, rec *discosrv.DatabaseRecord) error {
	if s.journal == nil {
		return errors.New("journal not open")
	}
	rr := &discosrv.ReplicationRecord{
		Key:        key[:],
		Seen:       rec.Seen,
		Generation: rec.Generation,
		Retired:    rec.Retired,
	}
	payload, err := proto.Marshal(rr)
	if err != nil {
		return err
	}
	frame := marshalCRCFrame(nil, payload)
	if _, err := s.journal.Write(frame); err != nil {
		return err
	}
	return s.journal.Sync()
}

func (s *inMemoryStore) replayJournal() int {
	fd, err := os.Open(filepath.Join(s.dir, journalFile))
	if err != nil {
		return 0
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	var buf []byte
	n := 0
	for {
		payload, err := readCRCFrame(br, buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// A torn or partial trailing write from a crash mid
				// append: everything before it is committed and the
				// rest never existed.
				slog.Warn("Ignoring trailing partial journal data", "error", err, "events", n)
			}
			break
		}
		buf = payload

		rec := &discosrv.ReplicationRecord{}
		if err := proto.Unmarshal(payload, rec); err != nil {
			slog.Warn("Ignoring unparsable journal event", "error", err)
			break
		}
		key, err := deviceIDFromRecordKey(rec.Key)
		if err != nil {
			slog.Warn("Ignoring journal event with bad device ID", "error", err)
			break
		}
		if err := s.apply(&key, rec.Addresses, rec.Seen, rec.Generation, rec.Retired); err != nil {
			slog.Warn("Failed to replay journal event", "error", err)
			break
		}
		n++
	}
	return n
}

func openJournal(dir string) *os.File {
	fd, err := os.OpenFile(filepath.Join(dir, journalFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		slog.Error("Failed to open database journal", "error", err)
		return nil
	}
	return fd
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	if err := d.Sync(); err != nil {
		slog.Warn("Failed to sync database directory", "dir", dir, "error", err)
	}
	_ = d.Close()
}

func deviceIDFromRecordKey(key []byte) (protocol.DeviceID, error) {
	id, err := protocol.DeviceIDFromBytes(key)
	if err != nil {
		id, err = protocol.DeviceIDFromString(string(key))
	}
	return id, err
}

// marshalCRCFrame returns a [uint32 length][payload][uint32 CRC32]
// frame.
func marshalCRCFrame(dst, payload []byte) []byte {
	dst = dst[:0]
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(payload)))
	dst = append(dst, l[:]...)
	dst = append(dst, payload...)
	var c [4]byte
	binary.BigEndian.PutUint32(c[:], crc32.ChecksumIEEE(payload))
	dst = append(dst, c[:]...)
	return dst
}

// readCRCFrame reads one marshalCRCFrame frame. A clean end of file
// returns io.EOF; truncation, oversize or checksum mismatch returns an
// error.
func readCRCFrame(r io.Reader, buf []byte) ([]byte, error) {
	var l [4]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(l[:])
	if n > maxFrameSize {
		return nil, errCorruptSnapshot
	}
	if int(n)+4 > cap(buf) {
		buf = make([]byte, int(n)+4)
	}
	if _, err := io.ReadFull(r, buf[:n]); err != nil {
		return nil, errCorruptSnapshot
	}
	var c [4]byte
	if _, err := io.ReadFull(r, c[:]); err != nil {
		return nil, errCorruptSnapshot
	}
	if binary.BigEndian.Uint32(c[:]) != crc32.ChecksumIEEE(buf[:n]) {
		return nil, errCorruptSnapshot
	}
	return buf[:n], nil
}

// merge returns the merged result of the two database records a and b. The
// result is the union of the two address sets, with the newer expiry time
// chosen for any duplicates. The address list in a is overwritten and
// reused for the result; b is not modified.
func merge(a, b *discosrv.DatabaseRecord) *discosrv.DatabaseRecord {
	// Both lists must be sorted for this to work.

	a.Seen = max(a.Seen, b.Seen)

	aIdx := 0
	bIdx := 0
	for aIdx < len(a.Addresses) && bIdx < len(b.Addresses) {
		switch cmp.Compare(a.Addresses[aIdx].Address, b.Addresses[bIdx].Address) {
		case 0:
			// a == b, choose the newer expiry time
			a.Addresses[aIdx].Expires = max(a.Addresses[aIdx].Expires, b.Addresses[bIdx].Expires)
			aIdx++
			bIdx++
		case -1:
			// a < b, keep a and move on
			aIdx++
		case 1:
			// a > b, insert b before a
			a.Addresses = append(a.Addresses[:aIdx], append([]*discosrv.DatabaseAddress{b.Addresses[bIdx]}, a.Addresses[aIdx:]...)...)
			bIdx++
		}
	}
	if bIdx < len(b.Addresses) {
		a.Addresses = append(a.Addresses, b.Addresses[bIdx:]...)
	}

	return a
}

// expire returns the list of addresses after removing expired entries. A
// new slice is allocated if any changes are required, and the changed
// boolean indicates whether that happened or not.
func expire(addrs []*discosrv.DatabaseAddress, now time.Time) (result []*discosrv.DatabaseAddress, changed bool) {
	cutoff := now.UnixNano()
	remains := 0
	for _, a := range addrs {
		if a.Expires < cutoff {
			changed = true
		} else {
			remains++
		}
	}
	if !changed {
		return addrs, false
	}
	if remains == 0 {
		return nil, true
	}

	naddrs := make([]*discosrv.DatabaseAddress, 0, remains)
	for _, a := range addrs {
		if a.Expires >= cutoff {
			naddrs = append(naddrs, a)
		}
	}
	return naddrs, true
}

func Cmp(d, other *discosrv.DatabaseAddress) (n int) {
	if c := cmp.Compare(d.Address, other.Address); c != 0 {
		return c
	}
	return cmp.Compare(d.Expires, other.Expires)
}

func Equal(d, other *discosrv.DatabaseAddress) bool {
	return d.Address == other.Address
}
