// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package serve

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/ur/contract"
)

// stateFormat is the version of the persisted reconciliation state.
const stateFormat = 1

// liveSource is the provenance source for reports received directly from
// installations via the realtime endpoint.
const liveSource = ""

var (
	// ErrStaleBatch is returned when a batch comes from a source version
	// that has already been superseded. Nothing is applied.
	ErrStaleBatch = errors.New("stale batch: source version already applied")
)

// storeEntry is the per-install reconciliation state.
type storeEntry struct {
	// Report is the newest accepted observation for this installation.
	// It is retained even while the installation is disabled, but is
	// excluded from metrics and exports until it is explicitly re-enabled.
	Report *contract.Report `json:"report"`

	// Source is the regional source (batch manifest source) that the
	// accepted report came from, or liveSource for realtime reports.
	Source string `json:"source,omitempty"`

	// Version is the source version fence at the time the report was
	// accepted. Reports from older source versions can never roll it back.
	Version int64 `json:"version,omitempty"`

	// rankSum caches a hex digest of the report payload, used as the final
	// deterministic tie break. It is recomputed after loading state.
	rankSum string `json:"-"`

	// Disabled says whether the installation is currently explicitly
	// disabled. StateGen is the generation of the latest enable/disable
	// marker seen; only a strictly higher generation can change the state.
	Disabled bool  `json:"disabled,omitempty"`
	StateGen int64 `json:"stateGen,omitempty"`
}

// persistedState is the durable, replayable reconciliation state. It
// contains tombstones and provenance in addition to the reports
// themselves, so that disabled installations stay disabled after restart
// and old packages cannot roll newer data back.
type persistedState struct {
	Format   int                    `json:"format"`
	Versions map[string]int64       `json:"versions"`
	Entries  map[string]*storeEntry `json:"entries"`
}

type storeState struct {
	versions map[string]int64
	entries  map[string]*storeEntry
}

// reportStore keeps the canonical, reconciled set of installation reports.
//
// Reconciliation rules:
//
//   - Entries are keyed by the installation unique ID.
//   - A batch is accepted for a source only when its version is strictly
//     newer than the last applied version for that source; the same version
//     replays as a no-op, older versions are rejected wholesale.
//   - A batch record replaces the current report for an installation only
//     when it comes from a source version at least as new as the fence on
//     the current report and carries a newer observation (received
//     timestamp, with deterministic tie breaks). Older records in
//     replayed packages are ignored, so realtime reports arriving later
//     than a snapshot are never rolled back.
//   - Records absent from a snapshot are left untouched: a batch never
//     deletes entries it does not mention.
//   - Explicit disable markers hide an installation from metrics and
//     exports until an enable marker with a strictly higher generation
//     arrives; no report (batch or realtime) can bring it back on its own.
//   - All state changes from a batch are staged, durably committed
//     atomically, and only then published; any failure leaves the previous
//     state intact.
type reportStore struct {
	statePath string
	exportPath string

	// enrich post-processes a raw report (geoIP, version normalization and
	// similar). May be nil in tests.
	enrich func(*contract.Report)

	mut sync.RWMutex
	st  *storeState
}

func newReportStore(statePath, exportPath string, enrich func(*contract.Report)) *reportStore {
	return &reportStore{
		statePath:  statePath,
		exportPath: exportPath,
		enrich:     enrich,
		st: &storeState{
			versions: make(map[string]int64),
			entries:  make(map[string]*storeEntry),
		},
	}
}

// statePathFor derives the reconciliation state file path from the export
// dump path.
func statePathFor(dumpFile string) string {
	if strings.HasSuffix(dumpFile, ".jsons.gz") {
		return strings.TrimSuffix(dumpFile, ".jsons.gz") + ".state.gz"
	}
	return dumpFile + ".state"
}

// liveResult is the outcome of a realtime report ingest.
type liveResult int

const (
	liveAccepted liveResult = iota
	liveReplaced
	liveIgnoredDisabled
)

// AddLive stores a realtime report. Realtime reports are always the
// freshest observation for an active installation; they are stamped with
// the current global source-version fence, so an equal or older replayed
// package can never roll them back. Reports for explicitly disabled
// installations are ignored until a higher-generation enable marker.
func (s *reportStore) AddLive(rep *contract.Report) liveResult {
	s.mut.Lock()
	defer s.mut.Unlock()

	if e := s.st.entries[rep.UniqueID]; e != nil && e.Disabled {
		return liveIgnoredDisabled
	}

	_, existed := s.st.entries[rep.UniqueID]
	s.st.entries[rep.UniqueID] = &storeEntry{
		Report:  rep,
		Source:  liveSource,
		Version: s.maxVersionLocked(),
		rankSum: reportSum(rep),
	}
	if existed {
		return liveReplaced
	}
	return liveAccepted
}

// maxVersionLocked returns the highest applied source version across all
// regional sources.
func (s *reportStore) maxVersionLocked() int64 {
	var max int64
	for _, v := range s.st.versions {
		if v > max {
			max = v
		}
	}
	return max
}

// BatchResult summarizes an applied (or replayed) batch.
type BatchResult struct {
	Source  string
	Version int64
	// Replayed is true when the same source/version had already been
	// applied; nothing changed.
	Replayed bool

	AcceptedRecords  int
	IgnoredRecords   int
	Disabled         int
	Enabled          int
	IgnoredMarkers   int
}

// ApplyBatch validates and applies a parsed regional archive atomically.
// On any persistence error the in-memory state is left exactly as it was
// before the call; the caller is expected to have validated the archive
// contents already (parseBatch), but the source-version ordering is
// enforced here as well.
func (s *reportStore) ApplyBatch(b *parsedBatch) (*BatchResult, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	res := &BatchResult{Source: b.Source, Version: b.Version}

	switch cur := s.st.versions[b.Source]; {
	case b.Version < cur:
		return nil, ErrStaleBatch
	case b.Version == cur:
		// Idempotent replay of an already applied batch: nothing changes,
		// regardless of what realtime reports have arrived meanwhile.
		res.Replayed = true
		return res, nil
	}

	// Stage everything on a copy. Shared entry structs are never mutated
	// in place, so on failure the staged maps are simply discarded.
	cand := &storeState{
		versions: make(map[string]int64, len(s.st.versions)+1),
		entries:  make(map[string]*storeEntry, len(s.st.entries)),
	}
	for src, v := range s.st.versions {
		cand.versions[src] = v
	}
	for uid, e := range s.st.entries {
		cand.entries[uid] = e
	}

	// Explicit install-state markers are applied first, so records in the
	// same batch observe the resulting enabled/disabled state.
	for _, m := range b.InstallState {
		e := cand.entries[m.UniqueID]
		if e == nil {
			e = &storeEntry{}
			cand.entries[m.UniqueID] = e
		} else {
			e = cloneEntry(e)
			cand.entries[m.UniqueID] = e
		}

		if m.Generation <= e.StateGen {
			// An out of order or replayed marker cannot change state.
			res.IgnoredMarkers++
			continue
		}
		e.StateGen = m.Generation
		if m.Enabled {
			e.Disabled = false
			res.Enabled++
		} else {
			e.Disabled = true
			res.Disabled++
		}
	}

	// Records are reconciled one installation at a time.
	for _, rep := range b.reports {
		if s.enrich != nil {
			s.enrich(rep)
		}
		if rep.Received.IsZero() {
			rep.Received = b.Created
		}
		if rep.Date == "" {
			rep.Date = rep.Received.UTC().Format("20060102")
		}

		e := cand.entries[rep.UniqueID]
		if e != nil && e.Disabled {
			// A disabled installation is invisible until a higher
			// generation enable marker; reports cannot resurrect it.
			res.IgnoredRecords++
			continue
		}

		candRep := &storeEntry{
			Report:  rep,
			Source:  b.Source,
			Version: b.Version,
			rankSum: reportSum(rep),
		}
		if e != nil {
			e = cloneEntry(e)
			cand.entries[rep.UniqueID] = e
			if !recordNewer(candRep, e) {
				res.IgnoredRecords++
				continue
			}
		}
		cand.entries[rep.UniqueID] = candRep
		res.AcceptedRecords++
	}
	cand.versions[b.Source] = b.Version

	// Durably commit before publishing. A failure here means the whole
	// batch is as if it never arrived.
	if err := s.writeStateLocked(cand); err != nil {
		return nil, fmt.Errorf("persist batch state: %w", err)
	}

	s.st = cand

	// The export is derived data: refresh it best-effort. It gets
	// regenerated periodically and at startup in any case.
	if err := s.exportLocked(); err != nil {
		slog.Warn("Failed to refresh report export after batch", slogutil.Error(err))
	}

	return res, nil
}

// cloneEntry returns a shallow copy that can be modified without touching
// the shared original.
func cloneEntry(e *storeEntry) *storeEntry {
	cp := *e
	return &cp
}

// recordNewer reports whether the candidate report should replace the
// current entry.
//
// The candidate's source version must be at least as new as the fence on
// the current entry (older packages never roll anything back), and its
// observation must be strictly newer, with source and payload digests
// providing a deterministic total order for exact-timestamp ties.
func recordNewer(cand, cur *storeEntry) bool {
	if cand.Version < cur.Version {
		return false
	}
	if !cand.Report.Received.Equal(cur.Report.Received) {
		return cand.Report.Received.After(cur.Report.Received)
	}
	if cand.Version != cur.Version {
		return cand.Version > cur.Version
	}
	if cand.Source != cur.Source {
		return cand.Source > cur.Source
	}
	return cand.rankSum > cur.rankSum
}

func reportSum(rep *contract.Report) string {
	bs, err := json.Marshal(rep)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(bs)
	return hex.EncodeToString(sum[:])
}

// ActiveCount returns the number of visible (non disabled) installations.
func (s *reportStore) ActiveCount() int {
	s.mut.RLock()
	defer s.mut.RUnlock()
	var n int
	for _, e := range s.st.entries {
		if !e.Disabled && e.Report != nil {
			n++
		}
	}
	return n
}

// Lookup returns the current report for an installation and whether it is
// visible. Used by tests and diagnostics.
func (s *reportStore) Lookup(uid string) (*contract.Report, bool) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	e := s.st.entries[uid]
	if e == nil || e.Disabled || e.Report == nil {
		return nil, false
	}
	return e.Report, true
}

// IsDisabled reports whether the installation currently has an active
// tombstone.
func (s *reportStore) IsDisabled(uid string) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()
	e := s.st.entries[uid]
	return e != nil && e.Disabled
}

// CollectActive calls fn for every visible report with a received time at
// or after the cutoff. Visible reports older than the cutoff are pruned,
// matching the previous time based retention; tombstones are always
// retained so disabled installations cannot reappear.
func (s *reportStore) CollectActive(now time.Time, maxAge time.Duration, fn func(*contract.Report)) int {
	s.mut.Lock()
	defer s.mut.Unlock()

	cutoff := now.Add(-maxAge)
	for uid, e := range s.st.entries {
		if e.Disabled || e.Report == nil {
			continue
		}
		if e.Report.Received.Before(cutoff) {
			delete(s.st.entries, uid)
			continue
		}
		fn(e.Report)
	}
	return len(s.st.entries)
}

// Persist writes the full reconciliation state (including tombstones and
// provenance) durably and atomically. It is used by the periodic dumper
// for realtime reports that have accumulated since the last batch.
func (s *reportStore) Persist() error {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.writeStateLocked(s.st)
}

// WriteExport writes the visible report set (one JSON object per line,
// gzip compressed) atomically, sorted by installation ID for stable
// output. Disabled installations are excluded.
func (s *reportStore) WriteExport() error {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.exportLocked()
}

func (s *reportStore) writeStateLocked(st *storeState) error {
	data := persistedState{
		Format:   stateFormat,
		Versions: st.versions,
		Entries:  st.entries,
	}
	return writeGzipAtomic(s.statePath, func(w io.Writer) error {
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		return enc.Encode(data)
	})
}

func (s *reportStore) exportLocked() error {
	ids := make([]string, 0, len(s.st.entries))
	for uid, e := range s.st.entries {
		if !e.Disabled && e.Report != nil {
			ids = append(ids, uid)
		}
	}
	sort.Strings(ids)

	return writeGzipAtomic(s.exportPath, func(w io.Writer) error {
		bw := bufio.NewWriter(w)
		enc := json.NewEncoder(bw)
		for _, uid := range ids {
			if err := enc.Encode(s.st.entries[uid].Report); err != nil {
				return err
			}
		}
		return bw.Flush()
	})
}

// writeGzipAtomic streams gzip-compressed data to a temp file in the same
// directory, fsyncs and renames it into place. Readers therefore only ever
// see a complete file, and a failed write leaves the previous file alone.
func writeGzipAtomic(path string, write func(io.Writer) error) error {
	dir := filepath.Dir(path)
	fd, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := fd.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	gw := gzip.NewWriter(fd)
	if err := write(gw); err != nil {
		_ = gw.Close()
		_ = fd.Close()
		cleanup()
		return err
	}
	if err := gw.Close(); err != nil {
		_ = fd.Close()
		cleanup()
		return fmt.Errorf("close gzip: %w", err)
	}
	if err := fd.Sync(); err != nil {
		_ = fd.Close()
		cleanup()
		return fmt.Errorf("sync: %w", err)
	}
	if err := fd.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return fmt.Errorf("rename: %w", err)
	}
	syncDir(dir)
	return nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// LoadState reads a persisted reconciliation state. Unknown formats and
// malformed data are rejected; it never partially overwrites current
// contents.
func (s *reportStore) LoadState(r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("state gzip: %w", err)
	}
	bs, err := io.ReadAll(gr)
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	if err := gr.Close(); err != nil {
		return fmt.Errorf("state gzip trailer: %w", err)
	}

	var data persistedState
	if err := json.Unmarshal(bs, &data); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	if data.Format != stateFormat {
		return fmt.Errorf("unsupported state format %d", data.Format)
	}
	if data.Entries == nil {
		data.Entries = make(map[string]*storeEntry)
	}
	if data.Versions == nil {
		data.Versions = make(map[string]int64)
	}
	for _, e := range data.Entries {
		if e != nil && e.Report != nil {
			e.rankSum = reportSum(e.Report)
		}
	}

	s.mut.Lock()
	s.st = &storeState{versions: data.Versions, entries: data.Entries}
	s.mut.Unlock()
	return nil
}

// LoadLegacy bootstraps the store from a pre-reconciliation gzip JSON
// stream of reports (one object per line). Such records are treated as
// realtime observations at source version zero. A broken stream aborts the
// bootstrap without touching the store.
func (s *reportStore) LoadLegacy(r io.Reader) (int, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return 0, fmt.Errorf("dump gzip: %w", err)
	}

	type stagedRecord struct {
		uid string
		rep *contract.Report
	}
	var staged []stagedRecord

	dec := json.NewDecoder(gr)
	for {
		var rep contract.Report
		if err := dec.Decode(&rep); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return 0, fmt.Errorf("decode legacy record: %w", err)
		}
		if s.enrich != nil {
			s.enrich(&rep)
		}
		staged = append(staged, stagedRecord{rep.UniqueID, &rep})
	}
	if err := gr.Close(); err != nil {
		return 0, fmt.Errorf("dump gzip trailer: %w", err)
	}

	s.mut.Lock()
	for _, st := range staged {
		s.st.entries[st.uid] = &storeEntry{
			Report:  st.rep,
			Source:  liveSource,
			Version: 0,
			rankSum: reportSum(st.rep),
		}
	}
	n := len(staged)
	s.mut.Unlock()
	return n, nil
}

// sniffStateGzip reports whether gzip-compressed data is a reconciliation
// state file rather than a legacy JSON-lines export.
func sniffStateGzip(gzipped []byte) bool {
	gr, err := gzip.NewReader(bytes.NewReader(gzipped))
	if err != nil {
		return false
	}
	dec := json.NewDecoder(gr)
	var probe struct {
		Format int `json:"format"`
	}
	if err := dec.Decode(&probe); err != nil {
		return false
	}
	return probe.Format == stateFormat
}
