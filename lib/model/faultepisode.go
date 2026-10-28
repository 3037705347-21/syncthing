// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
)

// faultEpisodeKVPrefix is the KV namespace holding the persistent fault
// episode ledgers. The v1 suffix allows for incompatible format changes in
// the future.
const faultEpisodeKVPrefix = "faults/v1"

// maxResolvedFaultEpisodes bounds how many closed episodes are kept per
// folder. Open episodes are never pruned.
const maxResolvedFaultEpisodes = 1000

const (
	// A fault was observed while scanning the folder.
	faultSourceScan = "scan"
	// A fault was observed while pulling (syncing) items.
	faultSourcePull = "pull"
)

const (
	// faultResolutionProcessed means the affected item was actually
	// processed successfully after the fault was recorded.
	faultResolutionProcessed = "processed"
	// faultResolutionValidated means a whole scan/pull batch completed
	// without the fault recurring.
	faultResolutionValidated = "validated"
)

// FaultEpisode is a single persistent fault record. An episode covers one
// path with one kind of error from its first sighting until it is resolved
// or acknowledged and later resolved. Repeated sightings while the episode
// stays open update LastSeen/Occurrences instead of creating new records;
// the same fault recurring after resolution starts a new episode, leaving
// the old one in history.
type FaultEpisode struct {
	ID    string `json:"id"`
	Path  string `json:"path"`
	Error string `json:"error"`
	// Source is either "scan" or "pull".
	Source string `json:"source"`

	FirstSeen   time.Time `json:"firstSeen"`
	LastSeen    time.Time `json:"lastSeen"`
	Occurrences int64     `json:"occurrences"`

	// Acknowledged episodes are open but muted: they are excluded from the
	// default listing until they are resolved.
	Acknowledged   bool      `json:"acknowledged"`
	AcknowledgedAt time.Time `json:"acknowledgedAt,omitempty"`

	// Resolved episodes are closed and only returned as history.
	Resolved   bool      `json:"resolved"`
	ResolvedAt time.Time `json:"resolvedAt,omitempty"`
	// Resolution is "processed" or "validated".
	Resolution string `json:"resolution,omitempty"`
}

// FaultEpisodeFilter controls which episodes are returned. The default
// (zero value) returns only open, unacknowledged episodes.
type FaultEpisodeFilter struct {
	IncludeAcknowledged bool
	IncludeResolved     bool
}

// FaultEpisodesEventData is emitted whenever the persistent fault state
// for a folder changes.
type FaultEpisodesEventData struct {
	Folder       string `json:"folder"`
	Active       int    `json:"active"`
	Acknowledged int    `json:"acknowledged"`
	Resolved     int    `json:"resolved"`
}

// faultObservation is a fault sighting during a scan or pull.
type faultObservation struct {
	source string
	path   string
	errMsg string
}

// faultResolution carries the evidence that allows open episodes to be
// closed. No field set means no recovery evidence is available, in which
// case observed episodes are merely recorded and everything else stays
// open.
type faultResolution struct {
	// scanValidated marks a successfully completed scan covering
	// scanSubdirs (empty means the whole folder) as recovery evidence.
	scanValidated bool
	scanSubdirs   []string
	// pullValidated marks a pull that completed without any error as
	// recovery evidence.
	pullValidated bool
	// pullProcessed lists item paths that were actually pulled
	// successfully, providing per-item recovery evidence.
	pullProcessed map[string]struct{}
}

func faultIdentity(source, path, errMsg string) string {
	return source + "\x00" + path + "\x00" + errMsg
}

// faultCovers reports whether a validated scan of subDirs (empty means the
// whole folder) provides recovery evidence for path.
func faultCovers(subDirs []string, path string) bool {
	if len(subDirs) == 0 {
		return true
	}
	for _, sub := range subDirs {
		if path == sub || fs.IsParent(path, sub) {
			return true
		}
	}
	return false
}

// faultEpisodeRegistry hands out per-folder episode stores.
type faultEpisodeRegistry struct {
	kv       db.KV
	evLogger events.Logger

	mut    sync.Mutex
	stores map[string]*faultEpisodeStore
}

func newFaultEpisodeRegistry(kv db.KV, evLogger events.Logger) *faultEpisodeRegistry {
	return &faultEpisodeRegistry{
		kv:       kv,
		evLogger: evLogger,
		stores:   make(map[string]*faultEpisodeStore),
	}
}

func (r *faultEpisodeRegistry) store(folder string) *faultEpisodeStore {
	r.mut.Lock()
	defer r.mut.Unlock()
	s, ok := r.stores[folder]
	if !ok {
		s = &faultEpisodeStore{
			reg:    r,
			folder: folder,
			kv:     db.NewTyped(r.kv, faultEpisodeKVPrefix),
		}
		r.stores[folder] = s
	}
	return s
}

// faultEpisodeStore persists all episodes of one folder as a single JSON
// blob. Every state change is therefore written with one KV put, which is
// a single atomic database statement: a crash can never leave half a
// record behind, and a failed write fails the whole update without
// touching the previously committed state.
type faultEpisodeStore struct {
	reg    *faultEpisodeRegistry
	folder string
	kv     *db.Typed

	mut    sync.Mutex
	loaded bool
	eps    []FaultEpisode
}

type faultEpisodeFile struct {
	Episodes []FaultEpisode `json:"episodes"`
}

type faultEpisodeCounts struct {
	active       int
	acknowledged int
	resolved     int
}

func (s *faultEpisodeStore) loadLocked() error {
	if s.loaded {
		return nil
	}
	bs, ok, err := s.kv.Bytes(s.folder)
	if err != nil {
		return fmt.Errorf("load fault episodes for %q: %w", s.folder, err)
	}
	if ok {
		var data faultEpisodeFile
		if err := json.Unmarshal(bs, &data); err != nil {
			return fmt.Errorf("parse fault episodes for %q: %w", s.folder, err)
		}
		s.eps = data.Episodes
	}
	s.loaded = true
	return nil
}

// commit runs fn against a working copy of the episodes, then persists the
// result atomically. The cached state is only replaced after the write
// succeeds, so any error leaves the old state fully intact.
func (s *faultEpisodeStore) commit(fn func(eps []FaultEpisode, now time.Time) ([]FaultEpisode, bool)) (bool, faultEpisodeCounts, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if err := s.loadLocked(); err != nil {
		return false, faultEpisodeCounts{}, err
	}

	now := time.Now()
	next, changed := fn(append([]FaultEpisode(nil), s.eps...), now)
	if !changed {
		return false, countFaultEpisodes(s.eps), nil
	}
	next = pruneResolvedEpisodes(next)

	bs, err := json.Marshal(faultEpisodeFile{Episodes: next})
	if err != nil {
		return false, faultEpisodeCounts{}, fmt.Errorf("encode fault episodes for %q: %w", s.folder, err)
	}
	if err := s.kv.PutBytes(s.folder, bs); err != nil {
		return false, faultEpisodeCounts{}, fmt.Errorf("persist fault episodes for %q: %w", s.folder, err)
	}

	s.eps = next
	return true, countFaultEpisodes(next), nil
}

// changedAndLogged runs a commit and, when it changed anything, emits the
// change event.
func (s *faultEpisodeStore) changedAndLogged(fn func(eps []FaultEpisode, now time.Time) ([]FaultEpisode, bool)) error {
	changed, counts, err := s.commit(fn)
	if err != nil {
		return err
	}
	if changed && s.reg.evLogger != nil {
		s.reg.evLogger.Log(events.FaultEpisodes, FaultEpisodesEventData{
			Folder:       s.folder,
			Active:       counts.active,
			Acknowledged: counts.acknowledged,
			Resolved:     counts.resolved,
		})
	}
	return nil
}

// apply records observations and closes episodes according to the recovery
// evidence. The whole update is atomic.
func (s *faultEpisodeStore) apply(observations []faultObservation, res faultResolution) error {
	return s.changedAndLogged(func(eps []FaultEpisode, now time.Time) ([]FaultEpisode, bool) {
		open := make(map[string]int)
		for i, ep := range eps {
			if !ep.Resolved {
				open[faultIdentity(ep.Source, ep.Path, ep.Error)] = i
			}
		}

		seen := make(map[string]struct{}, len(observations))
		bumped := make(map[string]struct{})
		changed := false

		for _, ob := range observations {
			id := faultIdentity(ob.source, ob.path, ob.errMsg)
			seen[id] = struct{}{}

			if i, ok := open[id]; ok {
				// Same path with the same kind of error while the episode
				// is open: do not stack duplicates, just extend it.
				if _, done := bumped[id]; !done {
					eps[i].Occurrences++
					eps[i].LastSeen = now
					bumped[id] = struct{}{}
					changed = true
				}
				continue
			}

			ep := FaultEpisode{
				ID:          newFaultEpisodeID(eps),
				Path:        ob.path,
				Error:       ob.errMsg,
				Source:      ob.source,
				FirstSeen:   now,
				LastSeen:    now,
				Occurrences: 1,
			}
			eps = append(eps, ep)
			open[id] = len(eps) - 1
			changed = true
		}

		// Close episodes only when there is positive evidence that the
		// item was handled or a full batch validated. Anything observed
		// again in this run stays open.
		for i := range eps {
			ep := &eps[i]
			if ep.Resolved {
				continue
			}
			if _, observed := seen[faultIdentity(ep.Source, ep.Path, ep.Error)]; observed {
				continue
			}

			switch {
			case ep.Source == faultSourceScan && res.scanValidated && faultCovers(res.scanSubdirs, ep.Path):
				ep.Resolved = true
				ep.ResolvedAt = now
				ep.Resolution = faultResolutionValidated
				changed = true
			case ep.Source == faultSourcePull && res.pullValidated:
				ep.Resolved = true
				ep.ResolvedAt = now
				ep.Resolution = faultResolutionValidated
				changed = true
			case ep.Source == faultSourcePull && len(res.pullProcessed) > 0:
				if _, ok := res.pullProcessed[ep.Path]; ok {
					ep.Resolved = true
					ep.ResolvedAt = now
					ep.Resolution = faultResolutionProcessed
					changed = true
				}
			}
		}

		return eps, changed
	})
}

// acknowledge marks the given open episodes (by ID) as acknowledged. An
// empty ids slice acknowledges every open episode. Unknown or already
// closed IDs are ignored so the operation is idempotent.
func (s *faultEpisodeStore) acknowledge(ids []string) error {
	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}

	return s.changedAndLogged(func(eps []FaultEpisode, now time.Time) ([]FaultEpisode, bool) {
		changed := false
		for i := range eps {
			ep := &eps[i]
			if ep.Resolved || ep.Acknowledged {
				continue
			}
			if len(want) > 0 {
				if _, ok := want[ep.ID]; !ok {
					continue
				}
			}
			ep.Acknowledged = true
			ep.AcknowledgedAt = now
			changed = true
		}
		return eps, changed
	})
}

// list returns episodes matching the filter, in a deterministic order:
// open episodes first (by path), resolved history afterwards (most
// recently resolved first).
func (s *faultEpisodeStore) list(filter FaultEpisodeFilter) ([]FaultEpisode, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if err := s.loadLocked(); err != nil {
		return nil, err
	}

	eps := make([]FaultEpisode, 0, len(s.eps))
	for _, ep := range s.eps {
		switch {
		case !ep.Resolved && !ep.Acknowledged:
			eps = append(eps, ep)
		case !ep.Resolved && ep.Acknowledged && filter.IncludeAcknowledged:
			eps = append(eps, ep)
		case ep.Resolved && filter.IncludeResolved:
			eps = append(eps, ep)
		}
	}

	slices.SortFunc(eps, func(a, b FaultEpisode) int {
		if a.Resolved != b.Resolved {
			if a.Resolved {
				return 1
			}
			return -1
		}
		if a.Resolved {
			if c := b.ResolvedAt.Compare(a.ResolvedAt); c != 0 {
				return c
			}
		} else if c := strings.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		return b.FirstSeen.Compare(a.FirstSeen)
	})
	return eps, nil
}

// activeCount returns the number of open, unacknowledged episodes.
func (s *faultEpisodeStore) activeCount() (int, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if err := s.loadLocked(); err != nil {
		return 0, err
	}
	counts := countFaultEpisodes(s.eps)
	return counts.active, nil
}

func countFaultEpisodes(eps []FaultEpisode) faultEpisodeCounts {
	var c faultEpisodeCounts
	for _, ep := range eps {
		switch {
		case ep.Resolved:
			c.resolved++
		case ep.Acknowledged:
			c.acknowledged++
		default:
			c.active++
		}
	}
	return c
}

// pruneResolvedEpisodes keeps only the newest maxResolvedFaultEpisodes
// resolved episodes while retaining every open one.
func pruneResolvedEpisodes(eps []FaultEpisode) []FaultEpisode {
	resolvedIdx := make([]int, 0)
	for i, ep := range eps {
		if ep.Resolved {
			resolvedIdx = append(resolvedIdx, i)
		}
	}
	excess := len(resolvedIdx) - maxResolvedFaultEpisodes
	if excess <= 0 {
		return eps
	}

	// Oldest resolved episodes first.
	slices.SortFunc(resolvedIdx, func(a, b int) int {
		return eps[a].ResolvedAt.Compare(eps[b].ResolvedAt)
	})

	drop := make(map[int]struct{}, excess)
	for _, idx := range resolvedIdx[:excess] {
		drop[idx] = struct{}{}
	}

	out := make([]FaultEpisode, 0, len(eps)-excess)
	for i, ep := range eps {
		if _, ok := drop[i]; !ok {
			out = append(out, ep)
		}
	}
	return out
}

func newFaultEpisodeID(existing []FaultEpisode) string {
	for {
		var b [8]byte
		_, _ = rand.Read(b[:])
		id := hex.EncodeToString(b[:])
		if !slices.ContainsFunc(existing, func(ep FaultEpisode) bool { return ep.ID == id }) {
			return id
		}
	}
}
