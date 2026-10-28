// Copyright © 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/url"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/geoip"
	"github.com/syncthing/syncthing/lib/osutil"
)

const (
	// requiredHealthChecks is the number of consecutive successful health
	// check reports a (new) address needs before it is published.
	requiredHealthChecks = 3

	generationHeader = "X-Relay-Pool-Generation"

	// expiryRetryInterval is how long an expiry eviction waits before
	// retrying a failed generation commit.
	expiryRetryInterval = 30 * time.Second
)

// errNoCommit aborts a commit without bumping the generation or touching
// disk, because the attempted transition does not change any state.
var errNoCommit = errors.New("no state change to commit")

// lease is the admission lease for a single relay identity. The identity is
// derived from the relay certificate (the "id" query parameter).
type lease struct {
	id string

	// activeURL is the currently published, canonical relay URL. The empty
	// string means the identity has never completed admission yet.
	activeURL string

	// pendingURL is a candidate address awaiting enough consecutive
	// successful health check reports, and pendingOK is the count.
	pendingURL string
	pendingOK  int

	// expiresAt is the lease deadline. Every successful report (including
	// for the pending address) extends it.
	expiresAt time.Time

	// active is the runtime representation of activeURL (stats, location).
	// It is not persisted.
	active *relay

	// incoming is the runtime entry delivered by the report currently
	// being committed. It becomes active at install time only when its URL
	// equals activeURL. It is not persisted.
	incoming *relay
}

type leaseRecord struct {
	ID         string    `json:"id"`
	ActiveURL  string    `json:"activeURL"`
	PendingURL string    `json:"pendingURL,omitempty"`
	PendingOK  int       `json:"pendingOK,omitempty"`
	ExpiresAt  time.Time `json:"expiresAt"`
}

type leaseLedger struct {
	Generation uint64        `json:"generation"`
	Leases     []leaseRecord `json:"leases"`
}

type publishedEntry struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type publishedFile struct {
	Generation uint64           `json:"generation"`
	Relays     []publishedEntry `json:"relays"`
}

// relayStore owns the admission leases and the published relay list. Both
// always reflect the same committed generation.
type relayStore struct {
	mu            sync.RWMutex
	leasesPath    string
	publishedPath string
	geoip         *geoip.Provider

	generation uint64
	leases     map[string]*lease
	published  []*relay // committed dynamic relays, sorted by identity
	timers     map[string]*time.Timer
}

func newRelayStore(leasesPath, publishedPath, legacyPath string, geoip *geoip.Provider) (*relayStore, error) {
	s := &relayStore{
		leasesPath:    leasesPath,
		publishedPath: publishedPath,
		geoip:         geoip,
		leases:        make(map[string]*lease),
		timers:        make(map[string]*time.Timer),
	}

	ledgerExisted := true
	ledger, err := loadLeasesFile(leasesPath)
	if errors.Is(err, os.ErrNotExist) {
		// First start with the new format: migrate the legacy
		// line-per-URL known relays file, if present.
		ledger = s.migrateLegacy(legacyPath)
		ledgerExisted = false
	} else if err != nil {
		return nil, err
	}

	now := time.Now()
	activeLeases := make(map[string]*lease, len(ledger.Leases))
	droppedExpired := 0
	for _, rec := range ledger.Leases {
		if !rec.ExpiresAt.IsZero() && !now.Before(rec.ExpiresAt) {
			droppedExpired++
			if debug {
				log.Println("Dropping expired lease for", rec.ID)
			}
			continue
		}
		l := &lease{
			id:         rec.ID,
			activeURL:  rec.ActiveURL,
			pendingURL: rec.PendingURL,
			pendingOK:  rec.PendingOK,
			expiresAt:  rec.ExpiresAt,
		}
		if l.activeURL != "" {
			l.active = s.buildRelay(l.activeURL)
		}
		activeLeases[rec.ID] = l
	}

	s.leases = activeLeases
	s.published = publishedRelays(activeLeases)
	s.generation = ledger.Generation
	if droppedExpired > 0 {
		s.generation++
	}

	// Make the durable state match. The lease ledger is the commit
	// marker; the published list may lag (crash between the two renames),
	// in which case it gets rebuilt from the ledger here.
	needLedgerWrite := !ledgerExisted || droppedExpired > 0
	if err := s.reconcileLocked(needLedgerWrite); err != nil {
		return nil, err
	}

	s.armTimersLocked()
	publishedGenerationGauge.Set(float64(s.generation))
	return s, nil
}

func (s *relayStore) migrateLegacy(legacyPath string) *leaseLedger {
	ledger := &leaseLedger{Generation: 0, Leases: nil}
	if _, err := os.Stat(legacyPath); err != nil {
		return ledger
	}
	relays := loadRelays(legacyPath, s.geoip)
	if len(relays) == 0 {
		return ledger
	}
	now := time.Now()
	ledger.Generation = 1
	for _, r := range relays {
		ledger.Leases = append(ledger.Leases, leaseRecord{
			ID:        identityForURI(r.uri),
			ActiveURL: r.uri.String(),
			ExpiresAt: now.Add(evictionTime),
		})
	}
	log.Printf("Migrated %d known relays into lease ledger", len(relays))
	return ledger
}

func (s *relayStore) buildRelay(rawURL string) *relay {
	uri, err := url.Parse(rawURL)
	if err != nil {
		log.Println("Skipping unparsable relay URL from lease ledger:", rawURL)
		return nil
	}
	return &relay{
		URL:      rawURL,
		uri:      uri,
		Location: getLocation(uri.Host, s.geoip),
	}
}

func (s *relayStore) armTimersLocked() {
	for id, l := range s.leases {
		d := time.Until(l.expiresAt)
		if d < 0 {
			d = 0
		}
		id := id
		s.timers[id] = time.AfterFunc(d, func() { s.expire(id) })
	}
}

// reconcileLocked rewrites the state files when the on-disk published list
// does not match the ledger, or when the ledger itself needs (re)writing.
// Callers must not hold s.mu during construction; this takes the lock.
func (s *relayStore) reconcileLocked(writeLedger bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	desired := publishedFile{
		Generation: s.generation,
		Relays:     publishedEntries(s.leases),
	}

	if !writeLedger {
		onDisk, err := loadPublishedFile(s.publishedPath)
		if err == nil && onDisk.Generation == desired.Generation && equalEntries(onDisk.Relays, desired.Relays) {
			return nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Println("Failed to read published relay list, rebuilding:", err)
		}
	}

	// Same ordering as a regular commit: list first, ledger last.
	if err := writeJSONAtomic(s.publishedPath, desired); err != nil {
		return err
	}
	if writeLedger {
		if err := writeJSONAtomic(s.leasesPath, leaseLedger{
			Generation: s.generation,
			Leases:     leaseRecords(s.leases),
		}); err != nil {
			return err
		}
	}
	return nil
}

// commit runs apply against a copy of the lease state and durably commits
// the result as a new generation. The published list is written first and
// the lease ledger is renamed last as the commit marker; if either write
// fails, in-memory state is left completely untouched.
func (s *relayStore) commit(apply func(next map[string]*lease) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := make(map[string]*lease, len(s.leases))
	for id, l := range s.leases {
		cp := *l
		next[id] = &cp
	}

	if err := apply(next); err != nil {
		if errors.Is(err, errNoCommit) {
			return nil
		}
		return err
	}

	gen := s.generation + 1

	pub := publishedFile{
		Generation: gen,
		Relays:     publishedEntries(next),
	}
	if err := writeJSONAtomic(s.publishedPath, pub); err != nil {
		return err
	}
	ledger := leaseLedger{
		Generation: gen,
		Leases:     leaseRecords(next),
	}
	if err := writeJSONAtomic(s.leasesPath, ledger); err != nil {
		// The list rename landed but the commit marker did not. Roll the
		// list back; startup reconciliation would do the same.
		rollback := publishedFile{
			Generation: s.generation,
			Relays:     publishedEntries(s.leases),
		}
		if err := writeJSONAtomic(s.publishedPath, rollback); err != nil {
			log.Println("Failed to roll back published relay list after ledger write failure:", err)
		}
		return err
	}

	s.installLocked(next, gen)
	return nil
}

// installLocked swaps the committed state into memory, reconciles expiry
// timers and per-host metrics, and advances the generation. It is called
// only after both state files have been durably written.
func (s *relayStore) installLocked(next map[string]*lease, gen uint64) {
	oldHosts := make(map[string]struct{}, len(s.published))
	for _, r := range s.published {
		oldHosts[r.uri.Host] = struct{}{}
	}

	// Timers: stop those for removed leases and those whose deadline moved.
	for id, t := range s.timers {
		oldLease, okOld := s.leases[id]
		newLease, okNew := next[id]
		if !okNew || !okOld || !newLease.expiresAt.Equal(oldLease.expiresAt) {
			t.Stop()
			delete(s.timers, id)
		}
	}

	s.leases = next
	s.generation = gen

	// Apply the runtime entries of this report only where the committed
	// URL says they belong. A pending candidate must never replace the
	// active entry.
	for _, l := range next {
		if l.incoming != nil && l.incoming.URL == l.activeURL {
			l.active = l.incoming
		}
		l.incoming = nil
	}
	s.published = publishedRelays(next)

	// Start timers that are missing after the reconciliation above.
	for id, l := range next {
		if _, ok := s.timers[id]; ok {
			continue
		}
		d := time.Until(l.expiresAt)
		if d < 0 {
			d = 0
		}
		id := id
		s.timers[id] = time.AfterFunc(d, func() { s.expire(id) })
	}

	// Metric labels follow the address switch in the same critical
	// section, so metrics can never reference a non-published address.
	newHosts := make(map[string]struct{}, len(s.published))
	publishedByHost := make(map[string]*relay, len(s.published))
	for _, r := range s.published {
		newHosts[r.uri.Host] = struct{}{}
		publishedByHost[r.uri.Host] = r
	}
	for host := range oldHosts {
		if _, ok := newHosts[host]; !ok {
			deleteMetrics(host)
		}
	}
	for host := range newHosts {
		if _, ok := oldHosts[host]; ok {
			continue
		}
		if r := publishedByHost[host]; r != nil && r.Stats != nil {
			updateMetrics(host, *r.Stats, r.Location)
		}
	}

	publishedGenerationGauge.Set(float64(gen))
}

// admit records a successful health check report for an identity.
//
// The reported address first becomes (or refreshes) a pending candidate.
// It atomically replaces the active address only after requiredHealthChecks
// consecutive successful reports; every successful report extends the
// lease deadline regardless of pending/active state.
func (s *relayStore) admit(id string, r *relay, eviction time.Duration) error {
	return s.commit(func(next map[string]*lease) error {
		now := time.Now()
		l, ok := next[id]
		if !ok {
			next[id] = &lease{
				id:         id,
				pendingURL: r.URL,
				pendingOK:  1,
				expiresAt:  now.Add(eviction),
				incoming:   r,
			}
			if debug {
				log.Println("New identity", id, "entering pending state at", r.URL)
			}
			return nil
		}

		l.expiresAt = now.Add(eviction)
		l.incoming = r

		switch {
		case l.activeURL == r.URL:
			// Heartbeat for the published address; any candidate switch
			// is abandoned.
			l.pendingURL = ""
			l.pendingOK = 0
			if debug {
				log.Println("Renewed lease for", id, "at", r.URL)
			}
		case l.pendingURL == r.URL:
			l.pendingOK++
			if l.pendingOK >= requiredHealthChecks {
				if debug {
					log.Println("Promoting", id, "to published at", r.URL, "replacing", l.activeURL)
				}
				l.activeURL = r.URL
				l.pendingURL = ""
				l.pendingOK = 0
			} else if debug {
				log.Println("Pending address", r.URL, "for", id, "passed", l.pendingOK, "of", requiredHealthChecks, "checks")
			}
		default:
			if debug {
				log.Println("Identity", id, "got a new candidate address", r.URL, "replacing pending", l.pendingURL)
			}
			l.pendingURL = r.URL
			l.pendingOK = 1
		}
		return nil
	})
}

// reportFailure records a failed health check report: the consecutive
// success counter for the reported candidate address resets to zero. The
// active address is never touched.
func (s *relayStore) reportFailure(id, rawURL string) error {
	return s.commit(func(next map[string]*lease) error {
		l, ok := next[id]
		if !ok || l.pendingURL != rawURL || l.pendingOK == 0 {
			return errNoCommit
		}
		if debug {
			log.Println("Health check for pending address", rawURL, "of", id, "failed, resetting counter")
		}
		l.pendingOK = 0
		return nil
	})
}

// revoke explicitly removes an identity's lease. The identity goes offline
// immediately as one committed generation; re-joining later requires a full
// new admission.
func (s *relayStore) revoke(id string) (bool, error) {
	existed := false
	err := s.commit(func(next map[string]*lease) error {
		if _, ok := next[id]; !ok {
			return errNoCommit
		}
		existed = true
		delete(next, id)
		return nil
	})
	return existed, err
}

// expire is the lease deadline callback. It removes the lease only if it
// still exists and is actually due, and retries durable commits until they
// land, so an expiry can never be silently lost.
func (s *relayStore) expire(id string) {
	for {
		err := s.commit(func(next map[string]*lease) error {
			l, ok := next[id]
			if !ok || time.Now().Before(l.expiresAt) {
				return errNoCommit
			}
			if debug {
				log.Println("Lease for", id, "expired, evicting")
			}
			delete(next, id)
			return nil
		})
		if err == nil {
			return
		}
		log.Println("Failed to commit lease expiry for", id, ":", err)
		time.Sleep(expiryRetryInterval)
	}
}

// stop cancels all lease timers. It is used by tests.
func (s *relayStore) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.timers {
		t.Stop()
	}
	s.timers = map[string]*time.Timer{}
}

// snapshot returns the current generation and a copy of the committed
// dynamic relay list.
func (s *relayStore) snapshot() (uint64, []*relay) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*relay, len(s.published))
	copy(out, s.published)
	return s.generation, out
}

// allRelays returns permanent relays followed by the committed dynamic
// relays. Stats refresh operates on this set only; pending addresses are
// never scraped.
func (s *relayStore) allRelays() []*relay {
	s.mu.RLock()
	defer s.mu.RUnlock()
	relays := make([]*relay, 0, len(permanentRelays)+len(s.published))
	relays = append(relays, permanentRelays...)
	relays = append(relays, s.published...)
	return relays
}

func identityForURI(uri *url.URL) string {
	if id := uri.Query().Get("id"); id != "" {
		return id
	}
	return "host:" + uri.Host
}

func sortedLeaseIDs(leases map[string]*lease) []string {
	ids := make([]string, 0, len(leases))
	for id := range leases {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func publishedEntries(leases map[string]*lease) []publishedEntry {
	entries := make([]publishedEntry, 0, len(leases))
	for _, id := range sortedLeaseIDs(leases) {
		if url := leases[id].activeURL; url != "" {
			entries = append(entries, publishedEntry{ID: id, URL: url})
		}
	}
	return entries
}

func publishedRelays(leases map[string]*lease) []*relay {
	out := make([]*relay, 0, len(leases))
	for _, id := range sortedLeaseIDs(leases) {
		if r := leases[id].active; r != nil {
			out = append(out, r)
		}
	}
	return out
}

func leaseRecords(leases map[string]*lease) []leaseRecord {
	records := make([]leaseRecord, 0, len(leases))
	for _, id := range sortedLeaseIDs(leases) {
		l := leases[id]
		records = append(records, leaseRecord{
			ID:         l.id,
			ActiveURL:  l.activeURL,
			PendingURL: l.pendingURL,
			PendingOK:  l.pendingOK,
			ExpiresAt:  l.expiresAt,
		})
	}
	return records
}

func equalEntries(a, b []publishedEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func loadLeasesFile(path string) (*leaseLedger, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var lf leaseLedger
	if err := json.Unmarshal(bs, &lf); err != nil {
		return nil, err
	}
	return &lf, nil
}

func loadPublishedFile(path string) (*publishedFile, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pf publishedFile
	if err := json.Unmarshal(bs, &pf); err != nil {
		return nil, err
	}
	return &pf, nil
}

func writeJSONAtomic(path string, v any) error {
	fd, err := osutil.CreateAtomic(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(fd)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fd.Close()
		return err
	}
	return fd.Close()
}
