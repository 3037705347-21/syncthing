// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package versioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/osutil"
)

// The "managed" versioner performs retention strictly according to an
// externally maintained retention manifest. The manifest is the sole
// authority over which versions are "managed": cleanup removes exactly the
// versions the manifest declares expired, never touches versions declared
// retained and leaves every file that is not listed in the manifest alone,
// even if it looks like an old version.
//
// Any mismatch between the manifest and the versions directory, a malformed
// manifest or an interrupted batch aborts the whole cleanup before anything
// is deleted. A small journal (pending file) plus a tombstone ledger (state
// file) make interrupted batches resumable, so rerunning cleanup converges
// the versions directory to exactly the conclusions recorded in the
// manifest.

func init() {
	// Register the constructor for this type of versioner with the name "managed".
	factories["managed"] = newManaged
}

const (
	managedSchemaVersion = 1

	// defaultManifestName is the manifest file name, relative to the
	// versions directory, when the "manifest" parameter is not set.
	defaultManifestName = "retention.json"

	// managedStateName holds the ledger of versions that were removed by
	// previous cleanup runs. It lives in the versions directory and is
	// never treated as a version itself.
	managedStateName = ".stmanaged-state.json"

	// managedPendingName is the journal of a cleanup batch in flight.
	managedPendingName = ".stmanaged-cleanup.pending"

	decisionRetain = "retain"
	decisionExpire = "expire"

	// managedMaxGenerations bounds how many batch generations a single
	// Clean call may run (an interrupted generation plus at most one
	// further generation for decisions that appeared meanwhile).
	managedMaxGenerations = 10
)

// errRetentionConflict marks any condition that makes a cleanup batch
// unsafe to execute: malformed data, manifest/disk drift or an interrupted
// batch whose conclusions can no longer be guaranteed. Cleanup stops
// without deleting anything (or without deleting anything further) when it
// sees this error.
var errRetentionConflict = errors.New("retention manifest conflict")

func conflictf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errRetentionConflict, fmt.Sprintf(format, args...))
}

// managedManifestFile is the on-disk manifest format. The file is
// maintained outside Syncthing and is only ever read by the versioner,
// except that completed "expire" entries are consumed (see managedState).
type managedManifestFile struct {
	Version int            `json:"version"`
	Entries []managedEntry `json:"entries"`
}

type managedEntry struct {
	// Path is the path of the original file, relative to the folder root,
	// using slash separators.
	Path string `json:"path"`
	// VersionTime is the version timestamp, either in the versioner tag
	// format ("20060102-150405", local time) or RFC3339.
	VersionTime string `json:"versionTime"`
	// Decision is "retain" (must be kept) or "expire" (approved for
	// removal during cleanup).
	Decision string `json:"decision"`
}

// managedVersionRef identifies a single version by original path and
// normalized version tag.
type managedVersionRef struct {
	Path        string `json:"path"`
	VersionTime string `json:"versionTime"`
}

func (r managedVersionRef) key() string {
	return r.Path + "\x00" + r.VersionTime
}

// target returns the path of the tagged file representing this version
// inside the versions directory.
func (r managedVersionRef) target() string {
	return TagFilename(filepath.FromSlash(r.Path), r.VersionTime)
}

type validatedEntry struct {
	managedVersionRef
	decision string
}

// managedState is the tombstone ledger: versions that earlier cleanup
// batches removed. It makes reruns idempotent even though the manifest
// itself is not rewritten.
type managedState struct {
	Version int                 `json:"version"`
	Removed []managedVersionRef `json:"removed"`
}

// managedPending is the batch journal written before any deletion.
type managedPending struct {
	Version      int                 `json:"version"`
	ManifestHash string              `json:"manifestHash"`
	Plan         []managedVersionRef `json:"plan"`
}

type managed struct {
	folderFs        fs.Filesystem
	versionsFs      fs.Filesystem
	manifestName    string
	copyRangeMethod fs.CopyRangeMethod

	// mu serializes Clean calls within this process. Cross-process
	// serialization is provided by the exclusively created pending file.
	mu sync.Mutex
}

func newManaged(cfg config.FolderConfiguration) Versioner {
	manifestName := cfg.Versioning.Params["manifest"]
	if manifestName == "" {
		manifestName = defaultManifestName
	}

	m := &managed{
		folderFs:        cfg.Filesystem(),
		versionsFs:      versionerFsFromFolderCfg(cfg),
		manifestName:    manifestName,
		copyRangeMethod: cfg.CopyRangeMethod.ToFS(),
	}

	l.Debugf("instantiated %#v", m)
	return m
}

// Archive moves the named file away to the version archive. It never
// touches the manifest or removes anything: newly archived versions are
// unmanaged until the manifest lists them, so archiving can never delete
// manually retained versions.
func (m *managed) Archive(filePath string) error {
	return archiveFile(m.copyRangeMethod, m.folderFs, m.versionsFs, filePath, TagFilename)
}

func (m *managed) GetVersions() (map[string][]FileVersion, error) {
	return retrieveTaggedVersions(m.versionsFs)
}

func (m *managed) Restore(filePath string, versionTime time.Time) error {
	return restoreFile(m.copyRangeMethod, m.versionsFs, m.folderFs, filePath, versionTime, TagFilename)
}

func (m *managed) String() string {
	return fmt.Sprintf("Managed/%p", m)
}

// retrieveTaggedVersions lists only files carrying a parseable version
// tag. Files without a tag, including the retention manifest and the
// bookkeeping files as well as anything placed into the versions directory
// by hand, are not versions and stay invisible here even though cleanup
// leaves them alone.
func retrieveTaggedVersions(fileSystem fs.Filesystem) (map[string][]FileVersion, error) {
	files := make(map[string][]FileVersion)

	err := fileSystem.Walk(".", func(path string, f fs.FileInfo, err error) error {
		if path == "." {
			return nil
		}
		if err != nil {
			return err
		}
		if f.IsSymlink() {
			return fs.SkipDir
		}
		if f.IsDir() {
			return nil
		}

		name, tag := UntagFilename(path)
		if name == "" || tag == "" {
			return nil
		}
		versionTime, err := time.ParseInLocation(TimeFormat, tag, time.Local)
		if err != nil {
			return nil
		}

		modTime := f.ModTime().Truncate(time.Second)
		name = osutil.NormalizedFilename(name)
		files[name] = append(files[name], FileVersion{
			VersionTime: versionTime,
			ModTime:     modTime,
			Size:        f.Size(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// Clean executes one manifest-driven cleanup run. It is fail-closed and
// resumable: it deletes exactly the versions declared expired, stops the
// whole batch on any conflict or interruption, and converges to the
// manifest's conclusions when run again.
func (m *managed) Clean(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := m.versionsFs.Stat("."); fs.IsNotExist(err) {
		// Nothing to clean in a nonexistent versions directory.
		return nil
	} else if err != nil {
		return err
	}

	var dirs emptyDirTracker

	for generation := 0; ; generation++ {
		if generation >= managedMaxGenerations {
			return conflictf("cleanup did not converge after %d batch generations", managedMaxGenerations)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pending, hasPending, err := m.loadPending()
		if err != nil {
			return err
		}

		rawManifest, hasManifest, err := readSmallFile(m.versionsFs, m.manifestName)
		if err != nil {
			return err
		}
		if !hasManifest {
			if hasPending {
				return conflictf("cleanup was interrupted but the retention manifest %q is gone", m.manifestName)
			}
			// No manifest means nothing is managed: leave everything alone.
			return nil
		}
		manifestHash := hashBytes(rawManifest)

		entries, err := parseManifest(rawManifest)
		if err != nil {
			return err
		}

		state, err := m.loadState()
		if err != nil {
			return err
		}
		ledger := newRefSet(state.Removed)

		present, tracker, err := m.scanVersions(ctx)
		if err != nil {
			return err
		}
		dirs = tracker

		if hasPending {
			// Resuming an interrupted batch.
			if pending.Version != managedSchemaVersion {
				return conflictf("unsupported cleanup journal version %d", pending.Version)
			}
			if pending.ManifestHash != manifestHash {
				// The manifest changed while a batch was in flight. The
				// journal may only be discarded as stale when none of its
				// approved deletions happened yet; otherwise we cannot
				// prove that the new manifest agrees with what was done.
				for _, ref := range pending.Plan {
					if _, ok := present[ref.key()]; !ok {
						return conflictf("retention manifest changed during an interrupted cleanup that already removed %q (%s); reconcile the manifest with the versions directory and remove %q to retry", ref.Path, ref.VersionTime, managedPendingName)
					}
				}
				l.Debugln("Managed versioner: discarding stale journal without deletions")
				if err := m.versionsFs.Remove(managedPendingName); err != nil && !fs.IsNotExist(err) {
					return err
				}
				continue
			}

			pendingSet := newRefSet(pending.Plan)
			if err := validateAgainstDisk(entries, present, ledger, pendingSet); err != nil {
				return err
			}

			// Resume with the journaled plan, in journal order, skipping
			// files that were removed before the interruption.
			plan := make([]managedVersionRef, 0, len(pending.Plan))
			for _, ref := range pending.Plan {
				if _, ok := present[ref.key()]; ok {
					plan = append(plan, ref)
				}
			}

			if err := m.removeFiles(ctx, plan); err != nil {
				return err
			}
			if err := m.commitLedger(state.Removed, pending.Plan, entries); err != nil {
				return err
			}
			if err := m.versionsFs.Remove(managedPendingName); err != nil && !fs.IsNotExist(err) {
				return err
			}
			l.Debugf("Managed versioner: resumed batch complete (%d versions)", len(pending.Plan))
			// Another generation may be needed for expire decisions that
			// appeared while the batch was interrupted.
			continue
		}

		// Starting a fresh batch.
		var plan []managedVersionRef
		for _, entry := range entries {
			_, exists := present[entry.key()]
			switch entry.decision {
			case decisionRetain:
				if !exists {
					return conflictf("retained version %q (%s) is missing from the versions directory", entry.Path, entry.VersionTime)
				}
			case decisionExpire:
				if exists {
					plan = append(plan, entry.managedVersionRef)
				} else if !ledger.has(entry.managedVersionRef) {
					return conflictf("expired version %q (%s) is listed in the manifest but missing from the versions directory; add it back or remove the manifest entry", entry.Path, entry.VersionTime)
				}
			}
		}

		if len(plan) == 0 {
			// Versions directory and manifest are already in agreement.
			// Make sure the ledger agrees with the current manifest as
			// well (entries may have been removed since the last batch).
			if err := m.commitLedger(state.Removed, nil, entries); err != nil {
				return err
			}
			break
		}
		sortRefs(plan)

		pend := managedPending{
			Version:      managedSchemaVersion,
			ManifestHash: manifestHash,
			Plan:         plan,
		}
		if err := m.createPending(pend); err != nil {
			return err
		}

		// The manifest may have been edited between the read above and the
		// journal creation; recheck before deleting anything.
		rawAgain, stillExists, err := readSmallFile(m.versionsFs, m.manifestName)
		if err != nil {
			return err
		}
		if !stillExists || hashBytes(rawAgain) != manifestHash {
			_ = m.versionsFs.Remove(managedPendingName)
			return conflictf("retention manifest %q changed while cleanup was starting; rerun cleanup to retry", m.manifestName)
		}

		if err := m.removeFiles(ctx, plan); err != nil {
			return err
		}
		if err := m.commitLedger(state.Removed, plan, entries); err != nil {
			return err
		}
		if err := m.versionsFs.Remove(managedPendingName); err != nil && !fs.IsNotExist(err) {
			return err
		}
		l.Debugf("Managed versioner: removed %d expired versions", len(plan))

		// Re-run to verify convergence and pick up any newly appeared work.
	}

	dirs.deleteEmptyDirs(m.versionsFs)
	l.Debugln("Managed versioner: cleanup converged")
	return nil
}

// validateAgainstDisk checks every manifest entry against what is on disk.
// A missing expired version is acceptable when it is covered by the
// tombstone ledger (removed by an earlier completed batch) or by the
// pending journal (removed during the batch being resumed).
func validateAgainstDisk(entries []validatedEntry, present refSet, ledger, pending refSet) error {
	for _, entry := range entries {
		_, exists := present[entry.key()]
		if exists {
			continue
		}
		switch entry.decision {
		case decisionRetain:
			return conflictf("retained version %q (%s) is missing from the versions directory", entry.Path, entry.VersionTime)
		case decisionExpire:
			if !ledger.has(entry.managedVersionRef) && !pending.has(entry.managedVersionRef) {
				return conflictf("expired version %q (%s) is listed in the manifest but missing from the versions directory; add it back or remove the manifest entry", entry.Path, entry.VersionTime)
			}
		}
	}
	return nil
}

// removeFiles deletes the given versions. It is journal-bound: every file
// is part of a pending batch, so a file that vanished (for example removed
// just before a crash) is not an error. Any other error or cancellation
// aborts the whole batch and leaves the journal in place for resume.
func (m *managed) removeFiles(ctx context.Context, plan []managedVersionRef) error {
	for _, ref := range plan {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		target := ref.target()
		if err := m.versionsFs.Remove(target); err != nil && !fs.IsNotExist(err) {
			return fmt.Errorf("remove expired version %q (%s): %w", ref.Path, ref.VersionTime, err)
		}
		l.Debugln("Managed versioner: removed expired version", target)
	}
	return nil
}

// commitLedger merges the removed references of a finished batch into the
// tombstone ledger and prunes tombstones that no longer correspond to
// expire entries in the current manifest. It only writes when the ledger
// actually changed. The write itself is atomic.
func (m *managed) commitLedger(previous, removed []managedVersionRef, entries []validatedEntry) error {
	expire := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.decision == decisionExpire {
			expire[entry.key()] = struct{}{}
		}
	}

	merged := newRefSet(previous)
	for _, ref := range removed {
		merged.add(ref)
	}

	kept := make([]managedVersionRef, 0, len(merged))
	for _, ref := range merged.values() {
		if _, ok := expire[ref.key()]; ok {
			kept = append(kept, ref)
		}
	}
	sortRefs(kept)

	// Skip the write when the pruned ledger is identical to what is
	// already stored; stored ledgers are always sorted.
	if len(kept) == len(previous) {
		same := true
		for i, ref := range kept {
			if previous[i] != ref {
				same = false
				break
			}
		}
		if same {
			return nil
		}
	}

	return atomicWriteJSON(m.versionsFs, managedStateName, managedState{
		Version: managedSchemaVersion,
		Removed: kept,
	}, 0o600)
}

func (m *managed) createPending(pending managedPending) error {
	f, err := m.versionsFs.OpenFile(managedPendingName, fs.OptReadWrite|fs.OptCreate|fs.OptExclusive, 0o600)
	if err != nil {
		if fs.IsExist(err) {
			return conflictf("another cleanup batch is already in progress (%s); finish or resolve it before cleaning again", managedPendingName)
		}
		return err
	}
	if err := json.NewEncoder(f).Encode(pending); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (m *managed) loadPending() (*managedPending, bool, error) {
	data, exists, err := readSmallFile(m.versionsFs, managedPendingName)
	if err != nil || !exists {
		return nil, false, err
	}
	var pending managedPending
	if err := json.Unmarshal(data, &pending); err != nil {
		return nil, true, conflictf("unreadable cleanup journal %s: %v", managedPendingName, err)
	}
	return &pending, true, nil
}

func (m *managed) loadState() (*managedState, error) {
	data, exists, err := readSmallFile(m.versionsFs, managedStateName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return &managedState{Version: managedSchemaVersion}, nil
	}
	var state managedState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, conflictf("unreadable retention state file %s: %v", managedStateName, err)
	}
	if state.Version != managedSchemaVersion {
		return nil, conflictf("unsupported retention state version %d", state.Version)
	}
	return &state, nil
}

// scanVersions returns the set of version references currently present in
// the versions directory (keyed like managedVersionRef.key) plus a tracker
// of empty directories.
func (m *managed) scanVersions(ctx context.Context) (refSet, emptyDirTracker, error) {
	present := make(refSet)
	dirs := make(emptyDirTracker)

	walkFn := func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if path == "." {
			return nil
		}
		if info.IsDir() {
			if !info.IsSymlink() {
				dirs.addDir(path)
			}
			return nil
		}
		if info.IsSymlink() {
			return nil
		}

		dirs.addFile(path)

		if path == m.manifestName || path == managedStateName || path == managedPendingName {
			return nil
		}

		name, tag := UntagFilename(path)
		if name == "" || tag == "" {
			// Untagged or otherwise unrecognized file: not a managed
			// version, leave it alone.
			return nil
		}
		if _, err := time.ParseInLocation(TimeFormat, tag, time.Local); err != nil {
			return nil
		}
		ref := managedVersionRef{
			Path:        osutil.NormalizedFilename(name),
			VersionTime: tag,
		}
		present.add(ref)
		return nil
	}

	if err := m.versionsFs.Walk(".", walkFn); err != nil {
		if !errors.Is(err, context.Canceled) {
			l.Debugln("Managed versioner: scan failed:", err)
		}
		return nil, nil, err
	}
	return present, dirs, nil
}

// parseManifest parses and strictly validates the manifest. Any ambiguity
// (unknown schema, malformed entry, duplicate reference, unknown decision)
// is a conflict: cleanup must rather do nothing than guess.
func parseManifest(raw []byte) ([]validatedEntry, error) {
	var manifest managedManifestFile
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, conflictf("invalid JSON in retention manifest: %v", err)
	}
	if manifest.Version != managedSchemaVersion {
		return nil, conflictf("unsupported retention manifest version %d (expected %d)", manifest.Version, managedSchemaVersion)
	}

	entries := make([]validatedEntry, 0, len(manifest.Entries))
	seen := make(map[string]struct{}, len(manifest.Entries))

	for i, entry := range manifest.Entries {
		what := fmt.Sprintf("retention manifest entry %d", i)

		p := strings.TrimSpace(entry.Path)
		if p == "" {
			return nil, conflictf("%s: path must not be empty", what)
		}
		if strings.ContainsAny(p, `\:`) || strings.HasPrefix(p, "/") {
			return nil, conflictf("%s: path %q must be a slash separated relative path", what, entry.Path)
		}
		p = path.Clean(p)
		if p == "." || p == ".." || strings.HasPrefix(p, "../") {
			return nil, conflictf("%s: path %q must name a file inside the versions directory", what, entry.Path)
		}

		tag, err := normalizeVersionTime(entry.VersionTime)
		if err != nil {
			return nil, conflictf("%s for %q: %v", what, p, err)
		}

		switch entry.Decision {
		case decisionRetain, decisionExpire:
		default:
			return nil, conflictf("%s for %q (%s): decision must be %q or %q, got %q", what, p, tag, decisionRetain, decisionExpire, entry.Decision)
		}

		ref := managedVersionRef{Path: p, VersionTime: tag}
		if _, dup := seen[ref.key()]; dup {
			return nil, conflictf("duplicate manifest entry for %q (%s)", p, tag)
		}
		seen[ref.key()] = struct{}{}

		entries = append(entries, validatedEntry{
			managedVersionRef: ref,
			decision:          entry.Decision,
		})
	}

	return entries, nil
}

// normalizeVersionTime accepts the versioner tag format ("20060102-150405",
// interpreted in local time like the archived file names) and RFC3339, and
// returns the canonical tag.
func normalizeVersionTime(in string) (string, error) {
	in = strings.TrimSpace(in)
	if in == "" {
		return "", errors.New("version time must not be empty")
	}
	if t, err := time.ParseInLocation(TimeFormat, in, time.Local); err == nil {
		return t.Format(TimeFormat), nil
	}
	if t, err := time.Parse(time.RFC3339, in); err == nil {
		return t.Local().Format(TimeFormat), nil
	}
	return "", fmt.Errorf("version time %q must use format %q or RFC3339", in, TimeFormat)
}

// refSet is an ordered set of version references.
type refSet map[string]managedVersionRef

func newRefSet(refs []managedVersionRef) refSet {
	s := make(refSet, len(refs))
	for _, ref := range refs {
		s.add(ref)
	}
	return s
}

func (s refSet) add(ref managedVersionRef) {
	s[ref.key()] = ref
}

func (s refSet) has(ref managedVersionRef) bool {
	_, ok := s[ref.key()]
	return ok
}

func (s refSet) values() []managedVersionRef {
	out := make([]managedVersionRef, 0, len(s))
	for _, ref := range s {
		out = append(out, ref)
	}
	return out
}

func sortRefs(refs []managedVersionRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Path != refs[j].Path {
			return refs[i].Path < refs[j].Path
		}
		return refs[i].VersionTime < refs[j].VersionTime
	})
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func readSmallFile(filesystem fs.Filesystem, name string) ([]byte, bool, error) {
	f, err := filesystem.Open(name)
	if fs.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// atomicWriteJSON writes JSON to a temporary file in the same directory and
// renames it into place, so readers never observe a partially written file.
func atomicWriteJSON(filesystem fs.Filesystem, name string, value any, mode fs.FileMode) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmp := fs.TempName(name)
	f, err := filesystem.OpenFile(tmp, fs.OptReadWrite|fs.OptCreate|fs.OptTruncate, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return filesystem.Rename(tmp, name)
}
