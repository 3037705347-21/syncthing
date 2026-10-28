// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package versioner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
)

func managedTestSetup(t *testing.T) (*managed, fs.Filesystem, fs.Filesystem) {
	t.Helper()

	dir := t.TempDir()
	cfg := config.FolderConfiguration{
		FilesystemType: config.FilesystemTypeBasic,
		Path:           dir,
		Versioning: config.VersioningConfiguration{
			Type:   "managed",
			Params: map[string]string{},
		},
	}
	folderFs := cfg.Filesystem()
	versionsFs := fs.NewFilesystem(fs.FilesystemTypeBasic, filepath.Join(dir, ".stversions"))
	if err := versionsFs.MkdirAll(".", 0o755); err != nil {
		t.Fatal(err)
	}

	v := newManaged(cfg).(*managed)
	return v, folderFs, versionsFs
}

// putFile writes a file at the given path inside the filesystem, creating
// parent directories.
func putFile(t *testing.T, fsys fs.Filesystem, name, content string) {
	t.Helper()
	if err := fsys.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, fsys, name, content)
}

// putVersion writes the archived file representing logicalPath at the
// given version tag.
func putVersion(t *testing.T, fsys fs.Filesystem, logicalPath, tag string) {
	t.Helper()
	putFile(t, fsys, TagFilename(filepath.FromSlash(logicalPath), tag), "v-"+logicalPath+"@"+tag)
}

func putManifest(t *testing.T, fsys fs.Filesystem, entries []managedEntry) {
	t.Helper()
	if err := atomicWriteJSON(fsys, defaultManifestName, managedManifestFile{
		Version: managedSchemaVersion,
		Entries: entries,
	}, 0o644); err != nil {
		t.Fatal(err)
	}
}

func manifestRaw(t *testing.T, fsys fs.Filesystem) string {
	t.Helper()
	data, exists, err := readSmallFile(fsys, defaultManifestName)
	if err != nil || !exists {
		t.Fatal("manifest missing:", err)
	}
	return hashBytes(data)
}

func fileExists(t *testing.T, fsys fs.Filesystem, name string) bool {
	t.Helper()
	_, err := fsys.Lstat(name)
	if err == nil {
		return true
	}
	if fs.IsNotExist(err) {
		return false
	}
	t.Fatal(err)
	return false
}

func versionPath(logicalPath, tag string) string {
	return TagFilename(filepath.FromSlash(logicalPath), tag)
}

func rfcTag(in string) string {
	tm, err := time.Parse(time.RFC3339, in)
	if err != nil {
		panic(err)
	}
	return tm.Local().Format(TimeFormat)
}

// The basic contract: exactly the manifest-expired managed versions are
// removed; retained, unmanaged and hand-placed files stay; reruns converge
// and are idempotent.
func TestManagedCleanBasic(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	const (
		aOld    = "20260101-120000"
		aKeep   = "20260102-120000"
		aManual = "20260103-120000"
	)
	bTag := rfcTag("2026-01-04T08:00:00Z") // normalized local tag the manifest entry resolves to

	putVersion(t, versionsFs, "a.txt", aOld)
	putVersion(t, versionsFs, "a.txt", aKeep)
	putVersion(t, versionsFs, "a.txt", aManual)
	putVersion(t, versionsFs, "sub/b.dat", bTag)
	// A hand-placed, untagged file in the versions directory.
	putFile(t, versionsFs, "notes.txt", "manual keep")
	// A hand-placed tagged version the manifest does not know about, in
	// another directory.
	putVersion(t, versionsFs, "other/c.bin", "20260105-120000")

	rfcTime := "2026-01-04T08:00:00Z"
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: aOld, Decision: decisionExpire},
		{Path: "a.txt", VersionTime: aKeep, Decision: decisionRetain},
		{Path: "sub/b.dat", VersionTime: rfcTime, Decision: decisionExpire},
	})

	if err := v.Clean(context.Background()); err != nil {
		t.Fatal("first clean:", err)
	}

	if fileExists(t, versionsFs, versionPath("a.txt", aOld)) {
		t.Error("expired version should have been removed")
	}
	if fileExists(t, versionsFs, versionPath("sub/b.dat", bTag)) {
		t.Error("expired version should have been removed")
	}
	for _, p := range []string{
		versionPath("a.txt", aKeep),
		versionPath("a.txt", aManual),
		"notes.txt",
		versionPath("other/c.bin", "20260105-120000"),
	} {
		if !fileExists(t, versionsFs, p) {
			t.Errorf("file %q should have been kept", p)
		}
	}
	// The directory that only held the removed version is cleaned up.
	if fileExists(t, versionsFs, "sub") {
		t.Error("empty directory should have been removed")
	}
	// The journal is gone after a completed batch.
	if fileExists(t, versionsFs, managedPendingName) {
		t.Error("pending journal should have been removed")
	}
	// The tombstone ledger records exactly the removed versions.
	state := readState(t, versionsFs)
	stateKeys := refKeys(state.Removed)
	if !slices.Equal(stateKeys, []string{"a.txt\x00" + aOld, "sub/b.dat\x00" + bTag}) {
		t.Errorf("unexpected ledger: %v", stateKeys)
	}

	// A rerun must be a converged, idempotent no-op.
	if err := v.Clean(context.Background()); err != nil {
		t.Fatal("second clean:", err)
	}
	for _, p := range []string{
		versionPath("a.txt", aKeep),
		versionPath("a.txt", aManual),
		"notes.txt",
		versionPath("other/c.bin", "20260105-120000"),
	} {
		if !fileExists(t, versionsFs, p) {
			t.Errorf("file %q should still be kept after rerun", p)
		}
	}

	// Bookkeeping files and the manifest are not reported as versions.
	versions, err := v.GetVersions()
	if err != nil {
		t.Fatal(err)
	}
	for _, bogus := range []string{defaultManifestName, managedStateName, managedPendingName, "notes.txt"} {
		if _, ok := versions[bogus]; ok {
			t.Errorf("bookkeeping/untagged file %q should not be reported as a version", bogus)
		}
	}
	av := versions["a.txt"]
	if len(av) != 2 {
		t.Errorf("expected two a.txt versions (retained + unmanaged), got %d: %+v", len(av), av)
	}
}

// Without a manifest nothing is managed and cleanup removes nothing.
func TestManagedCleanWithoutManifest(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	putVersion(t, versionsFs, "old.txt", "20000101-000000")
	putFile(t, versionsFs, "loose", "x")

	if err := v.Clean(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fileExists(t, versionsFs, versionPath("old.txt", "20000101-000000")) {
		t.Error("unmanaged version must not be removed without a manifest")
	}
	if !fileExists(t, versionsFs, "loose") {
		t.Error("unmanaged file must not be removed")
	}
}

func TestManagedCleanMissingVersionsDir(t *testing.T) {
	dir := t.TempDir()
	cfg := config.FolderConfiguration{
		FilesystemType: config.FilesystemTypeBasic,
		Path:           dir,
		Versioning: config.VersioningConfiguration{
			Type:   "managed",
			Params: map[string]string{},
		},
	}
	v := newManaged(cfg)
	if err := v.Clean(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// All conflict conditions abort the whole batch with a conflict error and
// leave the directory untouched.
func TestManagedManifestConflicts(t *testing.T) {
	const tag = "20260101-120000"

	cases := []struct {
		name    string
		write   func(t *testing.T, fsys fs.Filesystem)
		wantErr bool
	}{
		{
			name: "invalid JSON",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putFile(t, fsys, defaultManifestName, "{not json")
			},
			wantErr: true,
		},
		{
			name: "unsupported version",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putFile(t, fsys, defaultManifestName, `{"version":99,"entries":[]}`)
			},
			wantErr: true,
		},
		{
			name: "empty path",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putManifest(t, fsys, []managedEntry{
					{Path: "", VersionTime: tag, Decision: decisionExpire},
				})
			},
			wantErr: true,
		},
		{
			name: "absolute path",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putManifest(t, fsys, []managedEntry{
					{Path: "/etc/passwd", VersionTime: tag, Decision: decisionExpire},
				})
			},
			wantErr: true,
		},
		{
			name: "parent traversal",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putManifest(t, fsys, []managedEntry{
					{Path: "../escape.txt", VersionTime: tag, Decision: decisionExpire},
				})
			},
			wantErr: true,
		},
		{
			name: "directory root path",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putManifest(t, fsys, []managedEntry{
					{Path: "a/..", VersionTime: tag, Decision: decisionExpire},
				})
			},
			wantErr: true,
		},
		{
			name: "bad version time",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putManifest(t, fsys, []managedEntry{
					{Path: "a.txt", VersionTime: "last-week", Decision: decisionExpire},
				})
			},
			wantErr: true,
		},
		{
			name: "unknown decision",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putManifest(t, fsys, []managedEntry{
					{Path: "a.txt", VersionTime: tag, Decision: "maybe"},
				})
			},
			wantErr: true,
		},
		{
			name: "duplicate reference",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putManifest(t, fsys, []managedEntry{
					{Path: "a.txt", VersionTime: tag, Decision: decisionExpire},
					{Path: "a.txt", VersionTime: tag, Decision: decisionRetain},
				})
			},
			wantErr: true,
		},
		{
			name: "expired version missing on disk",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putVersion(t, fsys, "keep.txt", tag)
				putManifest(t, fsys, []managedEntry{
					{Path: "keep.txt", VersionTime: tag, Decision: decisionRetain},
					{Path: "gone.txt", VersionTime: tag, Decision: decisionExpire},
				})
			},
			wantErr: true,
		},
		{
			name: "retained version missing on disk",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putVersion(t, fsys, "old.txt", tag)
				putManifest(t, fsys, []managedEntry{
					{Path: "old.txt", VersionTime: tag, Decision: decisionExpire},
					{Path: "pinned.txt", VersionTime: tag, Decision: decisionRetain},
				})
			},
			wantErr: true,
		},
		{
			name: "valid manifest",
			write: func(t *testing.T, fsys fs.Filesystem) {
				putVersion(t, fsys, "a.txt", tag)
				putManifest(t, fsys, []managedEntry{
					{Path: "a.txt", VersionTime: tag, Decision: decisionRetain},
				})
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, _, versionsFs := managedTestSetup(t)
			tc.write(t, versionsFs)

			err := v.Clean(context.Background())
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected conflict error, got nil")
				}
				if !errors.Is(err, errRetentionConflict) {
					t.Fatalf("expected errRetentionConflict, got %v", err)
				}
				// Nothing was deleted and no batch state was left behind.
				if fileExists(t, versionsFs, managedPendingName) {
					t.Error("no journal may exist when a batch never started")
				}
				if fileExists(t, versionsFs, managedStateName) {
					t.Error("no ledger may exist when a batch never started")
				}
			} else if err != nil {
				t.Fatal("unexpected error:", err)
			}
		})
	}
}

// When a batch is interrupted after some deletions, the journal records
// exactly what was approved; the rerun resumes and converges.
func TestManagedCleanInterruptedResume(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	tags := []string{"20260101-120000", "20260102-120000", "20260103-120000"}
	var entries []managedEntry
	var plan []managedVersionRef
	for _, tag := range tags {
		putVersion(t, versionsFs, "a.txt", tag)
		entries = append(entries, managedEntry{Path: "a.txt", VersionTime: tag, Decision: decisionExpire})
		plan = append(plan, managedVersionRef{Path: "a.txt", VersionTime: tag})
	}
	const keep = "20260104-120000"
	putVersion(t, versionsFs, "a.txt", keep)
	entries = append(entries, managedEntry{Path: "a.txt", VersionTime: keep, Decision: decisionRetain})
	putManifest(t, versionsFs, entries)

	// Simulate a crash in the middle of the batch: journal exists with the
	// approved plan and one file has already been removed.
	already := plan[0]
	if err := versionsFs.Remove(already.target()); err != nil {
		t.Fatal(err)
	}
	pend := managedPending{
		Version:      managedSchemaVersion,
		ManifestHash: manifestRaw(t, versionsFs),
		Plan:         plan,
	}
	writeJSON(t, versionsFs, managedPendingName, pend)

	if err := v.Clean(context.Background()); err != nil {
		t.Fatal("resume clean:", err)
	}

	for _, tag := range tags {
		if fileExists(t, versionsFs, versionPath("a.txt", tag)) {
			t.Errorf("expired version %s should be gone after resume", tag)
		}
	}
	if !fileExists(t, versionsFs, versionPath("a.txt", keep)) {
		t.Error("retained version must survive the resumed batch")
	}
	if fileExists(t, versionsFs, managedPendingName) {
		t.Error("journal should be removed after completion")
	}

	// Another rerun is a no-op.
	if err := v.Clean(context.Background()); err != nil {
		t.Fatal("rerun after resume:", err)
	}
}

// A journal whose manifest changed but which deleted nothing is stale and
// gets discarded; the new manifest is executed instead.
func TestManagedStalePendingManifestChangedNoDeletions(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	putVersion(t, versionsFs, "a.txt", "20260101-120000")
	putVersion(t, versionsFs, "a.txt", "20260102-120000")
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
	})
	oldHash := manifestRaw(t, versionsFs)

	// Old in-flight batch, nothing removed (both files still present).
	writeJSON(t, versionsFs, managedPendingName, managedPending{
		Version:      managedSchemaVersion,
		ManifestHash: oldHash,
		Plan:         []managedVersionRef{{Path: "a.txt", VersionTime: "20260101-120000"}},
	})

	// Edit the manifest before resuming: only the other version is now expired.
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260102-120000", Decision: decisionExpire},
	})

	if err := v.Clean(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fileExists(t, versionsFs, versionPath("a.txt", "20260102-120000")) {
		t.Error("new manifest's expired version should be removed")
	}
	if !fileExists(t, versionsFs, versionPath("a.txt", "20260101-120000")) {
		t.Error("version not expired by the new manifest must be kept")
	}
}

// A journal whose manifest changed after the batch already deleted a file
// cannot be proven safe: cleanup must stop.
func TestManagedPendingChangedManifestAfterDeletion(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	putVersion(t, versionsFs, "a.txt", "20260101-120000")
	putVersion(t, versionsFs, "a.txt", "20260102-120000")
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
	})
	oldHash := manifestRaw(t, versionsFs)

	// Interrupted batch: the first file was already removed.
	if err := versionsFs.Remove(versionPath("a.txt", "20260101-120000")); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, versionsFs, managedPendingName, managedPending{
		Version:      managedSchemaVersion,
		ManifestHash: oldHash,
		Plan:         []managedVersionRef{{Path: "a.txt", VersionTime: "20260101-120000"}},
	})

	// New manifest now wants to keep the removed version and expire the other.
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260102-120000", Decision: decisionExpire},
	})

	err := v.Clean(context.Background())
	if !errors.Is(err, errRetentionConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	// The other version must not have been touched.
	if !fileExists(t, versionsFs, versionPath("a.txt", "20260102-120000")) {
		t.Error("batch must stop entirely; remaining versions untouched")
	}
}

// Cancellation aborts the batch with the journal in place and no further
// files touched; rerunning with a live context converges.
func TestManagedCleanCancellation(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	tags := []string{"20260101-120000", "20260102-120000", "20260103-120000"}
	var entries []managedEntry
	var plan []managedVersionRef
	for _, tag := range tags {
		putVersion(t, versionsFs, "a.txt", tag)
		entries = append(entries, managedEntry{Path: "a.txt", VersionTime: tag, Decision: decisionExpire})
		plan = append(plan, managedVersionRef{Path: "a.txt", VersionTime: tag})
	}
	putManifest(t, versionsFs, entries)
	writeJSON(t, versionsFs, managedPendingName, managedPending{
		Version:      managedSchemaVersion,
		ManifestHash: manifestRaw(t, versionsFs),
		Plan:         plan,
	})

	// A cancellation while executing the approved batch stops before the
	// first removal.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := v.removeFiles(ctx, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	for _, tag := range tags {
		if !fileExists(t, versionsFs, versionPath("a.txt", tag)) {
			t.Errorf("version %s should remain after cancellation", tag)
		}
	}
	if !fileExists(t, versionsFs, managedPendingName) {
		t.Fatal("journal should survive cancellation")
	}

	// Rerun resumes and converges.
	if err := v.Clean(context.Background()); err != nil {
		t.Fatal("rerun:", err)
	}
	for _, tag := range tags {
		if fileExists(t, versionsFs, versionPath("a.txt", tag)) {
			t.Errorf("version %s should be removed after converging rerun", tag)
		}
	}
}

// An existing journal for a different in-flight batch blocks a new cleanup.
func TestManagedConcurrentJournalRejected(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	putVersion(t, versionsFs, "a.txt", "20260101-120000")
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
	})
	writeJSON(t, versionsFs, managedPendingName, managedPending{
		Version:      managedSchemaVersion,
		ManifestHash: "deadbeef", // different hash
		Plan: []managedVersionRef{
			{Path: "other.txt", VersionTime: "20260101-120000"},
		},
	})
	// The other batch claims to have removed something that never existed.

	err := v.Clean(context.Background())
	if !errors.Is(err, errRetentionConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if !fileExists(t, versionsFs, versionPath("a.txt", "20260101-120000")) {
		t.Error("nothing may be deleted while another batch is in flight")
	}
}

// Tombstones are pruned to expire entries still in the manifest; entries
// removed from the manifest no longer need their tombstones.
func TestManagedLedgerPruning(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	putVersion(t, versionsFs, "a.txt", "20260101-120000")
	putVersion(t, versionsFs, "b.txt", "20260101-120000")
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
		{Path: "b.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
	})
	if err := v.Clean(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := refKeys(readState(t, versionsFs).Removed); len(got) != 2 {
		t.Fatalf("expected two tombstones, got %v", got)
	}

	// New manifest drops "a.txt" entirely; its tombstone is pruned.
	putManifest(t, versionsFs, []managedEntry{
		{Path: "b.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
	})
	if err := v.Clean(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := refKeys(readState(t, versionsFs).Removed); !slices.Equal(got, []string{"b.txt\x0020260101-120000"}) {
		t.Fatalf("expected only b's tombstone, got %v", got)
	}

	// Re-adding an expire entry for a version missing without a
	// tombstone is a conflict (can't prove ownership of its absence).
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
		{Path: "b.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
	})
	if err := v.Clean(context.Background()); !errors.Is(err, errRetentionConflict) {
		t.Fatalf("expected conflict for unproven missing version, got %v", err)
	}
}

// Archiving never deletes anything, even when the manifest would expire
// versions of that file; only Clean executes manifest decisions. A newly
// archived version is unmanaged and survives cleanup.
func TestManagedArchiveDoesNotDelete(t *testing.T) {
	v, folderFs, versionsFs := managedTestSetup(t)

	putVersion(t, versionsFs, "a.txt", "20260101-120000")
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
	})

	// Archive the current file; the old expired version must remain
	// untouched by Archive itself.
	writeFile(t, folderFs, "a.txt", "current")
	if err := v.Archive("a.txt"); err != nil {
		t.Fatal(err)
	}
	if !fileExists(t, versionsFs, versionPath("a.txt", "20260101-120000")) {
		t.Error("Archive must not run manifest-driven cleanup")
	}

	// Cleanup removes the manifest-expired version; the freshly archived
	// version is not in the manifest and is therefore retained.
	if err := v.Clean(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fileExists(t, versionsFs, versionPath("a.txt", "20260101-120000")) {
		t.Error("expired version should be removed by Clean")
	}
	entries, err := filepath.Glob(filepath.Join(versionsFs.URI(), "a~*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("expected the newly archived version to survive, got %v", entries)
	}
}

// Restore works like for the other tag-based versioners.
func TestManagedRestore(t *testing.T) {
	v, folderFs, versionsFs := managedTestSetup(t)

	const tag = "20260101-120000"
	putVersion(t, versionsFs, "a.txt", tag)

	vt := parseTime(tag)
	if err := v.Restore("a.txt", vt); err != nil {
		t.Fatal(err)
	}
	if content := readFile(t, folderFs, "a.txt"); content != "v-a.txt@"+tag {
		t.Errorf("unexpected restored content %q", content)
	}
	if fileExists(t, versionsFs, versionPath("a.txt", tag)) {
		t.Error("restored version should be removed from the archive")
	}
}

// parseManifest normalization and validation details.
func TestManagedParseManifest(t *testing.T) {
	raw := []byte(`{
		"version": 1,
		"entries": [
			{"path": "dir/../a.txt", "versionTime": "2026-01-01T12:00:00Z", "decision": "retain"},
			{"path": "sub//b.txt", "versionTime": "20260102-120000", "decision": "expire"}
		]
	}`)
	entries, err := parseManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := []validatedEntry{
		{managedVersionRef: managedVersionRef{Path: "a.txt", VersionTime: rfcTag("2026-01-01T12:00:00Z")}, decision: decisionRetain},
		{managedVersionRef: managedVersionRef{Path: "sub/b.txt", VersionTime: "20260102-120000"}, decision: decisionExpire},
	}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d", len(entries), len(want))
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Errorf("entry %d = %+v, want %+v", i, entries[i], want[i])
		}
	}
}

func readState(t *testing.T, fsys fs.Filesystem) managedState {
	t.Helper()
	data, exists, err := readSmallFile(fsys, managedStateName)
	if err != nil || !exists {
		t.Fatal("state file missing:", err)
	}
	var state managedState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func writeJSON(t *testing.T, fsys fs.Filesystem, name string, v any) {
	t.Helper()
	if err := atomicWriteJSON(fsys, name, v, 0o600); err != nil {
		t.Fatal(err)
	}
}

func refKeys(refs []managedVersionRef) []string {
	var out []string
	for _, ref := range refs {
		out = append(out, ref.key())
	}
	slices.Sort(out)
	return out
}

// Guard against accidental leftovers: no stray temp files after a run.
func TestManagedNoTempFilesLeftovers(t *testing.T) {
	v, _, versionsFs := managedTestSetup(t)

	putVersion(t, versionsFs, "a.txt", "20260101-120000")
	putManifest(t, versionsFs, []managedEntry{
		{Path: "a.txt", VersionTime: "20260101-120000", Decision: decisionExpire},
	})
	if err := v.Clean(context.Background()); err != nil {
		t.Fatal(err)
	}

	var leftovers []string
	err := filepath.WalkDir(versionsFs.URI(), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if fs.IsTemporary(filepath.Base(p)) {
			leftovers = append(leftovers, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) > 0 {
		t.Errorf("temporary files left behind: %v", leftovers)
	}
}
