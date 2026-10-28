// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package serve

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/ur/contract"
)

func testReport(uid string, received time.Time) *contract.Report {
	rep := contract.New()
	rep.UniqueID = uid
	rep.Version = "v1.2.3"
	rep.Platform = "linux-amd64"
	rep.Received = received
	rep.Date = received.UTC().Format("20060102")
	return rep
}

func mustBatch(t *testing.T, source string, version int64, created time.Time, reps []*contract.Report, markers []InstallStateMarker) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteBatch(&buf, source, version, created, reps, markers); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	return buf.Bytes()
}

func TestBatchRoundTrip(t *testing.T) {
	created := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	reps := []*contract.Report{
		testReport("a", created.Add(-time.Hour)),
		testReport("b", created.Add(-2*time.Hour)),
	}
	markers := []InstallStateMarker{{UniqueID: "z", Enabled: false, Generation: 1}}
	bs := mustBatch(t, "eu", 1, created, reps, markers)

	b, err := parseBatchBytes(bs)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if b.Source != "eu" || b.Version != 1 || len(b.reports) != 2 {
		t.Fatalf("unexpected manifest/records: %+v", b)
	}
	if got := b.reports[0].UniqueID; got != "a" {
		t.Errorf("record id: %q", got)
	}
	if len(b.InstallState) != 1 || b.InstallState[0].UniqueID != "z" || b.InstallState[0].Enabled {
		t.Errorf("bad markers: %+v", b.InstallState)
	}
}

func TestBatchCorruptAndConflicts(t *testing.T) {
	created := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	reps := []*contract.Report{testReport("a", created)}
	good := mustBatch(t, "eu", 1, created, reps, nil)

	tamperedChecksum := rewriteZip(t, mustZipReader(t, good), batchManifestName, func(m []byte) []byte {
		s := string(m)
		const marker = `"recordsSHA256":"`
		i := strings.Index(s, marker)
		if i < 0 {
			t.Fatal("no checksum in manifest")
		}
		pos := i + len(marker)
		c := s[pos]
		repl := byte('0')
		if c == '0' {
			repl = 'f'
		}
		return []byte(s[:pos] + string(rune(repl)) + s[pos+1:])
	}, true)

	cases := []struct {
		name string
		bs   []byte
		want error
	}{
		{"not a zip", []byte("this is not a zip at all"), ErrCorruptArchive},
		{"truncated zip", good[:len(good)-40], ErrCorruptArchive},
		{"checksum tampered", tamperedChecksum, ErrManifestConflict},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseBatchBytes(tc.bs); !errorIs(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestBatchManifestMismatches(t *testing.T) {
	created := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	reps := []*contract.Report{testReport("a", created)}
	good := mustBatch(t, "eu", 1, created, reps, nil)
	zr := mustZipReader(t, good)

	t.Run("count mismatch", func(t *testing.T) {
		bs := rewriteZip(t, zr, batchManifestName, func(m []byte) []byte {
			return []byte(strings.Replace(string(m), `"recordCount":1`, `"recordCount":2`, 1))
		}, true)
		if _, err := parseBatchBytes(bs); !errorIs(err, ErrManifestConflict) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("unsupported format", func(t *testing.T) {
		bs := rewriteZip(t, zr, batchManifestName, func(m []byte) []byte {
			return []byte(strings.Replace(string(m), `"format":1`, `"format":99`, 1))
		}, true)
		if _, err := parseBatchBytes(bs); !errorIs(err, ErrManifestConflict) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("missing records", func(t *testing.T) {
		bs := rewriteZip(t, zr, batchRecordsName, nil, false)
		if _, err := parseBatchBytes(bs); !errorIs(err, ErrManifestConflict) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("garbled records payload", func(t *testing.T) {
		bs := rewriteZip(t, zr, batchRecordsName, func(_ []byte) []byte {
			return []byte("not gzip")
		}, true)
		if _, err := parseBatchBytes(bs); !errorIs(err, ErrCorruptArchive) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestBatchDuplicateIDsAndMarkers(t *testing.T) {
	created := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)

	t.Run("duplicate report id", func(t *testing.T) {
		reps := []*contract.Report{
			testReport("a", created.Add(-time.Minute)),
			testReport("a", created),
		}
		var buf bytes.Buffer
		if err := WriteBatch(&buf, "eu", 1, created, reps, nil); err != nil {
			t.Fatal(err)
		}
		// WriteBatch does not dedupe; parsing must reject it.
		if _, err := parseBatchBytes(buf.Bytes()); !errorIs(err, ErrManifestConflict) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("duplicate marker", func(t *testing.T) {
		markers := []InstallStateMarker{
			{UniqueID: "a", Enabled: false, Generation: 1},
			{UniqueID: "a", Enabled: true, Generation: 2},
		}
		bs := mustBatch(t, "eu", 1, created, nil, markers)
		if _, err := parseBatchBytes(bs); !errorIs(err, ErrManifestConflict) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("marker with zero generation", func(t *testing.T) {
		bs := mustBatch(t, "eu", 1, created, nil, []InstallStateMarker{{UniqueID: "a", Generation: 0}})
		if _, err := parseBatchBytes(bs); !errorIs(err, ErrManifestConflict) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestBatchInvalidSourceAndVersion(t *testing.T) {
	created := time.Now()

	var buf bytes.Buffer
	if err := WriteBatch(&buf, "", 1, created, nil, nil); !errorIs(err, ErrManifestConflict) {
		t.Fatalf("empty source: %v", err)
	}

	buf.Reset()
	if err := WriteBatch(&buf, "eu", 0, created, nil, nil); !errorIs(err, ErrManifestConflict) {
		t.Fatalf("zero version: %v", err)
	}
}

// --- zip editing helpers ---

func errorIs(err, target error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, target) {
		return true
	}
	return strings.Contains(err.Error(), target.Error())
}

func mustZipReader(t *testing.T, bs []byte) *zip.Reader {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(bs), int64(len(bs)))
	if err != nil {
		t.Fatal(err)
	}
	return zr
}

// rewriteZip copies a zip, replacing the named member with mutate(contents)
// when mutate is non-nil, or dropping it when nil.
func rewriteZip(t *testing.T, zr *zip.Reader, target string, mutate func([]byte) []byte, keep bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range zr.File {
		if f.Name == target {
			if !keep {
				continue
			}
			rc, err := f.Open()
			if err != nil {
				t.Fatal(err)
			}
			old := make([]byte, f.UncompressedSize64)
			if _, err := io.ReadFull(rc, old); err != nil {
				t.Fatal(err)
			}
			_ = rc.Close()
			w, err := zw.Create(f.Name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(mutate(old)); err != nil {
				t.Fatal(err)
			}
			continue
		}
		// copy other members verbatim
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		w, err := zw.Create(f.Name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(w, rc); err != nil {
			t.Fatal(err)
		}
		_ = rc.Close()
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
