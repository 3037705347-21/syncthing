// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package serve

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/syncthing/syncthing/lib/ur/contract"
)

// Batch archive members.
const (
	batchManifestName = "manifest.json"
	batchRecordsName  = "reports.jsons.gz"
	batchFormat       = 1
)

var (
	// ErrCorruptArchive means the zip or its payload cannot be read or
	// decoded.
	ErrCorruptArchive = errors.New("corrupt archive")
	// ErrManifestConflict means the archive contents do not agree with
	// its manifest (missing/unknown members, checksum or count mismatch,
	// duplicate installation IDs, invalid markers, ...).
	ErrManifestConflict = errors.New("manifest conflict")
)

// InstallStateMarker explicitly enables or disables an installation at a
// given generation. Only a marker with a strictly higher generation than
// the current state can change it.
type InstallStateMarker struct {
	UniqueID   string `json:"uniqueID"`
	Enabled    bool   `json:"enabled"`
	Generation int64  `json:"generation"`
}

// batchManifest describes a regional report archive.
type batchManifest struct {
	Format  int    `json:"format"`
	Source  string `json:"source"`
	Version int64  `json:"version"`
	Created time.Time `json:"created"`

	// Records is the name of the gzip JSON-lines member containing the
	// snapshot reports.
	Records string `json:"records"`
	// RecordCount is the exact number of report records.
	RecordCount int `json:"recordCount"`
	// RecordsSHA256 is the lowercase hex SHA-256 of the *decompressed*
	// JSON-lines payload.
	RecordsSHA256 string `json:"recordsSHA256"`

	// InstallState holds explicit enable/disable markers.
	InstallState []InstallStateMarker `json:"installState,omitempty"`
}

// parsedBatch is a fully validated archive, ready to be applied.
type parsedBatch struct {
	batchManifest
	reports []*contract.Report
}

// WriteBatch builds a regional archive: a gzip JSON-lines records member
// plus a manifest with its count and SHA-256. It is used by regional
// relays (and tests) to produce archives parseBatch accepts.
func WriteBatch(w io.Writer, source string, version int64, created time.Time, reps []*contract.Report, markers []InstallStateMarker) error {
	if source == "" {
		return fmt.Errorf("%w: empty source", ErrManifestConflict)
	}
	if version <= 0 {
		return fmt.Errorf("%w: version must be positive", ErrManifestConflict)
	}

	var payload bytes.Buffer
	enc := json.NewEncoder(&payload)
	for _, rep := range reps {
		if err := enc.Encode(rep); err != nil {
			return fmt.Errorf("encode record: %w", err)
		}
	}
	sum := sha256.Sum256(payload.Bytes())

	var compressed bytes.Buffer
	gw := gzip.NewWriter(&compressed)
	if _, err := gw.Write(payload.Bytes()); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}

	m := batchManifest{
		Format:        batchFormat,
		Source:        source,
		Version:       version,
		Created:       created.UTC(),
		Records:       batchRecordsName,
		RecordCount:   len(reps),
		RecordsSHA256: hex.EncodeToString(sum[:]),
		InstallState:  markers,
	}
	var manifest bytes.Buffer
	manifestEnc := json.NewEncoder(&manifest)
	manifestEnc.SetEscapeHTML(false)
	if err := manifestEnc.Encode(m); err != nil {
		return err
	}

	zw := zip.NewWriter(w)
	mw, err := zw.Create(batchManifestName)
	if err != nil {
		return err
	}
	if _, err := mw.Write(manifest.Bytes()); err != nil {
		return err
	}
	rw, err := zw.Create(batchRecordsName)
	if err != nil {
		return err
	}
	if _, err := rw.Write(compressed.Bytes()); err != nil {
		return err
	}
	return zw.Close()
}

// parseBatch reads and fully validates a regional archive. Nothing is
// returned without cross-checking the manifest against the actual archive
// contents; any discrepancy rejects the whole archive.
func parseBatch(zr *zip.Reader) (*parsedBatch, error) {
	var manifestFile, recordsFile *zip.File
	for _, f := range zr.File {
		switch {
		case f.FileInfo().IsDir():
			continue
		case f.Name == batchManifestName:
			if manifestFile != nil {
				return nil, fmt.Errorf("%w: duplicate manifest member", ErrManifestConflict)
			}
			manifestFile = f
		case f.Name == batchRecordsName:
			if recordsFile != nil {
				return nil, fmt.Errorf("%w: duplicate records member", ErrManifestConflict)
			}
			recordsFile = f
		default:
			// Unknown extra members are tolerated.
		}
	}
	if manifestFile == nil {
		return nil, fmt.Errorf("%w: missing %s", ErrManifestConflict, batchManifestName)
	}
	if recordsFile == nil {
		return nil, fmt.Errorf("%w: missing %s", ErrManifestConflict, batchRecordsName)
	}

	mr, err := manifestFile.Open()
	if err != nil {
		return nil, fmt.Errorf("%w: open manifest: %v", ErrCorruptArchive, err)
	}
	mbs, err := io.ReadAll(mr)
	_ = mr.Close()
	if err != nil {
		return nil, fmt.Errorf("%w: read manifest: %v", ErrCorruptArchive, err)
	}

	var m batchManifest
	if err := json.Unmarshal(mbs, &m); err != nil {
		return nil, fmt.Errorf("%w: decode manifest: %v", ErrCorruptArchive, err)
	}
	if m.Format != batchFormat {
		return nil, fmt.Errorf("%w: unsupported batch format %d", ErrManifestConflict, m.Format)
	}
	if strings.TrimSpace(m.Source) == "" {
		return nil, fmt.Errorf("%w: empty source", ErrManifestConflict)
	}
	if m.Version <= 0 {
		return nil, fmt.Errorf("%w: source version must be positive", ErrManifestConflict)
	}
	if m.Created.IsZero() {
		return nil, fmt.Errorf("%w: missing created timestamp", ErrManifestConflict)
	}
	if m.Records != batchRecordsName {
		return nil, fmt.Errorf("%w: unexpected records member %q", ErrManifestConflict, m.Records)
	}
	if m.RecordCount < 0 {
		return nil, fmt.Errorf("%w: negative record count", ErrManifestConflict)
	}
	if m.RecordsSHA256 == "" {
		return nil, fmt.Errorf("%w: missing records checksum", ErrManifestConflict)
	}
	if safeMemberName(m.Records) != m.Records {
		return nil, fmt.Errorf("%w: unsafe records member name", ErrManifestConflict)
	}

	// Stream the decompressed records payload through the checksum while
	// fully decoding it; the gzip CRC is validated when the stream is
	// drained and closed.
	rr, err := recordsFile.Open()
	if err != nil {
		return nil, fmt.Errorf("%w: open records: %v", ErrCorruptArchive, err)
	}
	gr, err := gzip.NewReader(rr)
	if err != nil {
		_ = rr.Close()
		return nil, fmt.Errorf("%w: records gzip: %v", ErrCorruptArchive, err)
	}
	hasher := sha256.New()
	tr := io.TeeReader(gr, hasher)
	dec := json.NewDecoder(tr)

	reps := make([]*contract.Report, 0, m.RecordCount)
	seenIDs := make(map[string]struct{}, m.RecordCount)
	for {
		var rep contract.Report
		if err := dec.Decode(&rep); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			_ = gr.Close()
			_ = rr.Close()
			return nil, fmt.Errorf("%w: decode record %d: %v", ErrCorruptArchive, len(reps)+1, err)
		}
		if rep.Received.IsZero() {
			rep.Received = m.Created
		}
		if rep.Date == "" {
			rep.Date = rep.Received.UTC().Format("20060102")
		}
		if err := rep.Validate(); err != nil {
			_ = gr.Close()
			_ = rr.Close()
			return nil, fmt.Errorf("%w: record %d: %v", ErrManifestConflict, len(reps)+1, err)
		}
		if _, dup := seenIDs[rep.UniqueID]; dup {
			_ = gr.Close()
			_ = rr.Close()
			return nil, fmt.Errorf("%w: duplicate installation %q in batch", ErrManifestConflict, rep.UniqueID)
		}
		seenIDs[rep.UniqueID] = struct{}{}
		reps = append(reps, &rep)
	}
	// Drain through the tee so the gzip CRC/trailer is verified and the
	// full decompressed payload is hashed.
	if _, err := io.Copy(io.Discard, tr); err != nil {
		_ = gr.Close()
		_ = rr.Close()
		return nil, fmt.Errorf("%w: records gzip: %v", ErrCorruptArchive, err)
	}
	if err := gr.Close(); err != nil {
		_ = rr.Close()
		return nil, fmt.Errorf("%w: records gzip trailer: %v", ErrCorruptArchive, err)
	}
	if err := rr.Close(); err != nil {
		return nil, fmt.Errorf("%w: read records: %v", ErrCorruptArchive, err)
	}

	if len(reps) != m.RecordCount {
		return nil, fmt.Errorf("%w: record count says %d but archive holds %d", ErrManifestConflict, m.RecordCount, len(reps))
	}
	if got := hex.EncodeToString(hasher.Sum(nil)); got != strings.ToLower(m.RecordsSHA256) {
		return nil, fmt.Errorf("%w: records checksum mismatch", ErrManifestConflict)
	}

	// Validate markers: unique targets and positive generations.
	seenMarkers := make(map[string]struct{}, len(m.InstallState))
	for _, mk := range m.InstallState {
		if mk.UniqueID == "" {
			return nil, fmt.Errorf("%w: marker with empty installation id", ErrManifestConflict)
		}
		if mk.Generation <= 0 {
			return nil, fmt.Errorf("%w: marker for %q must have positive generation", ErrManifestConflict, mk.UniqueID)
		}
		if _, dup := seenMarkers[mk.UniqueID]; dup {
			return nil, fmt.Errorf("%w: duplicate marker for installation %q", ErrManifestConflict, mk.UniqueID)
		}
		seenMarkers[mk.UniqueID] = struct{}{}
	}

	return &parsedBatch{batchManifest: m, reports: reps}, nil
}

// parseBatchBytes parses an in-memory zip archive.
func parseBatchBytes(bs []byte) (*parsedBatch, error) {
	zr, err := zip.NewReader(bytes.NewReader(bs), int64(len(bs)))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptArchive, err)
	}
	return parseBatch(zr)
}

func safeMemberName(name string) string {
	if name == "" || strings.Contains(name, "\\") {
		return ""
	}
	if path.IsAbs(name) {
		return ""
	}
	cleaned := path.Clean(name)
	if cleaned != name || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return ""
	}
	return cleaned
}
