// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package serve

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/syncthing/syncthing/internal/blob"
	"github.com/syncthing/syncthing/internal/blob/s3"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/geoip"
	"github.com/syncthing/syncthing/lib/ur/contract"
	"github.com/thejerf/suture/v4"
)

type CLI struct {
	Listen          string        `env:"UR_LISTEN" help:"Usage reporting & metrics endpoint listen address" default:"0.0.0.0:8080"`
	ListenInternal  string        `env:"UR_LISTEN_INTERNAL" help:"Internal metrics endpoint listen address" default:"0.0.0.0:8082"`
	GeoIPLicenseKey string        `env:"UR_GEOIP_LICENSE_KEY"`
	GeoIPAccountID  int           `env:"UR_GEOIP_ACCOUNT_ID"`
	DumpFile        string        `env:"UR_DUMP_FILE" default:"reports.jsons.gz"`
	DumpInterval    time.Duration `env:"UR_DUMP_INTERVAL" default:"5m"`

	S3Endpoint    string `name:"s3-endpoint" env:"UR_S3_ENDPOINT"`
	S3Region      string `name:"s3-region" env:"UR_S3_REGION"`
	S3Bucket      string `name:"s3-bucket" env:"UR_S3_BUCKET"`
	S3AccessKeyID string `name:"s3-access-key-id" env:"UR_S3_ACCESS_KEY_ID"`
	S3SecretKey   string `name:"s3-secret-key" env:"UR_S3_SECRET_KEY"`
}

var (
	compilerRe         = regexp.MustCompile(`\(([A-Za-z0-9()., -]+) \w+-\w+(?:| android| default)\) ([\w@.-]+)`)
	knownDistributions = []distributionMatch{
		// Maps well known builders to the official distribution method that
		// they represent

		{regexp.MustCompile(`\steamcity@build\.syncthing\.net`), "GitHub"},
		{regexp.MustCompile(`\sjenkins@build\.syncthing\.net`), "GitHub"},
		{regexp.MustCompile(`\sbuilder@github\.syncthing\.net`), "GitHub"},

		{regexp.MustCompile(`\sdeb@build\.syncthing\.net`), "APT"},
		{regexp.MustCompile(`\sdebian@github\.syncthing\.net`), "APT"},

		{regexp.MustCompile(`\sdocker@syncthing\.net`), "Docker Hub"},
		{regexp.MustCompile(`\sdocker@build.syncthing\.net`), "Docker Hub"},
		{regexp.MustCompile(`\sdocker@github.syncthing\.net`), "Docker Hub"},

		{regexp.MustCompile(`\sandroid-builder@github\.syncthing\.net`), "Google Play"},
		{regexp.MustCompile(`\sandroid-.*teamcity@build\.syncthing\.net`), "Google Play"},

		{regexp.MustCompile(`\sandroid-.*vagrant@basebox-stretch64`), "F-Droid"},
		{regexp.MustCompile(`\svagrant@bullseye`), "F-Droid"},
		{regexp.MustCompile(`\svagrant@bookworm`), "F-Droid"},

		{regexp.MustCompile(`\sreproducible-build@Catfriend1-syncthing-android`), "Syncthing-Fork Catfriend1 (3rd party)"},
		{regexp.MustCompile(`\sreproducible-build@nel0x-syncthing-android-gplay`), "Syncthing-Fork nel0x (3rd party)"},

		{regexp.MustCompile(`\sbuilduser@(archlinux|svetlemodry)`), "Arch (3rd party)"},
		{regexp.MustCompile(`\ssyncthing@archlinux`), "Arch (3rd party)"},
		{regexp.MustCompile(`@debian`), "Debian (3rd party)"},
		{regexp.MustCompile(`@fedora`), "Fedora (3rd party)"},
		{regexp.MustCompile(`@openSUSE`), "openSUSE (3rd party)"},
		{regexp.MustCompile(`\sbrew@`), "Homebrew (3rd party)"},
		{regexp.MustCompile(`\sroot@buildkitsandbox`), "LinuxServer.io (3rd party)"},
		{regexp.MustCompile(`\sports@freebsd`), "FreeBSD (3rd party)"},
		{regexp.MustCompile(`\snix@nix`), "Nix (3rd party)"},
		{regexp.MustCompile(`.`), "Others"},
	}
)

type distributionMatch struct {
	matcher      *regexp.Regexp
	distribution string
}

func (cli *CLI) Run() error {
	slog.Info("Starting", "version", build.Version)

	// Listening

	urListener, err := net.Listen("tcp", cli.Listen)
	if err != nil {
		slog.Error("Failed to listen (usage reports)", slogutil.Error(err))
		return err
	}
	slog.Info("Listening (usage reports)", slogutil.Address(urListener.Addr()))

	internalListener, err := net.Listen("tcp", cli.ListenInternal)
	if err != nil {
		slog.Error("Failed to listen (internal)", slogutil.Error(err))
		return err
	}
	slog.Info("Listening (internal)", slogutil.Address(internalListener.Addr()))

	var geo *geoip.Provider
	if cli.GeoIPAccountID != 0 && cli.GeoIPLicenseKey != "" {
		geo, err = geoip.NewGeoLite2CityProvider(context.Background(), cli.GeoIPAccountID, cli.GeoIPLicenseKey, os.TempDir())
		if err != nil {
			slog.Error("Failed to load GeoIP", slogutil.Error(err))
			return err
		}
		go geo.Serve(context.TODO())
	}

	// Blob storage

	var blobs blob.Store
	if cli.S3Endpoint != "" {
		blobs, err = s3.NewSession(cli.S3Endpoint, cli.S3Region, cli.S3Bucket, cli.S3AccessKeyID, cli.S3SecretKey)
		if err != nil {
			slog.Error("Failed to create S3 session", slogutil.Error(err))
			return err
		}
	}

	// server

	srv := &server{
		geo:   geo,
		store: newReportStore(statePathFor(cli.DumpFile), cli.DumpFile, nil),
	}
	srv.store.enrich = srv.enrichReport

	// Reconciliation state (including tombstones and source versions) is
	// authoritative. The plain report dump is only used as a legacy
	// bootstrap and as the derived export.
	if err := cli.bootstrap(srv.store, blobs); err != nil {
		slog.Error("Failed to bootstrap report state", slogutil.Error(err))
	}

	go func() {
		for range time.Tick(cli.DumpInterval) {
			if err := cli.saveDumpFile(srv, blobs); err != nil {
				slog.Error("Failed to write dump file", slogutil.Error(err))
			}
		}
	}()

	// The internal metrics endpoint just serves metrics about what the
	// server is doing.

	http.Handle("/metrics", promhttp.Handler())

	internalSrv := http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	go internalSrv.Serve(internalListener)

	// New external metrics endpoint accepts reports from clients and serves
	// aggregated usage reporting metrics.

	main := suture.NewSimple("main")
	main.ServeBackground(context.Background())

	ms := newMetricsSet(srv)
	main.Add(ms)

	reg := prometheus.NewRegistry()
	reg.MustRegister(ms)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/newdata", srv.handleNewData)
	mux.HandleFunc("/import", srv.handleImport)
	mux.HandleFunc("/ping", srv.handlePing)

	metricsSrv := http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 60 * time.Second,
		Handler:      mux,
	}

	slog.Info("Ready to serve")
	return metricsSrv.Serve(urListener)
}

// bootstrap loads the canonical reconciliation state. Local state wins;
// otherwise a local legacy dump is imported and upgraded; otherwise the
// latest blob in object storage is fetched and sniffed for its format.
func (cli *CLI) bootstrap(s *reportStore, blobs blob.Store) error {
	if fd, err := os.Open(s.statePath); err == nil {
		err := s.LoadState(fd)
		_ = fd.Close()
		if err != nil {
			return fmt.Errorf("load state file: %w", err)
		}
		slog.Info("Loaded reconciliation state", "active", s.ActiveCount())
		// Self-heal the derived export.
		if err := s.WriteExport(); err != nil {
			slog.Warn("Failed to write export from state", slogutil.Error(err))
		}
		return nil
	}

	if fd, err := os.Open(s.exportPath); err == nil {
		n, err := s.LoadLegacy(fd)
		_ = fd.Close()
		if err != nil {
			return fmt.Errorf("load legacy dump: %w", err)
		}
		slog.Info("Loaded legacy report dump", "count", n)
		if err := s.Persist(); err != nil {
			slog.Warn("Failed to persist upgraded state", slogutil.Error(err))
		}
		return nil
	}

	if blobs == nil {
		return nil
	}

	latestKey, err := blobs.LatestKey(context.Background())
	if err != nil {
		return fmt.Errorf("list latest blob: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.exportPath), ".urdownload-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := blobs.Download(context.Background(), latestKey, tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("download blob %q: %w", latestKey, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close download: %w", err)
	}
	bs, err := os.ReadFile(tmpName)
	if err != nil {
		return fmt.Errorf("read download: %w", err)
	}

	isState := sniffStateGzip(bs)
	target := s.exportPath
	if isState {
		target = s.statePath
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("install downloaded blob: %w", err)
	}

	fd, err := os.Open(target)
	if err != nil {
		return err
	}
	if isState {
		err = s.LoadState(fd)
		_ = fd.Close()
		if err != nil {
			return fmt.Errorf("load downloaded state %q: %w", latestKey, err)
		}
		slog.Info("Loaded reconciliation state from blob storage", "active", s.ActiveCount(), "key", latestKey)
		if err := s.WriteExport(); err != nil {
			slog.Warn("Failed to write export from state", slogutil.Error(err))
		}
	} else {
		n, lerr := s.LoadLegacy(fd)
		_ = fd.Close()
		if lerr != nil {
			return fmt.Errorf("load downloaded dump %q: %w", latestKey, lerr)
		}
		slog.Info("Loaded legacy report dump from blob storage", "count", n, "key", latestKey)
		if err := s.Persist(); err != nil {
			slog.Warn("Failed to persist upgraded state", slogutil.Error(err))
		}
	}
	return nil
}

func (cli *CLI) saveDumpFile(srv *server, blobs blob.Store) error {
	t0 := time.Now()
	defer func() {
		metricsWriteSecondsLast.Set(float64(time.Since(t0)))
	}()

	// The state file is the source of truth; the export is derived. Both
	// are written atomically (temp file + rename).
	if err := srv.store.Persist(); err != nil {
		return fmt.Errorf("persisting state: %w", err)
	}
	if err := srv.store.WriteExport(); err != nil {
		return fmt.Errorf("writing export: %w", err)
	}
	slog.Info("Dump file saved", "active", srv.store.ActiveCount(), "d", time.Since(t0).String())

	if blobs != nil {
		day := time.Now().UTC().Format("2006-01-02")

		t1 := time.Now()
		exportKey := fmt.Sprintf("reports-%s.jsons.gz", day)
		if err := uploadFile(blobs, exportKey, cli.DumpFile); err != nil {
			return err
		}
		// Upload the state last so that the latest-modified blob is the
		// authoritative one on a fresh bootstrap.
		stateKey := fmt.Sprintf("reports-%s.state.gz", day)
		if err := uploadFile(blobs, stateKey, statePathFor(cli.DumpFile)); err != nil {
			return err
		}
		slog.Info("Dump files uploaded", "d", time.Since(t1).String())
	}

	return nil
}

func uploadFile(blobs blob.Store, key, path string) error {
	fd, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %s: %w", path, err)
	}
	if err := blobs.Upload(context.Background(), key, fd); err != nil {
		_ = fd.Close()
		return fmt.Errorf("uploading %s: %w", key, err)
	}
	_ = fd.Close()
	return nil
}

type server struct {
	geo   *geoip.Provider
	store *reportStore
}

func (s *server) handlePing(w http.ResponseWriter, r *http.Request) {
}

func (s *server) handleNewData(w http.ResponseWriter, r *http.Request) {
	result := "fail"
	defer func() {
		// result is "accept" (new report), "replace" (existing report),
		// "disabled" (installation tombstoned) or "fail"
		metricReportsTotal.WithLabelValues(result).Inc()
	}()

	defer r.Body.Close()

	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	addr := r.Header.Get("X-Forwarded-For")
	if addr != "" {
		addr = strings.Split(addr, ", ")[0]
	} else {
		addr = r.RemoteAddr
	}

	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}

	log := slog.With("addr", addr)

	if net.ParseIP(addr) == nil {
		addr = ""
	}

	var rep contract.Report

	lr := &io.LimitedReader{R: r.Body, N: 40 * 1024}
	bs, _ := io.ReadAll(lr)
	if err := json.Unmarshal(bs, &rep); err != nil {
		log.Error("Failed to decode JSON", slogutil.Error(err))
		http.Error(w, "JSON Decode Error", http.StatusBadRequest)
		return
	}

	rep.Received = time.Now()
	rep.Date = rep.Received.UTC().Format("20060102")
	rep.Address = addr

	if err := rep.Validate(); err != nil {
		log.Error("Failed to validate report", slogutil.Error(err))
		http.Error(w, "Validation Error", http.StatusBadRequest)
		return
	}

	switch s.store.AddLive(&rep) {
	case liveAccepted:
		result = "accept"
	case liveReplaced:
		result = "replace"
	case liveIgnoredDisabled:
		result = "disabled"
	}
}

// handleImport accepts a regional report archive (zip) and reconciles it
// into the canonical report set. Corrupt archives and manifest conflicts
// are rejected with 400 without touching state; a stale (already
// superseded) source version is rejected with 409.
func (s *server) handleImport(w http.ResponseWriter, r *http.Request) {
	result := "reject"
	defer func() {
		metricBatchesTotal.WithLabelValues(result).Inc()
	}()

	defer r.Body.Close()

	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Spool to a temp file because zip needs random access.
	tmp, err := os.CreateTemp("", "urbatch-*.zip")
	if err != nil {
		http.Error(w, "spool failed", http.StatusInternalServerError)
		return
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, r.Body); err != nil {
		_ = tmp.Close()
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}
	fi, err := tmp.Stat()
	if err != nil {
		_ = tmp.Close()
		http.Error(w, "stat failed", http.StatusInternalServerError)
		return
	}
	zr, err := zip.NewReader(tmp, fi.Size())
	if err != nil {
		_ = tmp.Close()
		http.Error(w, "corrupt archive: "+err.Error(), http.StatusBadRequest)
		return
	}

	batch, err := parseBatch(zr)
	_ = tmp.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := s.store.ApplyBatch(batch)
	if err != nil {
		if errors.Is(err, ErrStaleBatch) {
			result = "stale"
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		result = "error"
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if res.Replayed {
		result = "replayed"
	} else {
		result = "applied"
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// enrichReport fills in derived fields on a raw report.
func (s *server) enrichReport(rep *contract.Report) {
	if s.geo != nil {
		if ip := net.ParseIP(rep.Address); ip != nil {
			if city, err := s.geo.City(ip); err == nil {
				rep.Country = city.Country.Names["en"]
				rep.CountryCode = city.Country.IsoCode
			}
		}
	}
	if rep.Country == "" {
		rep.Country = "Unknown"
	}
	if rep.CountryCode == "" {
		rep.CountryCode = "ZZ"
	}

	rep.Version = transformVersion(rep.Version)
	if strings.Contains(rep.Version, ".") {
		split := strings.SplitN(rep.Version, ".", 3)
		if len(split) == 3 {
			rep.MajorVersion = strings.Join(split[:2], ".")
		}
	}
	rep.OS, rep.Arch, _ = strings.Cut(rep.Platform, "-")

	if m := compilerRe.FindStringSubmatch(rep.LongVersion); len(m) == 3 {
		rep.Compiler = m[1]
		rep.Builder = m[2]
	}
	for _, d := range knownDistributions {
		if d.matcher.MatchString(rep.LongVersion) {
			rep.Distribution = d.distribution
			break
		}
	}
	rep.DistDist = rep.Distribution
	rep.DistOS = rep.OS
	rep.DistArch = rep.Arch

	if strings.HasPrefix(rep.Version, "v2.") {
		rep.Database.ModernCSQLite = strings.Contains(rep.LongVersion, "modernc-sqlite")
		rep.Database.MattnSQLite = !rep.Database.ModernCSQLite
	} else {
		rep.Database.LevelDB = true
	}
}

var (
	plusRe  = regexp.MustCompile(`(\+.*|[.-]dev\..*)$`)
	plusStr = "-dev"
)

// transformVersion returns a version number formatted correctly, with all
// development versions aggregated into one.
func transformVersion(v string) string {
	if v == "unknown-dev" {
		return v
	}
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	v = plusRe.ReplaceAllString(v, plusStr)

	return v
}
