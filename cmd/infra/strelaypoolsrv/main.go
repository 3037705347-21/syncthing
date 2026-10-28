// Copyright (C) 2015 Audrius Butkevicius and Contributors (see the CONTRIBUTORS file).

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/syncthing/syncthing/cmd/infra/strelaypoolsrv/auto"
	"github.com/syncthing/syncthing/lib/assets"
	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/geoip"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
	"github.com/syncthing/syncthing/lib/relay/client"
	"github.com/syncthing/syncthing/lib/tlsutil"
)

type location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	City      string  `json:"city"`
	Country   string  `json:"country"`
	Continent string  `json:"continent"`
}

type relay struct {
	URL            string   `json:"url"`
	Location       location `json:"location"`
	uri            *url.URL
	Stats          *stats    `json:"stats"`
	StatsRetrieved time.Time `json:"statsRetrieved"`
}

type relayShort struct {
	URL string `json:"url"`
}

type stats struct {
	StartTime          time.Time `json:"startTime"`
	UptimeSeconds      int       `json:"uptimeSeconds"`
	PendingSessionKeys int       `json:"numPendingSessionKeys"`
	ActiveSessions     int       `json:"numActiveSessions"`
	Connections        int       `json:"numConnections"`
	Proxies            int       `json:"numProxies"`
	BytesProxied       int       `json:"bytesProxied"`
	GoVersion          string    `json:"goVersion"`
	GoOS               string    `json:"goOS"`
	GoArch             string    `json:"goArch"`
	GoMaxProcs         int       `json:"goMaxProcs"`
	GoRoutines         int       `json:"goNumRoutine"`
	Rates              []int64   `json:"kbps10s1m5m15m30m60m"`
	Options            struct {
		NetworkTimeout int      `json:"network-timeout"`
		PintInterval   int      `json:"ping-interval"`
		MessageTimeout int      `json:"message-timeout"`
		SessionRate    int      `json:"per-session-rate"`
		GlobalRate     int      `json:"global-rate"`
		Pools          []string `json:"pools"`
		ProvidedBy     string   `json:"provided-by"`
	} `json:"options"`
}

func (r relay) String() string {
	return r.URL
}

type request struct {
	relay      *relay
	identity   string
	result     chan result
	queueTimer *prometheus.Timer
}

type result struct {
	err          error
	eviction     time.Duration
	commitFailed bool
}

var (
	testCert          tls.Certificate
	knownRelaysFile   = filepath.Join(os.TempDir(), "strelaypoolsrv_known_relays")
	leasesFile        = filepath.Join(os.TempDir(), "strelaypoolsrv_leases.json")
	publishedListFile = filepath.Join(os.TempDir(), "strelaypoolsrv_published.json")
	listen            = ":80"
	metricsListen     = ":8081"
	dir               string
	evictionTime      = time.Hour
	debug             bool
	permRelaysFile    string
	ipHeader          string
	proto             string
	statsRefresh      = time.Minute
	requestQueueLen   = 64
	requestProcessors = 8
	geoipLicenseKey   = os.Getenv("GEOIP_LICENSE_KEY")
	geoipAccountID, _ = strconv.Atoi(os.Getenv("GEOIP_ACCOUNT_ID"))
	maxRelaysReturned = 100

	requests chan request

	store           *relayStore
	permanentRelays = make([]*relay, 0)
	globalBlocklist = newErrorTracker(1000)

	// probeRelay is the health check probe; it is a variable so tests can
	// replace it. Each report gets one probe: consecutive successes are
	// counted across reports in the lease state machine.
	probeRelay = client.TestRelay
)

const (
	httpStatusEnhanceYourCalm = 429
	maxJSONBodyBytes          = 8 << 10
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Lshortfile)

	flag.StringVar(&listen, "listen", listen, "Listen address")
	flag.StringVar(&metricsListen, "metrics-listen", metricsListen, "Metrics listen address")
	flag.StringVar(&dir, "keys", dir, "Directory where http-cert.pem and http-key.pem is stored for TLS listening")
	flag.BoolVar(&debug, "debug", debug, "Enable debug output")
	flag.DurationVar(&evictionTime, "eviction", evictionTime, "After how long the relay is evicted")
	flag.StringVar(&permRelaysFile, "perm-relays", "", "Path to list of permanent relays")
	flag.StringVar(&knownRelaysFile, "known-relays", knownRelaysFile, "Path to legacy list of current relays (migrated on first start)")
	flag.StringVar(&leasesFile, "leases-file", leasesFile, "Path to the admission lease ledger")
	flag.StringVar(&publishedListFile, "published-file", publishedListFile, "Path to the published relay list")
	flag.StringVar(&ipHeader, "ip-header", "", "Name of header which holds clients ip:port. Only meaningful when running behind a reverse proxy.")
	flag.StringVar(&proto, "protocol", "tcp", "Protocol used for listening. 'tcp' for IPv4 and IPv6, 'tcp4' for IPv4, 'tcp6' for IPv6")
	flag.DurationVar(&statsRefresh, "stats-refresh", statsRefresh, "Interval at which to refresh relay stats")
	flag.IntVar(&requestQueueLen, "request-queue", requestQueueLen, "Queue length for incoming test requests")
	flag.IntVar(&requestProcessors, "request-processors", requestProcessors, "Number of request processor routines")
	flag.StringVar(&geoipLicenseKey, "geoip-license-key", geoipLicenseKey, "License key for GeoIP database")
	flag.IntVar(&maxRelaysReturned, "max-relays-returned", maxRelaysReturned, "Maximum number of relays returned for a normal endpoint query")

	flag.Parse()

	log.Println(build.LongVersionFor("strelaypoolsrv"))

	requests = make(chan request, requestQueueLen)
	geoip, err := geoip.NewGeoLite2CityProvider(context.Background(), geoipAccountID, geoipLicenseKey, os.TempDir())
	if err != nil {
		log.Fatalln("Failed to create GeoIP provider:", err)
	}
	go geoip.Serve(context.TODO())

	var listener net.Listener

	if permRelaysFile != "" {
		permanentRelays = loadRelays(permRelaysFile, geoip)
	}

	testCert = createTestCertificate()

	// The committed lease ledger and published list are authoritative:
	// after a restart we serve exactly the last committed generation
	// (including pending switch state and lease deadlines) instead of
	// re-admitting relays through health checks at boot.
	var storeErr error
	store, storeErr = newRelayStore(leasesFile, publishedListFile, knownRelaysFile, geoip)
	if storeErr != nil {
		log.Fatalln("Failed to load relay lease store:", storeErr)
	}

	for range requestProcessors {
		go requestProcessor(geoip)
	}

	go statsRefresher(statsRefresh)

	if dir != "" {
		if debug {
			log.Println("Starting TLS listener on", listen)
		}
		certFile, keyFile := filepath.Join(dir, "http-cert.pem"), filepath.Join(dir, "http-key.pem")
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatalln("Failed to load HTTP X509 key pair:", err)
		}

		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS10, // No SSLv3
			ClientAuth:   tls.RequestClientCert,
			CipherSuites: []uint16{
				// No RC4
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
				tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
			},
		}

		listener, err = tls.Listen(proto, listen, tlsCfg)
	} else {
		if debug {
			log.Println("Starting plain listener on", listen)
		}
		listener, err = net.Listen(proto, listen)
	}

	if err != nil {
		log.Fatalln("listen:", err)
	}

	if metricsListen != "" {
		mmux := http.NewServeMux()
		mmux.HandleFunc("/metrics", handleMetrics)
		go func() {
			if err := http.ListenAndServe(metricsListen, mmux); err != nil {
				log.Fatalln("HTTP serve metrics:", err)
			}
		}()
	}

	getMux := http.NewServeMux()
	getMux.HandleFunc("/", handleAssets)
	getMux.HandleFunc("/endpoint", withAPIMetrics(handleEndpointShort))
	getMux.HandleFunc("/endpoint/full", withAPIMetrics(handleEndpointFull))

	postMux := http.NewServeMux()
	postMux.HandleFunc("/endpoint", withAPIMetrics(handleRegister))
	postMux.HandleFunc("/endpoint/revoke", withAPIMetrics(handleRevoke))

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			getMux.ServeHTTP(w, r)
		case http.MethodPost:
			postMux.ServeHTTP(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	srv := http.Server{
		Handler:     handler,
		ReadTimeout: 10 * time.Second,
	}
	srv.SetKeepAlivesEnabled(false)

	err = srv.Serve(listener)
	if err != nil {
		log.Fatalln("serve:", err)
	}
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(metricsRequestsSeconds)
	// Acquire the mutex just to make sure we're not caught mid-way stats collection
	store.mu.RLock()
	promhttp.Handler().ServeHTTP(w, r)
	store.mu.RUnlock()
	timer.ObserveDuration()
}

func handleAssets(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-cache, must-revalidate")

	path := r.URL.Path[1:]
	if path == "" {
		path = "index.html"
	}

	as, ok := auto.Assets()[path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	assets.Serve(w, r, as)
}

func withAPIMetrics(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		timer := prometheus.NewTimer(apiRequestsSeconds.WithLabelValues(r.Method))
		w = NewLoggingResponseWriter(w)
		defer func() {
			timer.ObserveDuration()
			lw := w.(*loggingResponseWriter)
			apiRequestsTotal.WithLabelValues(r.Method, strconv.Itoa(lw.statusCode)).Inc()
		}()
		next(w, r)
	}
}

// handleEndpointFull returns the relay list with full metadata and
// statistics. Large, and expensive.
func handleEndpointFull(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	gen, dynamic := store.snapshot()
	relays := make([]*relay, 0, len(permanentRelays)+len(dynamic))
	relays = append(relays, permanentRelays...)
	relays = append(relays, dynamic...)
	rw.Header().Set(generationHeader, strconv.FormatUint(gen, 10))

	_ = json.NewEncoder(rw).Encode(map[string]any{
		"relays":     relays,
		"generation": gen,
	})
}

// handleEndpointShort returns the relay list with only the URL.
func handleEndpointShort(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	gen, dynamic := store.snapshot()
	relays := make([]relayShort, 0, len(permanentRelays)+len(dynamic))
	for _, r := range permanentRelays {
		relays = append(relays, relayShort{URL: slimURL(r.URL)})
	}
	for _, r := range dynamic {
		relays = append(relays, relayShort{URL: slimURL(r.URL)})
	}
	if len(relays) > maxRelaysReturned {
		rand.Shuffle(relays)
		relays = relays[:maxRelaysReturned]
	}
	rw.Header().Set(generationHeader, strconv.FormatUint(gen, 10))

	_ = json.NewEncoder(rw).Encode(map[string]any{
		"relays":     relays,
		"generation": gen,
	})
}

// handleRevoke explicitly revokes (takes offline) the relay identity
// proven by the presented TLS client certificate.
func handleRevoke(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "client certificate required", http.StatusUnauthorized)
		return
	}
	id := protocol.NewDeviceID(r.TLS.PeerCertificates[0].Raw).String()

	existed, err := store.revoke(id)
	if err != nil {
		log.Println("Failed to commit revocation for", id, ":", err)
		http.Error(w, "failed to commit revocation", http.StatusInternalServerError)
		return
	}
	if !existed {
		http.Error(w, "unknown relay identity", http.StatusNotFound)
		return
	}
	log.Println("Relay identity", id, "explicitly revoked")
	w.WriteHeader(http.StatusNoContent)
}

func handleRegister(w http.ResponseWriter, r *http.Request) {
	// Get the IP address of the client
	rhost := r.RemoteAddr
	if ipHeader != "" {
		hdr := r.Header.Get(ipHeader)
		fields := strings.Split(hdr, ",")
		if len(fields) > 0 {
			rhost = strings.TrimSpace(fields[len(fields)-1])
		}
	}
	if host, _, err := net.SplitHostPort(rhost); err == nil {
		rhost = host
	}

	// Check the black list. A client is blacklisted if their last 10
	// attempts to join have all failed. The "Unauthorized" status return
	// causes strelaysrv to cease attempting to join.
	if globalBlocklist.IsBlocked(rhost) {
		log.Println("Rejected blocked client", rhost)
		http.Error(w, "Too many errors", http.StatusUnauthorized)
		globalBlocklist.ClearErrors(rhost)
		return
	}

	var relayCert *x509.Certificate
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		relayCert = r.TLS.PeerCertificates[0]
		log.Printf("Got TLS cert from relay server")
	}

	var newRelay relay
	err := json.NewDecoder(r.Body).Decode(&newRelay)
	r.Body.Close()

	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			if debug {
				log.Println("Payload too large")
			}
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if debug {
			log.Println("Failed to parse payload")
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	uri, err := url.Parse(newRelay.URL)
	if err != nil {
		if debug {
			log.Println("Failed to parse URI", newRelay.URL)
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Canonicalize the URL. In particular, parse and re-encode the query
	// string so that it's guaranteed to be valid.
	uri.RawQuery = uri.Query().Encode()
	newRelay.URL = uri.String()

	if relayCert != nil {
		advertisedId := uri.Query().Get("id")
		idFromCert := protocol.NewDeviceID(relayCert.Raw).String()
		if advertisedId != idFromCert {
			log.Println("Warning: Relay server requested to join with an ID different from the join request, rejecting")
			http.Error(w, "mismatched advertised id and join request cert", http.StatusBadRequest)
			return
		}
	}

	host, port, err := net.SplitHostPort(uri.Host)
	if err != nil {
		if debug {
			log.Println("Failed to split URI", newRelay.URL)
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ip := net.ParseIP(host)
	// The client did not provide an IP address, use the IP address of the client.
	if ip == nil || ip.IsUnspecified() {
		uri.Host = net.JoinHostPort(rhost, port)
		newRelay.URL = uri.String()
	} else if host != rhost && relayCert == nil {
		if debug {
			log.Println("IP address advertised does not match client IP address", rhost, uri)
		}
		http.Error(w, fmt.Sprintf("IP advertised %s does not match client IP %s", host, rhost), http.StatusUnauthorized)
		return
	}

	newRelay.uri = uri
	identity := identityForURI(uri)

	for _, current := range permanentRelays {
		if current.uri.Host == newRelay.uri.Host {
			if debug {
				log.Println("Asked to add a relay", newRelay, "which exists in permanent list")
			}
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
	}

	reschan := make(chan result)

	select {
	case requests <- request{relay: &newRelay, identity: identity, result: reschan, queueTimer: prometheus.NewTimer(relayTestActionsSeconds.WithLabelValues("queue"))}:
		result := <-reschan
		if result.err != nil {
			if result.commitFailed {
				// The health check passed but the generation commit
				// (lease ledger or published list write) failed. Nothing
				// was admitted; this is a server-side failure, not a
				// bad relay, so it must not feed the client blocklist.
				log.Println("Join from", rhost, "not committed:", result.err)
				relayTestsTotal.WithLabelValues("failed").Inc()
				http.Error(w, result.err.Error(), http.StatusInternalServerError)
				return
			}
			log.Println("Join from", rhost, "failed:", result.err)
			globalBlocklist.AddError(rhost)
			relayTestsTotal.WithLabelValues("failed").Inc()
			http.Error(w, result.err.Error(), http.StatusBadRequest)
			return
		}
		log.Println("Join from", rhost, "succeeded")
		globalBlocklist.ClearErrors(rhost)
		relayTestsTotal.WithLabelValues("success").Inc()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(map[string]time.Duration{
			"evictionIn": result.eviction,
		})

	default:
		relayTestsTotal.WithLabelValues("dropped").Inc()
		if debug {
			log.Println("Dropping request")
		}
		w.WriteHeader(httpStatusEnhanceYourCalm)
	}
}

func requestProcessor(geoip *geoip.Provider) {
	for request := range requests {
		if request.queueTimer != nil {
			request.queueTimer.ObserveDuration()
		}

		timer := prometheus.NewTimer(relayTestActionsSeconds.WithLabelValues("test"))
		handleRelayTest(request, geoip)
		timer.ObserveDuration()
	}
}

func handleRelayTest(request request, geoip *geoip.Provider) {
	if debug {
		log.Println("Request for", request.relay)
	}
	// A single probe per report: the consecutive-success requirement is
	// counted across reports by the lease state machine.
	if err := probeRelay(context.TODO(), request.relay.uri, []tls.Certificate{testCert}, time.Second, 2*time.Second, 1); err != nil {
		if debug {
			log.Println("Test for relay", request.relay, "failed:", err)
		}
		// A failed check resets the pending counter for this candidate;
		// the published address is never touched.
		if commitErr := store.reportFailure(request.identity, request.relay.URL); commitErr != nil {
			log.Println("Failed to commit health-check failure for", request.identity, ":", commitErr)
		}
		request.result <- result{err: err}
		return
	}

	stats := fetchStats(request.relay)
	location := getLocation(request.relay.uri.Host, geoip)
	request.relay.Stats = stats
	request.relay.StatsRetrieved = time.Now().Truncate(time.Second)
	request.relay.Location = location

	// The address enters pending state and replaces the active one only
	// when the check count is reached; the commit swaps the list and
	// metrics atomically. If either state file fails to write, the whole
	// admission is a no-op.
	if err := store.admit(request.identity, request.relay, evictionTime); err != nil {
		request.result <- result{err: err, commitFailed: true}
		return
	}

	request.result <- result{eviction: evictionTime}
}

func loadRelays(file string, geoip *geoip.Provider) []*relay {
	content, err := os.ReadFile(file)
	if err != nil {
		log.Println("Failed to load relays: " + err.Error())
		return nil
	}

	var relays []*relay
	for line := range strings.SplitSeq(string(content), "\n") {
		if line == "" {
			continue
		}

		uri, err := url.Parse(line)
		if err != nil {
			if debug {
				log.Println("Skipping relay", line, "due to parse error", err)
			}
			continue

		}

		relays = append(relays, &relay{
			URL:      line,
			Location: getLocation(uri.Host, geoip),
			uri:      uri,
		})
		if debug {
			log.Println("Adding relay", line)
		}
	}
	return relays
}

func createTestCertificate() tls.Certificate {
	tmpDir, err := os.MkdirTemp("", "relaypoolsrv")
	if err != nil {
		log.Fatal(err)
	}

	certFile, keyFile := filepath.Join(tmpDir, "cert.pem"), filepath.Join(tmpDir, "key.pem")
	cert, err := tlsutil.NewCertificate(certFile, keyFile, "relaypoolsrv", 20*365, false)
	if err != nil {
		log.Fatalln("Failed to create test X509 key pair:", err)
	}

	return cert
}

func getLocation(host string, geoip *geoip.Provider) location {
	timer := prometheus.NewTimer(locationLookupSeconds)
	defer timer.ObserveDuration()

	if geoip == nil {
		return location{}
	}

	addr, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return location{}
	}

	city, err := geoip.City(addr.IP)
	if err != nil {
		return location{}
	}

	return location{
		Longitude: city.Location.Longitude,
		Latitude:  city.Location.Latitude,
		City:      city.City.Names["en"],
		Country:   city.Country.IsoCode,
		Continent: city.Continent.Code,
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter

	statusCode int
}

func NewLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{w, http.StatusOK}
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

type errorTracker struct {
	errors *lru.TwoQueueCache[string, *errorCounter]
}

type errorCounter struct {
	count atomic.Int32
}

func newErrorTracker(size int) *errorTracker {
	cache, err := lru.New2Q[string, *errorCounter](size)
	if err != nil {
		panic(err)
	}
	return &errorTracker{
		errors: cache,
	}
}

func (b *errorTracker) AddError(host string) {
	entry, ok := b.errors.Get(host)
	if !ok {
		entry = &errorCounter{}
		b.errors.Add(host, entry)
	}
	c := entry.count.Add(1)
	log.Printf("Error count for %s is now %d", host, c)
}

func (b *errorTracker) ClearErrors(host string) {
	b.errors.Remove(host)
}

func (b *errorTracker) IsBlocked(host string) bool {
	if be, ok := b.errors.Get(host); ok {
		return be.count.Load() > 10
	}
	return false
}

func slimURL(u string) string {
	p, err := url.Parse(u)
	if err != nil {
		return u
	}
	newQuery := url.Values{}
	if id := p.Query().Get("id"); id != "" {
		newQuery.Set("id", id)
	}
	p.RawQuery = newQuery.Encode()
	return p.String()
}
