// Copyright © 2020 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func init() {
	for i := range 10 {
		u := fmt.Sprintf("permanent%d", i)
		permanentRelays = append(permanentRelays, &relay{URL: u})
	}
}

// The full endpoint serves permanent relays plus the committed dynamic
// relays, annotated with the store generation; permanent relays are never
// touched by the lease machinery.
func TestHandleGetRequest(t *testing.T) {
	s := newTestStore(t)
	for _, id := range []string{"known1", "known2", "known3"} {
		r := testRelay(id, "192.0.2."+id[len(id)-1:]+":22067")
		mustAdmit(t, s, id, r, 3)
	}
	gen, dynamic := s.snapshot()
	if len(dynamic) != 3 {
		t.Fatalf("expected 3 published relays, got %d", len(dynamic))
	}

	prevStore := store
	store = s
	t.Cleanup(func() { store = prevStore })

	w := httptest.NewRecorder()
	w.Body = new(bytes.Buffer)
	handleEndpointFull(w, httptest.NewRequest("GET", "/", nil))

	var resp struct {
		Relays     []*relay `json:"relays"`
		Generation uint64   `json:"generation"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	expect := 3 + len(permanentRelays)
	if len(resp.Relays) != expect {
		t.Errorf("expected %d relays, got %d", expect, len(resp.Relays))
	}
	if resp.Generation != gen {
		t.Errorf("body generation %d != store generation %d", resp.Generation, gen)
	}
	if hdr := w.Header().Get(generationHeader); hdr != fmt.Sprint(gen) {
		t.Errorf("header generation %q != %d", hdr, gen)
	}

	// Check for changes in permanentRelays.
	for i, r := range permanentRelays {
		switch {
		case !strings.HasPrefix(r.URL, "permanent"):
			t.Errorf("relay %q among permanent relays", r.URL)
		case r.URL != fmt.Sprintf("permanent%d", i):
			t.Error("order of permanent relays changed")
		}
	}
}

func TestCanonicalizeQueryValues(t *testing.T) {
	// This just demonstrates and validates the uri.Parse/String stuff in
	// regards to query strings.

	in := "http://example.com/?some weird= query^value"
	exp := "http://example.com/?some+weird=+query%5Evalue"

	uri, err := url.Parse(in)
	if err != nil {
		t.Fatal(err)
	}

	str := uri.String()
	if str != in {
		// Just re-encoding the URL doesn't sanitize the query string.
		t.Errorf("expected %q, got %q", in, str)
	}

	uri.RawQuery = uri.Query().Encode()
	str = uri.String()
	if str != exp {
		// The query string is now in correct format.
		t.Errorf("expected %q, got %q", exp, str)
	}
}

func TestSlimURL(t *testing.T) {
	cases := []struct {
		in, out string
	}{
		{"http://example.com/", "http://example.com/"},
		{"relay://192.0.2.42:22067/?globalLimitBps=0&id=EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M&networkTimeout=2m0s&pingInterval=1m0s&providedBy=Test&sessionLimitBps=0&statusAddr=%3A22070", "relay://192.0.2.42:22067/?id=EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M-EIC6B3M"},
	}

	for _, c := range cases {
		if got := slimURL(c.in); got != c.out {
			t.Errorf("expected %q, got %q", c.out, got)
		}
	}
}
