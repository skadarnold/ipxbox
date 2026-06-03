// Web dashboard module for ipxbox.
//
// Copyright (C) 2025-2026 Daniel Arnold. Part of a community fork of ipxbox.
// GPL-2.0-or-later.
//
// This is an optional module (enabled with -web) that serves a read-only
// browser dashboard showing connected clients and a live traffic topology. It
// observes traffic via a network tap (see TapSource), so it needs the tappable
// layer of the network stack; ipxbox.go wires that in. All dashboard logic lives
// here so the footprint on the rest of the tree stays tiny.

package web

import (
	"context"
	"flag"
	"net/http"
	"strings"
	"time"

	"github.com/fragglet/ipxbox/ipx"
	"github.com/fragglet/ipxbox/module"
)

// TapSource is the minimal interface the module needs to snoop traffic: a
// factory for a packet reader. *tappable.TappableNetwork satisfies it via
// NewTap(). It is set by ipxbox.go before the module starts.
type TapSource interface {
	NewTap() ipx.ReadCloser
}

type mod struct {
	enabled *bool
	addr    *string
	tap     TapSource
	tr      *tracker
}

// Module is the singleton registered in ipxbox.go. Call SetTapSource before
// Start so the dashboard can observe traffic.
var Module = &mod{}

// SetTapSource gives the module the tappable network to snoop. Must be called
// (with a non-nil source) before Start, or the dashboard runs with no traffic
// view. ipxbox.go does this when building the network stack.
func SetTapSource(t TapSource) { Module.tap = t }

// Observer returns a dosbox.ClientObserver-compatible value that feeds
// server-known client facts (source IP) into the dashboard. The server module
// sets this on the dosbox Protocol. It is typed as `any` here so this package
// does not import server/dosbox (avoiding a dependency-direction concern); the
// concrete type satisfies dosbox.ClientObserver and the caller asserts it.
//
// The tracker is created in Initialize() (below), so it is always present by the
// time Observer() is called.
func Observer() any {
	return observerAdapter{r: Module.tr.reporter}
}

func (m *mod) Initialize() {
	m.enabled = flag.Bool("web", false, "Serve a read-only web dashboard of connected clients and traffic topology.")
	m.addr = flag.String("web_addr", "8000", "Listen address for the -web dashboard. A bare port (8000) listens on all interfaces; use host:port (127.0.0.1:8000) to bind a specific interface.")
	// Create the tracker here, in Initialize(): the aggregate runs every
	// module's Initialize() sequentially BEFORE starting any module, but the
	// Start() calls run CONCURRENTLY. Creating the shared tracker here (rather
	// than lazily in Start()/Observer()) means the server module's Observer()
	// call and this module's Start() are guaranteed to use the same instance —
	// otherwise they could race and build two, and the observer's IP data would
	// never reach the dashboard.
	m.tr = newTracker()
}

// normalizeAddr accepts either a bare port ("8000") or a full host:port
// (":8000", "127.0.0.1:8000") and returns a value http.Server understands. A
// bare port is treated as ":<port>" (all interfaces).
func normalizeAddr(addr string) string {
	if addr == "" {
		return ":8000"
	}
	// Already contains a colon -> assume host:port (or ":port"); use as-is.
	if strings.ContainsRune(addr, ':') {
		return addr
	}
	// Bare port number -> listen on all interfaces.
	return ":" + addr
}

func (m *mod) Start(ctx context.Context, params *module.Parameters) error {
	if !*m.enabled {
		return module.NotNeeded
	}

	tr := m.tr // created in Initialize()

	// Drain a tap into the tracker, if we were given a tap source. Without one
	// the dashboard still serves (roster/topology just stay empty).
	if m.tap != nil {
		go func() {
			tap := m.tap.NewTap()
			defer tap.Close()
			for {
				pkt, err := tap.ReadPacket(ctx)
				if err != nil {
					return
				}
				tr.observe(pkt)
			}
		}()
	} else if params.Logger != nil {
		params.Logger.Warn("web dashboard: no tap source set; topology will be empty")
	}

	// Active RTT: our own node broadcasts a probe each interval and times each
	// client's reply (works during active play; no server-side coupling).
	go runPinger(ctx, params.Network, tr)

	addr := normalizeAddr(*m.addr)
	srv := newServer(tr)
	httpSrv := &http.Server{Addr: addr, Handler: srv.mux}

	if params.Logger != nil {
		params.Logger.Info("web dashboard listening", "addr", addr)
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		httpSrv.Shutdown(shutCtx)
	}()

	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
