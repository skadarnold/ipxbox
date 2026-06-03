// Server-side per-client facts (RTT, source IP) for the dashboard.
//
// Copyright (C) 2025-2026 Daniel Arnold. Part of a community fork of ipxbox.
// GPL-2.0-or-later.
//
// The tap can infer who is talking to whom, but it cannot accurately measure
// per-client round-trip latency (server pings are broadcast, so ping/reply
// pairing on the connectionless stream is ambiguous) nor the client's source IP
// (the tap sees IPX, not UDP). The dosbox server DOES know both — it pings each
// client directly and holds its UDP remote address. So the server reports those
// facts here, keyed by the client's IPX node address, and the dashboard merges
// them into the node view.
//
// This keeps the web module decoupled: the server depends only on the small
// Reporter interface (wired in ipxbox.go), not on the dashboard internals.
package web

import (
	"net"
	"sync"

	"github.com/fragglet/ipxbox/ipx"
)

// ClientReport carries the server-known facts about one connected client.
type clientReport struct {
	ip    string // source UDP address (host:port)
	rttMs int64  // most recent server<->client round-trip, ms (0 = unknown)
}

// reporter stores client reports keyed by IPX node address string.
type reporter struct {
	mu sync.Mutex
	m  map[string]clientReport
}

func newReporter() *reporter {
	return &reporter{m: map[string]clientReport{}}
}

func (r *reporter) get(node string) (clientReport, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep, ok := r.m[node]
	return rep, ok
}

// ReportClient records/updates a client's server-known facts. node is the IPX
// address string ("02:xx:.."); ip is the UDP remote address; rttMs is the
// latest measured round-trip (pass <0 to leave the existing RTT unchanged, e.g.
// on first connect before any ping has completed).
func (r *reporter) ReportClient(node, ip string, rttMs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := r.m[node]
	if ip != "" {
		rep.ip = ip
	}
	if rttMs >= 0 {
		rep.rttMs = rttMs
	}
	r.m[node] = rep
}

// RemoveClient drops a client's report on disconnect.
func (r *reporter) RemoveClient(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, node)
}

// observerAdapter adapts a reporter to the dosbox.ClientObserver interface so
// the dosbox server can report client facts without importing this package's
// internals. The module hands one to ipxbox.go, which sets it on the server.
type observerAdapter struct{ r *reporter }

func (o observerAdapter) ClientConnected(node ipx.Addr, remoteAddr net.Addr) {
	ip := ""
	if remoteAddr != nil {
		ip = remoteAddr.String()
	}
	o.r.ReportClient(node.String(), ip, -1) // RTT comes from our own pinger
}

func (o observerAdapter) ClientDisconnected(node ipx.Addr) {
	o.r.RemoveClient(node.String())
}
