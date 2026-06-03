// Package web provides an optional ipxbox module that serves a read-only web
// dashboard: the live roster of connected clients and a topology of who is
// exchanging traffic with whom, inferred from packets observed on a network tap.
//
// Copyright (C) 2025-2026 Daniel Arnold. Part of a community fork of ipxbox.
// GPL-2.0-or-later (same terms as the rest of the project).
//
// IPX (the DOSbox protocol ipxbox speaks) is connectionless: there is no
// disconnect event. So liveness here is inferred from a timeout — a node or link
// is "active" while traffic is recent, then decays to idle/stale and is finally
// pruned. The server does know its own registered nodes, but to show the links
// *between* clients (the interesting part) we watch the source/dest of every
// packet on a tap.
package web

import (
	"sort"
	"sync"
	"time"

	"github.com/fragglet/ipxbox/ipx"
)

// Liveness thresholds since a node/edge was last seen.
const (
	activeWithin = 5 * time.Second
	idleWithin   = 30 * time.Second
	pruneAfter   = 5 * time.Minute
)

// NodeView is one observed participant for the dashboard JSON.
type NodeView struct {
	Node     string `json:"node"`     // "02:xx:.." address
	Packets  uint64 `json:"packets"`  // packets seen involving this node
	Liveness string `json:"liveness"` // active | idle | stale
}

// EdgeView is an observed link between two nodes (they are exchanging traffic).
type EdgeView struct {
	A        string `json:"a"`
	B        string `json:"b"`
	Packets  uint64 `json:"packets"`
	Liveness string `json:"liveness"`
}

// Snapshot is the whole dashboard view.
type Snapshot struct {
	Nodes []NodeView `json:"nodes"`
	Edges []EdgeView `json:"edges"`
}

type nodeState struct {
	packets  uint64
	lastSeen time.Time
}

type edgeState struct {
	a, b     string
	packets  uint64
	lastSeen time.Time
}

// tracker accumulates topology from observed packets. Safe for concurrent use.
type tracker struct {
	mu    sync.Mutex
	nodes map[string]*nodeState
	edges map[string]*edgeState
	now   func() time.Time
}

func newTracker() *tracker {
	return &tracker{
		nodes: map[string]*nodeState{},
		edges: map[string]*edgeState{},
		now:   time.Now,
	}
}

// addrPingReply is the synthetic address IPXBox sends server keepalive pings
// from (see server/dosbox/protocol.go). It is server infrastructure, not a
// client, so it must not appear in the topology as a participant.
var addrPingReply = ipx.Addr{0x02, 0xff, 0xff, 0xff, 0x00, 0x00}

// trackable excludes broadcast/null/server-internal pseudo-addresses from the
// node/edge graph, so only real client nodes are shown.
func trackable(a ipx.Addr) bool {
	return a != ipx.AddrBroadcast && a != ipx.AddrNull && a != addrPingReply
}

// observe records one packet's endpoints.
func (t *tracker) observe(pkt *ipx.Packet) {
	if pkt == nil {
		return
	}
	src := pkt.Header.Src.Addr
	dst := pkt.Header.Dest.Addr
	now := t.now()

	t.mu.Lock()
	defer t.mu.Unlock()
	if trackable(src) {
		t.touchNode(src.String(), now)
	}
	if trackable(dst) {
		t.touchNode(dst.String(), now)
	}
	// A broadcast isn't a point-to-point link; only unicast pairs form an edge.
	if trackable(src) && trackable(dst) && src != dst {
		t.touchEdge(src.String(), dst.String(), now)
	}
}

func (t *tracker) touchNode(node string, now time.Time) {
	n := t.nodes[node]
	if n == nil {
		n = &nodeState{}
		t.nodes[node] = n
	}
	n.packets++
	n.lastSeen = now
}

func (t *tracker) touchEdge(a, b string, now time.Time) {
	if a > b {
		a, b = b, a
	}
	key := a + "|" + b
	e := t.edges[key]
	if e == nil {
		e = &edgeState{a: a, b: b}
		t.edges[key] = e
	}
	e.packets++
	e.lastSeen = now
}

func liveness(lastSeen, now time.Time) string {
	switch age := now.Sub(lastSeen); {
	case age <= activeWithin:
		return "active"
	case age <= idleWithin:
		return "idle"
	default:
		return "stale"
	}
}

// snapshot returns the current view, pruning anything past the prune timeout.
func (t *tracker) snapshot() Snapshot {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()

	for k, n := range t.nodes {
		if now.Sub(n.lastSeen) > pruneAfter {
			delete(t.nodes, k)
		}
	}
	for k, e := range t.edges {
		if now.Sub(e.lastSeen) > pruneAfter {
			delete(t.edges, k)
		}
	}

	out := Snapshot{
		Nodes: make([]NodeView, 0, len(t.nodes)),
		Edges: make([]EdgeView, 0, len(t.edges)),
	}
	for node, n := range t.nodes {
		out.Nodes = append(out.Nodes, NodeView{
			Node:     node,
			Packets:  n.packets,
			Liveness: liveness(n.lastSeen, now),
		})
	}
	for _, e := range t.edges {
		out.Edges = append(out.Edges, EdgeView{
			A:        e.a,
			B:        e.b,
			Packets:  e.packets,
			Liveness: liveness(e.lastSeen, now),
		})
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].Node < out.Nodes[j].Node })
	sort.Slice(out.Edges, func(i, j int) bool {
		if out.Edges[i].A != out.Edges[j].A {
			return out.Edges[i].A < out.Edges[j].A
		}
		return out.Edges[i].B < out.Edges[j].B
	})
	return out
}
