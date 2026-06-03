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
// *between* clients (the interesting part) and per-node traffic, we watch the
// source/dest and size of every packet on a tap.
package web

import (
	"sort"
	"strings"
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
	Node      string `json:"node"`      // "02:xx:.." address
	Packets   uint64 `json:"packets"`   // packets seen involving this node
	Bytes     uint64 `json:"bytes"`     // bytes seen involving this node
	PktRate   int    `json:"pktRate"`   // packets/sec (recent)
	ByteRate  int    `json:"byteRate"`  // bytes/sec (recent)
	FirstSeen int64  `json:"firstSeen"` // unix ms first observed
	AgeSec    int64  `json:"ageSec"`    // seconds since first observed
	Sockets   []int  `json:"sockets"`   // distinct IPX sockets this node has used
	Liveness  string `json:"liveness"`  // active | idle | stale
	// RTT/IP are filled in from the server-side reporter (see reporter.go), not
	// the tap. 0 / "" when unknown.
	RTTms int64  `json:"rttMs"`
	IP    string `json:"ip"`
}

// EdgeView is an observed link between two nodes (they are exchanging traffic).
type EdgeView struct {
	A        string `json:"a"`
	B        string `json:"b"`
	Packets  uint64 `json:"packets"`
	Liveness string `json:"liveness"`
}

// Totals summarizes server-wide traffic for the dashboard cards.
type Totals struct {
	Nodes       int    `json:"nodes"`
	ActiveNodes int    `json:"activeNodes"`
	Edges       int    `json:"edges"`
	Packets     uint64 `json:"packets"`
	Bytes       uint64 `json:"bytes"`
	PktRate     int    `json:"pktRate"`    // server-wide packets/sec (recent)
	ByteRate    int    `json:"byteRate"`   // server-wide bytes/sec (recent)
	Broadcasts  uint64 `json:"broadcasts"` // total broadcast packets seen
	Unicasts    uint64 `json:"unicasts"`   // total unicast packets seen
}

// Snapshot is the whole dashboard view.
type Snapshot struct {
	Nodes  []NodeView `json:"nodes"`
	Edges  []EdgeView `json:"edges"`
	Totals Totals     `json:"totals"`
}

type nodeState struct {
	packets   uint64
	bytes     uint64
	firstSeen time.Time
	lastSeen  time.Time
	sockets   map[uint16]bool
	lastPkts  uint64 // for rate
	lastBytes uint64
	pktRate   int
	byteRate  int
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

	// server-wide totals
	totalPkts     uint64
	totalBytes    uint64
	broadcasts    uint64
	unicasts      uint64
	lastTotalPkts uint64
	lastTotalByte uint64
	totalPktRate  int
	totalByteRate int

	// optional server-side per-client facts (RTT, IP), keyed by node address.
	reporter *reporter

	// ignore holds node addresses that are our own infrastructure (e.g. the RTT
	// pinger's node) and must not appear in the roster/topology as clients.
	ignore map[string]bool

	// events is a ring buffer of recent notable activity (new node, new link,
	// node gone) for the dashboard's activity feed. Kept small and signal-only
	// (not a per-packet firehose).
	events    []Event
	eventSeq  uint64
	seenNodes map[string]bool // for "new node" detection in the feed
}

// Event is one line in the activity feed.
type Event struct {
	Seq  uint64 `json:"seq"`
	TsMs int64  `json:"tsMs"`
	Kind string `json:"kind"` // node-new | node-gone | link-new
	Text string `json:"text"`
}

const maxEvents = 200

func newTracker() *tracker {
	t := &tracker{
		nodes:     map[string]*nodeState{},
		edges:     map[string]*edgeState{},
		now:       time.Now,
		reporter:  newReporter(),
		ignore:    map[string]bool{},
		seenNodes: map[string]bool{},
	}
	go t.rateLoop()
	return t
}

// addEvent appends a notable event to the ring buffer (caller holds t.mu).
func (t *tracker) addEvent(kind, text string) {
	t.eventSeq++
	t.events = append(t.events, Event{
		Seq:  t.eventSeq,
		TsMs: t.now().UnixMilli(),
		Kind: kind,
		Text: text,
	})
	if len(t.events) > maxEvents {
		t.events = t.events[len(t.events)-maxEvents:]
	}
}

// EventsSince returns events with Seq greater than the given value (for the SSE
// feed to send only what's new).
func (t *tracker) EventsSince(seq uint64) []Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := []Event{}
	for _, e := range t.events {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
	return out
}

// ignoreNode marks a node address as our own infrastructure so it is never shown
// as a client (e.g. the RTT pinger's node).
func (t *tracker) ignoreNode(node string) {
	t.mu.Lock()
	t.ignore[node] = true
	delete(t.nodes, node) // drop it if already observed
	t.mu.Unlock()
}

// trackable excludes broadcast/null/server-internal pseudo-addresses.
func trackable(a ipx.Addr) bool {
	return a != ipx.AddrBroadcast && a != ipx.AddrNull && a != addrPingReply
}

// addrPingReply is the synthetic address IPXBox sends server keepalive pings
// from (see server/dosbox/protocol.go) — infrastructure, not a client.
var addrPingReply = ipx.Addr{0x02, 0xff, 0xff, 0xff, 0x00, 0x00}

// observe records one packet's endpoints, size, and socket.
func (t *tracker) observe(pkt *ipx.Packet) {
	if pkt == nil {
		return
	}
	src := pkt.Header.Src.Addr
	dst := pkt.Header.Dest.Addr
	size := uint64(len(pkt.Payload) + ipx.HeaderLength)
	srcSock := pkt.Header.Src.Socket
	now := t.now()

	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalPkts++
	t.totalBytes += size
	if dst == ipx.AddrBroadcast {
		t.broadcasts++
	} else {
		t.unicasts++
	}

	srcStr, dstStr := src.String(), dst.String()
	srcOK := trackable(src) && !t.ignore[srcStr]
	dstOK := trackable(dst) && !t.ignore[dstStr]

	if srcOK {
		if !t.seenNodes[srcStr] {
			t.seenNodes[srcStr] = true
			t.addEvent("node-new", "client appeared: "+shortNodeStr(srcStr))
		}
		t.touchNode(srcStr, size, srcSock, now)
	}
	if dstOK {
		// Count the dest node as involved, but don't double-count bytes/socket
		// against it (the bytes belong to the sender's accounting).
		t.touchNode(dstStr, 0, 0, now)
	}
	if srcOK && dstOK && src != dst {
		key := edgeKeyOf(srcStr, dstStr)
		if _, exists := t.edges[key]; !exists {
			t.addEvent("link-new", "link formed: "+shortNodeStr(srcStr)+" ↔ "+shortNodeStr(dstStr))
		}
		t.touchEdge(srcStr, dstStr, now)
	}
}

// edgeKeyOf returns the canonical (order-independent) key for an edge.
func edgeKeyOf(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// shortNodeStr returns the last 5 chars of a node address for compact display.
func shortNodeStr(n string) string {
	if len(n) > 5 {
		return n[len(n)-5:]
	}
	return n
}

func (t *tracker) touchNode(node string, size uint64, sock uint16, now time.Time) {
	n := t.nodes[node]
	if n == nil {
		n = &nodeState{firstSeen: now, sockets: map[uint16]bool{}}
		t.nodes[node] = n
	}
	n.packets++
	n.bytes += size
	n.lastSeen = now
	if sock != 0 {
		n.sockets[sock] = true
	}
}

func (t *tracker) touchEdge(a, b string, now time.Time) {
	if a > b {
		a, b = b, a
	}
	key := edgeKeyOf(a, b)
	e := t.edges[key]
	if e == nil {
		e = &edgeState{a: a, b: b}
		t.edges[key] = e
	}
	e.packets++
	e.lastSeen = now
}

// rateLoop recomputes per-node and server-wide rates once per second.
func (t *tracker) rateLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		t.mu.Lock()
		for _, n := range t.nodes {
			n.pktRate = int(n.packets - n.lastPkts)
			n.byteRate = int(n.bytes - n.lastBytes)
			n.lastPkts = n.packets
			n.lastBytes = n.bytes
		}
		t.totalPktRate = int(t.totalPkts - t.lastTotalPkts)
		t.totalByteRate = int(t.totalBytes - t.lastTotalByte)
		t.lastTotalPkts = t.totalPkts
		t.lastTotalByte = t.totalBytes
		t.mu.Unlock()
	}
}

// maskIP reduces a "host:port" address to a privacy-preserving hint. For an
// IPv4 address it keeps the first two octets and masks the rest
// (192.168.0.122:64741 -> "192.168.x.x"); anything else becomes "•••". An empty
// input stays empty. The full address is never sent to the unauthenticated UI.
func maskIP(addr string) string {
	if addr == "" {
		return ""
	}
	host := addr
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		host = addr[:i]
	}
	host = strings.Trim(host, "[]") // strip IPv6 brackets if present
	parts := strings.Split(host, ".")
	if len(parts) == 4 {
		return parts[0] + "." + parts[1] + ".x.x"
	}
	return "•••"
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
			delete(t.seenNodes, k)
			t.addEvent("node-gone", "client gone: "+shortNodeStr(k))
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
	active := 0
	for node, n := range t.nodes {
		lv := liveness(n.lastSeen, now)
		if lv == "active" {
			active++
		}
		socks := make([]int, 0, len(n.sockets))
		for s := range n.sockets {
			socks = append(socks, int(s))
		}
		sort.Ints(socks)
		nv := NodeView{
			Node:      node,
			Packets:   n.packets,
			Bytes:     n.bytes,
			PktRate:   n.pktRate,
			ByteRate:  n.byteRate,
			FirstSeen: n.firstSeen.UnixMilli(),
			AgeSec:    int64(now.Sub(n.firstSeen).Seconds()),
			Sockets:   socks,
			Liveness:  lv,
		}
		if t.reporter != nil {
			if rep, ok := t.reporter.get(node); ok {
				nv.RTTms = rep.rttMs
				// Privacy: the raw client IP is NOT sent to the browser. The
				// dashboard is currently unauthenticated and may be reachable on
				// the LAN/internet, so exposing other clients' IPs is a risk. We
				// send only a masked form for now; full IP will be gated behind
				// authentication in a later phase (the reporter still holds the
				// real IP server-side for that future use).
				nv.IP = maskIP(rep.ip)
			}
		}
		out.Nodes = append(out.Nodes, nv)
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

	out.Totals = Totals{
		Nodes:       len(t.nodes),
		ActiveNodes: active,
		Edges:       len(t.edges),
		Packets:     t.totalPkts,
		Bytes:       t.totalBytes,
		PktRate:     t.totalPktRate,
		ByteRate:    t.totalByteRate,
		Broadcasts:  t.broadcasts,
		Unicasts:    t.unicasts,
	}
	return out
}
