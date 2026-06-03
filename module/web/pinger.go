// Active RTT measurement for the web dashboard.
//
// Copyright (C) 2025-2026 Daniel Arnold. Part of a community fork of ipxbox.
// GPL-2.0-or-later.
//
// To measure server<->client round-trip latency that works even while a client
// is actively sending game traffic, the dashboard runs its own probe rather than
// piggybacking on the server's keepalive (which only fires when a client is
// idle). It creates its own network node and periodically sends a broadcast
// socket-2 "ping"; every DOSBox client replies to such a packet, addressed back
// to our node with the client's own address as the source (see client/dosbox).
// We time send -> reply per client. This lives entirely in the web module, so it
// adds no coupling to the server's internals.
package web

import (
	"context"
	"sync"
	"time"

	"github.com/fragglet/ipxbox/ipx"
	"github.com/fragglet/ipxbox/network"
)

// pingInterval is how often we broadcast an RTT probe. Kept modest: one tiny
// broadcast per interval, and each connected client sends one small reply.
const pingInterval = 3 * time.Second

// pinger sends periodic broadcast pings from its own node and records the
// round-trip to each replying client into the tracker's reporter.
type pinger struct {
	node     network.Node
	reporter *reporter
	myAddr   ipx.Addr

	mu     sync.Mutex
	sentAt time.Time // when the most recent ping round went out
}

// runPinger creates a node on the given network and runs the probe loop until
// the context is cancelled. Safe to call with a nil network (no-op).
func runPinger(ctx context.Context, net network.Network, tr *tracker) {
	if net == nil {
		return
	}
	node, err := net.NewNode()
	if err != nil {
		return
	}
	defer node.Close()

	p := &pinger{
		node:     node,
		reporter: tr.reporter,
		myAddr:   network.NodeAddress(node),
	}
	// Our own probe node must not appear in the roster/topology as a client.
	tr.ignoreNode(p.myAddr.String())

	// Reader: match each reply to the last ping send-time, by the replying
	// client's source address.
	go p.readLoop(ctx)

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.sendPing()
		}
	}
}

// sendPing broadcasts a socket-2 ping that DOSBox clients reply to.
func (p *pinger) sendPing() {
	p.mu.Lock()
	p.sentAt = time.Now()
	p.mu.Unlock()
	p.node.WritePacket(&ipx.Packet{
		Header: ipx.Header{
			Dest: ipx.HeaderAddr{Addr: ipx.AddrBroadcast, Socket: 2},
			Src:  ipx.HeaderAddr{Addr: p.myAddr, Socket: 2},
		},
	})
}

// readLoop receives ping replies (and any other traffic delivered to our node)
// and records RTT per replying client.
//
// Matching: a reply is timed against the most recent ping (p.sentAt). A reply
// is only accepted if it arrives within one ping interval of that send — older
// "replies" (or stray packets) are ignored rather than producing a bogus value.
// We never overwrite a good RTT with 0; a sub-millisecond round-trip is reported
// as 1 ms so it never renders as "unknown". The reporter retains the last value
// between rounds, so the displayed RTT stays stable instead of flickering.
func (p *pinger) readLoop(ctx context.Context) {
	for {
		pkt, err := p.node.ReadPacket(ctx)
		if err != nil {
			return
		}
		// A reply to our ping is addressed to us; its source is the client.
		if pkt.Header.Dest.Addr != p.myAddr {
			continue
		}
		client := pkt.Header.Src.Addr
		if client == p.myAddr || client == ipx.AddrBroadcast || client == ipx.AddrNull {
			continue
		}
		p.mu.Lock()
		sentAt := p.sentAt
		p.mu.Unlock()
		if sentAt.IsZero() {
			continue
		}
		rtt := time.Since(sentAt)
		// Ignore replies that can't belong to the current outstanding ping.
		if rtt < 0 || rtt > pingInterval {
			continue
		}
		ms := rtt.Milliseconds()
		if ms < 1 {
			ms = 1 // sub-ms LAN round-trip: show 1 ms, not "unknown"
		}
		p.reporter.ReportClient(client.String(), "", ms)
	}
}
