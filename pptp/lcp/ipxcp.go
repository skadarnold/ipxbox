package lcp

import (
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

const PPPTypeIPXCP = layers.PPPType(0x802B)

var LayerTypeIPXCP = gopacket.RegisterLayerType(1819, gopacket.LayerTypeMetadata{
	Name:    "IPXCP",
	Decoder: gopacket.DecodeFunc(decodeIPXCP),
})

// TODO: Implement SerializeTo and make this SerializableLayer.
var _ = gopacket.Layer(&LCP{})

var (
	OptionIPXNetwork               = OptionType(1)
	OptionIPXNode                  = OptionType(2)
	OptionIPXCompressionProtocol   = OptionType(3)
	OptionIPXRoutingProtocol       = OptionType(4)
	OptionIPXRouterName            = OptionType(5)
	OptionIPXConfigurationComplete = OptionType(6)
)

// IPXCP is a gopacket layer for the PPP IPX Control Protocol.
type IPXCP struct {
	BaseLayer
}

func (l *IPXCP) LayerType() gopacket.LayerType {
	return LayerTypeIPXCP
}

func decodeIPXCP(data []byte, p gopacket.PacketBuilder) error {
	ipxcp := &IPXCP{}
	ipxcp.PPPType = PPPTypeIPXCP
	if err := ipxcp.UnmarshalBinary(data); err != nil {
		return err
	}
	p.AddLayer(ipxcp)
	return nil
}