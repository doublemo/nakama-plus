package server

import (
	"log"
	"os"
	"time"

	"github.com/doublemo/nakama-kit/kit"
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

type (
	PeerConfig struct {
		Addr                string                  `yaml:"gossip_bindaddr" json:"gossip_bindaddr" usage:"Interface address to bind Nakama to for discovery. By default listening on all interfaces."`
		Port                int                     `yaml:"gossip_bindport" json:"gossip_bindport" usage:"Port number to bind Nakama to for discovery. Default value is 7352."`
		PushPullInterval    int                     `yaml:"push_pull_interval" json:"push_pull_interval" usage:"push_pull_interval is the interval between complete state syncs, Default value is 60 Second"`
		GossipInterval      int                     `yaml:"gossip_interval" json:"gossip_interval" usage:"gossip_interval is the interval after which a node has died that, Default value is 200 Millisecond"`
		TCPTimeout          int                     `yaml:"tcp_timeout" json:"tcp_timeout" usage:"tcp_timeout is the timeout for establishing a stream connection with a remote node for a full state sync, and for stream read and writeoperations, Default value is 10 Second"`
		ProbeTimeout        int                     `yaml:"probe_timeout" json:"probe_timeout" usage:"probe_timeout is the timeout to wait for an ack from a probed node before assuming it is unhealthy. This should be set to 99-percentile of RTT (round-trip time) on your network, Default value is 500 Millisecond"`
		ProbeInterval       int                     `yaml:"probe_interval" json:"probe_interval" usage:"probe_interval is the interval between random node probes. Setting this lower (more frequent) will cause the memberlist cluster to detect failed nodes more quickly at the expense of increased bandwidth usage., Default value is 1 Second"`
		RetransmitMult      int                     `yaml:"retransmit_mult" json:"retransmit_mult" usage:"retransmit_mult is the multiplier used to determine the maximum number of retransmissions attempted, Default value is 2"`
		MaxGossipPacketSize int                     `yaml:"max_gossip_packet_size" json:"max_gossip_packet_size" usage:"max_gossip_packet_size Maximum number of bytes that memberlist will put in a packet (this will be for UDP packets by default with a NetTransport), Default value is 1400"`
		BroadcastQueueSize  int                     `yaml:"broadcast_queue_size" json:"broadcast_queue_size" usage:"broadcast message queue size"`
		Members             []string                `yaml:"members" json:"members" usage:""`
		SecretKey           string                  `yaml:"secret_key" json:"secretKey" usage:" SecretKey is used to initialize the primary encryption key in a keyring.The value should be either 16, 24, or 32 bytes to select AES-128,AES-192, or AES-256."`
		Etcd                *kit.EtcdClientv3Config `yaml:"etcd" json:"etcd" usage:"Etcd config"`
		Weight              int32                   `yaml:"weight" json:"weight" usage:"weight"`
		Balancer            int32                   `yaml:"balancer" json:"balancer" usage:"balancer"`
		Grpc                *kit.GrpcConfig         `yaml:"grpc" json:"grpc" usage:"grpc client setting"`
		LeaderElection      bool                    `yaml:"leader_election" json:"leader_election" usage:"leader_election participates in the Leader election, default is True."`
		Cache               *PeerCacherConfig       `yaml:"cache" json:"cache" usage:"Cache config"`
	}
)

func (c *PeerConfig) Validate(logger *zap.Logger) error {
	if c.Etcd != nil {
		if err := c.Etcd.Valid(); err != nil {
			return err
		}
	}
	return nil
}

func (c *PeerConfig) Clone() *PeerConfig {
	newConfig := &PeerConfig{
		Addr:                c.Addr,
		Port:                c.Port,
		PushPullInterval:    c.PushPullInterval,
		GossipInterval:      c.GossipInterval,
		TCPTimeout:          c.TCPTimeout,
		ProbeTimeout:        c.ProbeTimeout,
		ProbeInterval:       c.ProbeInterval,
		RetransmitMult:      c.RetransmitMult,
		MaxGossipPacketSize: c.MaxGossipPacketSize,
		BroadcastQueueSize:  c.BroadcastQueueSize,
		Members:             make([]string, len(c.Members)),
		SecretKey:           c.SecretKey,
		LeaderElection:      c.LeaderElection,
	}

	copy(newConfig.Members, c.Members)
	if c.Etcd != nil {
		newConfig.Etcd = c.Etcd.Clone()
	}

	if c.Grpc != nil {
		newConfig.Grpc = c.Grpc.Clone()
	}

	if c.Cache != nil {
		c.Cache = &PeerCacherConfig{
			NumCounters: c.Cache.NumCounters,
			MaxCost:     c.Cache.MaxCost,
			BufferItems: c.Cache.BufferItems,
			Prefix:      c.Cache.Prefix,
		}
	}
	return newConfig
}

func toMemberlistConfig(s Peer, name string, c *PeerConfig) *memberlist.Config {
	cfg := memberlist.DefaultLANConfig()
	cfg.BindAddr = c.Addr
	cfg.BindPort = c.Port

	if c.PushPullInterval > 0 {
		cfg.ProbeInterval = time.Duration(c.PushPullInterval) * time.Second
	}

	if c.GossipInterval > 0 {
		cfg.GossipInterval = time.Duration(c.GossipInterval) * time.Second
	}

	if c.TCPTimeout > 0 {
		cfg.TCPTimeout = time.Duration(c.TCPTimeout) * time.Second
	}

	if c.ProbeTimeout > 0 {
		cfg.ProbeTimeout = time.Duration(c.ProbeTimeout) * time.Second
	}

	if c.ProbeInterval > 0 {
		cfg.ProbeInterval = time.Duration(c.ProbeInterval) * time.Second
	}

	if c.RetransmitMult > 0 {
		cfg.RetransmitMult = c.RetransmitMult
	}

	if c.MaxGossipPacketSize > 0 {
		cfg.UDPBufferSize = c.MaxGossipPacketSize
	}

	if c.RetransmitMult > 0 {
		cfg.RetransmitMult = c.RetransmitMult
	}

	if c.SecretKey != "" {
		cfg.SecretKey = []byte(c.SecretKey)
	}

	cfg.Logger = log.New(os.Stdout, "", 0)
	cfg.Name = name
	cfg.Ping = s
	cfg.Delegate = s
	cfg.Events = s
	cfg.Alive = s
	cfg.Conflict = s
	cfg.Merge = s
	return cfg
}

func NewPeerConfig() *PeerConfig {
	return &PeerConfig{
		Addr:               "0.0.0.0",
		Port:               0,
		BroadcastQueueSize: 128,
		Members:            make([]string, 0),
		Grpc:               kit.NewGrpcConfig(),
		LeaderElection:     true,
		Cache: &PeerCacherConfig{
			NumCounters: 1e7,     // number of keys to track frequency of (10M).
			MaxCost:     1 << 30, // maximum cost of cache (1GB).
			BufferItems: 64,      // number of keys per Get buffer.
			Prefix:      "/nakama/",
		},
	}
}
