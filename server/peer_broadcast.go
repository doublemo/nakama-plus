// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
package server

import (
	"github.com/hashicorp/memberlist"
)

type PeerBroadcast struct {
	name     string
	msg      []byte
	finished chan struct{}
}

// The unique identity of this broadcast message.
func (b *PeerBroadcast) Name() string {
	return b.name
}

// Invalidates checks if enqueuing the current broadcast
// invalidates a previous broadcast
func (b *PeerBroadcast) Invalidates(other memberlist.Broadcast) bool {
	nb, ok := other.(memberlist.NamedBroadcast)
	if !ok {
		return false
	}

	return b.name == nb.Name()
}

// Returns a byte form of the message
func (b *PeerBroadcast) Message() []byte {
	return b.msg
}

// Finished is invoked when the message will no longer
// be broadcast, either due to invalidation or to the
// transmit limit being reached
func (b *PeerBroadcast) Finished() {
	if b.finished == nil {
		return
	}

	select {
	case b.finished <- struct{}{}:
	default:
	}
}
