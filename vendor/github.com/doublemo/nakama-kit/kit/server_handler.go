// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"context"

	"github.com/doublemo/nakama-kit/pb"
)

type ServerHandler interface {
	Call(ctx context.Context, in *pb.Peer_Request) (*pb.Peer_ResponseWriter, error)
	NotifyMsg(conn Connector, in *pb.Peer_Request)
	OnServiceUpdate(serviceRegistry ServiceRegistry, client Client)
}
