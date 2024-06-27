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
	Call(ctx context.Context, in *pb.Request) (*pb.ResponseWriter, error)
	NotifyMsg(conn Connector, in *pb.Request)
	OnServiceUpdate(serviceRegistry ServiceRegistry, client Client)
}
