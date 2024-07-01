// Copyright 2018 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"strconv"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Pipeline) any(logger *zap.Logger, session Session, envelope *rtapi.Envelope) (bool, *rtapi.Envelope) {
	req := envelope.GetRequest()
	if req.Name == "" {
		_ = session.Send(&rtapi.Envelope{Cid: envelope.Cid, Message: &rtapi.Envelope_Error{Error: &rtapi.Error{
			Code:    int32(rtapi.Error_BAD_INPUT),
			Message: "Name must be set",
		}}}, true)
		return false, nil
	}

	in := &pb.Request{
		Context: make(map[string]string),
		Payload: &pb.Request_In{In: req},
	}

	for k, v := range p.config.GetRuntime().Environment {
		in.Context[k] = v
	}
	in.Context["client_ip"] = session.ClientIP()
	in.Context["client_port"] = session.ClientPort()
	in.Context["userId"] = session.UserID().String()
	in.Context["username"] = session.Username()
	in.Context["expiry"] = strconv.FormatInt(session.Expiry(), 10)
	for k, v := range session.Vars() {
		in.Context["vars_"+k] = v
	}

	peer := p.runtime.GetPeer()
	if peer == nil {
		_ = session.Send(&rtapi.Envelope{Cid: envelope.Cid, Message: &rtapi.Envelope_Error{Error: &rtapi.Error{
			Code:    int32(codes.Unavailable),
			Message: "Service Unavailable",
		}}}, true)
		return false, nil
	}

	endpoint, ok := peer.GetServiceRegistry().Get(req.Name)
	if !ok {
		_ = session.Send(&rtapi.Envelope{Cid: envelope.Cid, Message: &rtapi.Envelope_Error{Error: &rtapi.Error{
			Code:    int32(codes.Unavailable),
			Message: "Service Unavailable",
		}}}, true)
		return false, nil
	}

	if err := endpoint.Send(in); err != nil {
		code, ok := status.FromError(err)
		if !ok {
			code = status.New(codes.Internal, err.Error())
		}
		_ = session.Send(&rtapi.Envelope{Cid: envelope.Cid, Message: &rtapi.Envelope_Error{Error: &rtapi.Error{
			Code:    int32(code.Code()),
			Message: code.Message(),
		}}}, true)
		return false, nil
	}
	return true, nil
}
