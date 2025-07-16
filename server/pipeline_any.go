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
	"context"
	"strconv"

	"github.com/doublemo/nakama-common/rtapi"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Pipeline) any(logger *zap.Logger, session Session, envelope *rtapi.Envelope) (bool, *rtapi.Envelope) {
	// Get the Any request from the envelope
	req := envelope.GetAnyRequest()
	if req.Name == "" {
		// Return an error if the name is not set
		_ = session.Send(&rtapi.Envelope{Cid: envelope.Cid, Message: &rtapi.Envelope_Error{Error: &rtapi.Error{
			Code:    int32(rtapi.Error_BAD_INPUT),
			Message: "Name must be set",
		}}}, true)
		return false, nil
	}

	// Initialize the context map and populate it with session information
	req.Context = make(map[string]string)
	req.Context["client_ip"] = session.ClientIP()
	req.Context["client_port"] = session.ClientPort()
	req.Context["client_cid"] = envelope.Cid
	req.Context["userId"] = session.UserID().String()
	req.Context["username"] = session.Username()
	req.Context["expiry"] = strconv.FormatInt(session.Expiry(), 10)
	// Add all session variables to the context with "vars_" prefix
	for k, v := range session.Vars() {
		req.Context["vars_"+k] = v
	}

	// Get the peer from runtime
	peer, ok := p.runtime.GetPeer()
	if !ok {
		// Return an error if the peer is not available
		_ = session.Send(&rtapi.Envelope{Cid: envelope.Cid, Message: &rtapi.Envelope_Error{Error: &rtapi.Error{
			Code:    int32(codes.Unavailable),
			Message: "Service Unavailable",
		}}}, true)
		return false, nil
	}

	// Send the message to the peer
	if err := peer.SendMS(context.Background(), req); err != nil {
		// Handle any errors that occur during sending
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
	// Return success
	return true, nil
}
