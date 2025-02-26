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

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Pipeline) any(logger *zap.Logger, session Session, envelope *rtapi.Envelope) (bool, *rtapi.Envelope) {
	req := envelope.GetAnyRequest()
	if req.Name == "" {
		_ = session.Send(&rtapi.Envelope{Cid: envelope.Cid, Message: &rtapi.Envelope_Error{Error: &rtapi.Error{
			Code:    int32(rtapi.Error_BAD_INPUT),
			Message: "Name must be set",
		}}}, true)
		return false, nil
	}

	in := toPeerRequest(req)
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

func toPeerRequest(req *api.AnyRequest) *pb.Peer_Request {
	in := &pb.Peer_Request{
		Query:   make(map[string]*pb.Peer_Query),
		Header:  make(map[string]string),
		Context: make(map[string]string),
	}

	if req == nil {
		return in
	}

	in.Cid = req.GetCid()
	in.Name = req.GetName()
	for k, v := range req.GetQuery() {
		if v == nil {
			continue
		}

		size := len(v.GetValue())
		in.Query[k] = &pb.Peer_Query{Value: make([]string, size)}
		if size > 0 {
			copy(in.Query[k].Value, v.GetValue())
		}
	}

	for k, v := range req.GetHeader() {
		in.Header[k] = v
	}

	switch v := req.GetBody().(type) {
	case *api.AnyRequest_BytesContent:
		size := len(v.BytesContent)
		body := &pb.Peer_Request_BytesContent{BytesContent: make([]byte, 0)}
		if size > 0 {
			copy(body.BytesContent, v.BytesContent)
		}

		in.Payload = body
	case *api.AnyRequest_StringContent:
		in.Payload = &pb.Peer_Request_StringContent{StringContent: v.StringContent}
	default:
	}
	return in
}

func toAnyResponseWriter(resp *pb.Peer_ResponseWriter) *api.AnyResponseWriter {
	out := &api.AnyResponseWriter{
		Header: make(map[string]string),
	}

	if resp == nil {
		return out
	}

	for k, v := range resp.GetHeader() {
		out.Header[k] = v
	}

	out.Header["cid"] = resp.GetCid()
	out.Header["name"] = resp.GetName()

	switch v := resp.GetPayload().(type) {
	case *pb.Peer_ResponseWriter_BytesContent:
		size := len(v.BytesContent)
		body := &api.AnyResponseWriter_BytesContent{BytesContent: make([]byte, 0)}
		if size > 0 {
			copy(body.BytesContent, v.BytesContent)
		}

		out.Body = body
	case *pb.Peer_ResponseWriter_StringContent:
		out.Body = &api.AnyResponseWriter_StringContent{StringContent: v.StringContent}
	default:
	}
	return out
}
