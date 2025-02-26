package server

import (
	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-kit/pb"
)

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

func toAnyResponseWriter(resp *pb.Peer_ResponseWriter, peer Peer) *api.AnyResponseWriter {
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

	case *pb.Peer_ResponseWriter_Notifications:
		if peer != nil {
			peer.ToClient(&rtapi.Envelope{Message: &rtapi.Envelope_Notifications{Notifications: v.Notifications}}, resp.GetRecipient())
		}
		out.Body = &api.AnyResponseWriter_StringContent{StringContent: `{"code": 0}`}
	default:
	}
	return out
}
