package server

import (
	"context"
	"strconv"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *ApiServer) Any(ctx context.Context, in *api.AnyRequest) (*api.AnyResponseWriter, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name must be set")
	}

	request := &pb.Request{
		Context: make(map[string]string),
		Payload: &pb.Request_In{In: in},
	}

	for k, v := range s.config.GetRuntime().Environment {
		request.Context[k] = v
	}

	if m := ctx.Value(ctxUserIDKey{}); m != nil {
		request.Context["userId"] = m.(uuid.UUID).String()
	}

	if m := ctx.Value(ctxUsernameKey{}); m != nil {
		request.Context["username"] = m.(string)
	}

	if v := ctx.Value(ctxVarsKey{}); v != nil {
		for k, v := range v.(map[string]string) {
			request.Context["vars_"+k] = v
		}
	}

	if e := ctx.Value(ctxExpiryKey{}); e != nil {
		request.Context["expiry"] = strconv.FormatInt(e.(int64), 10)
	}

	resp, err := s.internalRemoteCall(ctx, request)
	if err != nil {
		return nil, err
	}

	envelope := resp.GetEnvelope()
	if envelope == nil {
		return &api.AnyResponseWriter{}, nil
	}

	writer := envelope.GetAnyResponseWriter()
	if writer == nil {
		return &api.AnyResponseWriter{}, nil
	}
	return writer, nil
}

func (s *ApiServer) internalRemoteCall(ctx context.Context, in *pb.Request) (*pb.ResponseWriter, error) {
	req := in.GetIn()
	if req == nil || req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Invalid Argument")
	}

	peer := s.runtime.GetPeer()
	if peer == nil {
		return nil, status.Error(codes.Unavailable, "Service Unavailable")
	}

	endpoint, ok := peer.GetServiceRegistry().Get(req.Name)
	if !ok {
		return nil, status.Error(codes.Unavailable, "Service Unavailable")
	}

	return endpoint.Do(ctx, in)
}
