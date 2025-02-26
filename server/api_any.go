package server

import (
	"context"
	"strconv"
	"strings"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (s *ApiServer) Any(ctx context.Context, in *api.AnyRequest) (*api.AnyResponseWriter, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name must be set")
	}

	request := toPeerRequest(in)
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

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		for k, v := range md {
			size := len(v)
			if !strings.HasPrefix(k, "q_") {
				if size > 0 {
					request.Header[k] = v[0]
				} else {
					request.Header[k] = ""
				}
			} else {
				values := &pb.Peer_Query{Value: make([]string, size)}
				if size > 0 {
					copy(values.Value, v)
				}
				request.Query[k[2:]] = values
			}
		}
	}

	resp, err := s.internalRemoteCall(ctx, request)
	if err != nil {
		return nil, err
	}

	if resp == nil {
		return &api.AnyResponseWriter{}, nil
	}

	w, ns := toAnyResponseWriter(resp)
	if ns != nil {
		_ = SendAnyResponseWriter(context.Background(), s.logger, s.db, s.tracker, s.router, nil, ns, resp.GetRecipient())
	}
	return w, nil
}

func (s *ApiServer) internalRemoteCall(ctx context.Context, in *pb.Peer_Request) (*pb.Peer_ResponseWriter, error) {
	if in == nil || in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Invalid Argument")
	}

	peer := s.runtime.GetPeer()
	if peer == nil {
		return nil, status.Error(codes.Unavailable, "Service Unavailable")
	}

	endpoint, ok := peer.GetServiceRegistry().Get(in.Name)
	if !ok {
		return nil, status.Error(codes.Unavailable, "Service Unavailable")
	}

	return endpoint.Do(ctx, in)
}
