package server

import (
	"context"
	"strconv"
	"strings"

	"github.com/doublemo/nakama-common/api"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (s *ApiServer) Any(ctx context.Context, in *api.AnyRequest) (*api.AnyResponseWriter, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name must be set")
	}

	var (
		userId     string
		username   string
		vars       map[string]string
		expiry     int64
		fullMethod string
	)

	if m := ctx.Value(ctxUserIDKey{}); m != nil {
		userId = m.(uuid.UUID).String()
	}

	if m := ctx.Value(ctxUsernameKey{}); m != nil {
		username = m.(string)
	}

	if m := ctx.Value(ctxVarsKey{}); m != nil {
		vars = m.(map[string]string)
	}

	if m := ctx.Value(ctxExpiryKey{}); m != nil {
		expiry = m.(int64)
	}

	if m := ctx.Value(ctxFullMethodKey{}); m != nil {
		fullMethod = m.(string)
	}

	if in.Header == nil {
		in.Header = make(map[string]string)
	}

	if in.Query == nil {
		in.Query = make(map[string]*api.AnyQuery)
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		for k, v := range md {
			size := len(v)
			if !strings.HasPrefix(k, "q_") {
				if size > 0 {
					in.Header[k] = v[0]
				} else {
					in.Header[k] = ""
				}
			} else {
				values := &api.AnyQuery{Value: make([]string, size)}
				if size > 0 {
					copy(values.Value, v)
				}
				in.Query[k[2:]] = values
			}
		}
	}

	clientIP, clientPort := extractClientAddressFromContext(s.logger, ctx)
	in.Header["client_ip"] = clientIP
	in.Header["client_port"] = clientPort

	logger := LoggerWithTraceId(ctx, s.logger)
	// Before hook.
	if fn := s.runtime.BeforeAny(); fn != nil {
		beforeFn := func(clientIP, clientPort string) error {
			result, err, code := fn(ctx, logger, userId, username, vars, expiry, clientIP, clientPort, in)
			if err != nil {
				return status.Error(code, err.Error())
			}
			if result == nil {
				// If result is nil, requested resource is disabled.
				logger.Warn("Intercepted a disabled resource.", zap.Any("resource", fullMethod), zap.String("uid", userId))
				return status.Error(codes.NotFound, "Requested resource was not found.")
			}
			in = result
			return nil
		}
		// Execute the before function lambda wrapped in a trace for stats measurement.
		err := traceApiBefore(ctx, logger, s.metrics, ctx.Value(ctxFullMethodKey{}).(string), beforeFn)
		if err != nil {
			return nil, err
		}
	}

	in.Context = make(map[string]string)
	in.Context["userId"] = userId
	in.Context["username"] = username
	in.Context["expiry"] = strconv.FormatInt(expiry, 10)
	for k, v := range vars {
		in.Context["vars_"+k] = v
	}

	peer, ok := s.runtime.GetPeer()
	if !ok {
		return nil, status.Error(codes.Unavailable, "Service Unavailable")
	}

	w, err := peer.InvokeMS(ctx, in)
	if err != nil {
		return nil, err
	}

	// After hook.
	if fn := s.runtime.AfterAny(); fn != nil {
		afterFn := func(clientIP, clientPort string) error {
			return fn(ctx, logger, userId, username, vars, expiry, clientIP, clientPort, w, in)
		}
		// Execute the after function lambda wrapped in a trace for stats measurement.
		traceApiAfter(ctx, logger, s.metrics, fullMethod, afterFn)
	}
	return w, nil
}
