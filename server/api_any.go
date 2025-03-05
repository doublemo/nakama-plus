package server

import (
	"context"
	"strconv"

	"github.com/doublemo/nakama-common/api"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *ApiServer) Any(ctx context.Context, in *api.AnyRequest) (*api.AnyResponseWriter, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name must be set")
	}

	// Before hook.
	if fn := s.runtime.BeforeAny(); fn != nil {
		beforeFn := func(clientIP, clientPort string) error {
			result, err, code := fn(ctx, s.logger, ctx.Value(ctxUserIDKey{}).(uuid.UUID).String(), ctx.Value(ctxUsernameKey{}).(string), ctx.Value(ctxVarsKey{}).(map[string]string), ctx.Value(ctxExpiryKey{}).(int64), clientIP, clientPort, in)
			if err != nil {
				return status.Error(code, err.Error())
			}
			if result == nil {
				// If result is nil, requested resource is disabled.
				s.logger.Warn("Intercepted a disabled resource.", zap.Any("resource", ctx.Value(ctxFullMethodKey{}).(string)), zap.String("uid", ctx.Value(ctxUserIDKey{}).(uuid.UUID).String()))
				return status.Error(codes.NotFound, "Requested resource was not found.")
			}
			in = result
			return nil
		}

		// Execute the before function lambda wrapped in a trace for stats measurement.
		err := traceApiBefore(ctx, s.logger, s.metrics, ctx.Value(ctxFullMethodKey{}).(string), beforeFn)
		if err != nil {
			return nil, err
		}
	}

	in.Context = make(map[string]string)
	if m := ctx.Value(ctxUserIDKey{}); m != nil {
		in.Context["userId"] = m.(uuid.UUID).String()
	}

	if m := ctx.Value(ctxUsernameKey{}); m != nil {
		in.Context["username"] = m.(string)
	}

	if v := ctx.Value(ctxVarsKey{}); v != nil {
		for k, v := range v.(map[string]string) {
			in.Context["vars_"+k] = v
		}
	}

	if e := ctx.Value(ctxExpiryKey{}); e != nil {
		in.Context["expiry"] = strconv.FormatInt(e.(int64), 10)
	}

	peer, ok := s.runtime.GetPeer()
	if !ok {
		return nil, status.Error(codes.Unavailable, "Unavailable")
	}

	w, err := peer.InvokeMS(ctx, in)
	if err != nil {
		return nil, err
	}

	// After hook.
	if fn := s.runtime.AfterAny(); fn != nil {
		afterFn := func(clientIP, clientPort string) error {
			return fn(ctx, s.logger, ctx.Value(ctxUserIDKey{}).(uuid.UUID).String(), ctx.Value(ctxUsernameKey{}).(string), ctx.Value(ctxVarsKey{}).(map[string]string), ctx.Value(ctxExpiryKey{}).(int64), clientIP, clientPort, w, in)
		}
		// Execute the after function lambda wrapped in a trace for stats measurement.
		traceApiAfter(ctx, s.logger, s.metrics, ctx.Value(ctxFullMethodKey{}).(string), afterFn)
	}
	return w, nil
}
