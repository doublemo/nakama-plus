// Copyright 2021 The Nakama Authors
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

	"github.com/doublemo/nakama-common/api"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *ApiServer) SessionRefresh(ctx context.Context, in *api.SessionRefreshRequest) (*api.Session, error) {
	logger, traceID := LoggerWithTraceId(ctx, s.logger)
	// Before hook.
	if fn := s.runtime.BeforeSessionRefresh(); fn != nil {
		beforeFn := func(clientIP, clientPort string) error {
			result, err, code := fn(ctx, logger, traceID, "", "", nil, 0, clientIP, clientPort, in)
			if err != nil {
				return status.Error(code, err.Error())
			}
			if result == nil {
				// If result is nil, requested resource is disabled.
				logger.Warn("Intercepted a disabled resource.", zap.Any("resource", ctx.Value(ctxFullMethodKey{}).(string)))
				return status.Error(codes.NotFound, "Requested resource was not found.")
			}
			in = result
			return nil
		}

		// Execute the before function lambda wrapped in a trace for stats measurement.
		err := traceApiBefore(ctx, logger, s.config, s.metrics, ctx.Value(ctxFullMethodKey{}).(string), beforeFn)
		if err != nil {
			return nil, err
		}
	}

	if in.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "Refresh token is required.")
	}

	userID, username, vars, tokenId, tokenIssuedAt, version, err := SessionRefresh(ctx, logger, s.db, s.config, s.sessionCache, in.Token)
	if err != nil {
		return nil, err
	}

	// Use updated vars if they are provided, otherwise use existing ones from refresh token.
	useVars := in.Vars
	if useVars == nil {
		useVars = vars
	}
	userIDStr := userID.String()

	token, tokenExp := generateToken(s.config, tokenId, tokenIssuedAt, version, userIDStr, username, useVars)
	refreshToken, _ := generateRefreshToken(s.config, tokenId, tokenIssuedAt, version, userIDStr, username, useVars)
	session := &api.Session{Created: false, Token: token, RefreshToken: refreshToken}

	// After hook.
	if fn := s.runtime.AfterSessionRefresh(); fn != nil {
		afterFn := func(clientIP, clientPort string) error {
			return fn(ctx, logger, traceID, userIDStr, username, useVars, tokenExp, clientIP, clientPort, session, in)
		}

		// Execute the after function lambda wrapped in a trace for stats measurement.
		traceApiAfter(ctx, logger, s.config, s.metrics, ctx.Value(ctxFullMethodKey{}).(string), afterFn)
	}

	return session, nil
}

func (s *ApiServer) SessionLogout(ctx context.Context, in *api.SessionLogoutRequest) (*emptypb.Empty, error) {
	userID := ctx.Value(ctxUserIDKey{}).(uuid.UUID)
	logger, traceID := LoggerWithTraceId(ctx, s.logger)

	// Before hook.
	if fn := s.runtime.BeforeSessionLogout(); fn != nil {
		beforeFn := func(clientIP, clientPort string) error {
			result, err, code := fn(ctx, logger, traceID, userID.String(), ctx.Value(ctxUsernameKey{}).(string), ctx.Value(ctxVarsKey{}).(map[string]string), ctx.Value(ctxExpiryKey{}).(int64), clientIP, clientPort, in)
			if err != nil {
				return status.Error(code, err.Error())
			}
			if result == nil {
				// If result is nil, requested resource is disabled.
				logger.Warn("Intercepted a disabled resource.", zap.Any("resource", ctx.Value(ctxFullMethodKey{}).(string)), zap.String("uid", userID.String()))
				return status.Error(codes.NotFound, "Requested resource was not found.")
			}
			in = result
			return nil
		}

		// Execute the before function lambda wrapped in a trace for stats measurement.
		err := traceApiBefore(ctx, logger, s.config, s.metrics, ctx.Value(ctxFullMethodKey{}).(string), beforeFn)
		if err != nil {
			return nil, err
		}
	}

	if err := SessionLogout(ctx, s.config, s.sessionCache, userID, in.Token, in.RefreshToken); err != nil {
		if err == ErrSessionTokenInvalid {
			return nil, status.Error(codes.InvalidArgument, "Session token invalid.")
		}
		if err == ErrRefreshTokenInvalid {
			return nil, status.Error(codes.InvalidArgument, "Refresh token invalid.")
		}
		s.logger.Error("Error processing session logout.", zap.Error(err))
		return nil, status.Error(codes.Internal, "Error processing session logout.")
	}

	// After hook.
	if fn := s.runtime.AfterSessionLogout(); fn != nil {
		afterFn := func(clientIP, clientPort string) error {
			return fn(ctx, logger, traceID, userID.String(), ctx.Value(ctxUsernameKey{}).(string), ctx.Value(ctxVarsKey{}).(map[string]string), ctx.Value(ctxExpiryKey{}).(int64), clientIP, clientPort, in)
		}

		// Execute the after function lambda wrapped in a trace for stats measurement.
		traceApiAfter(ctx, logger, s.config, s.metrics, ctx.Value(ctxFullMethodKey{}).(string), afterFn)
	}

	return &emptypb.Empty{}, nil
}
