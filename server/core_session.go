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
	"database/sql"
	"errors"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrSessionTokenInvalid = errors.New("session token invalid")
	ErrRefreshTokenInvalid = errors.New("refresh token invalid")
)

func validateSessionToken(ctx context.Context, logger *zap.Logger, sessionCache SessionCache, userID uuid.UUID, token sessionTokenRef, refresh bool) error {
	valid, err := sessionCache.validate(ctx, userID, token, refresh)
	if err != nil {
		logger.Error("Error checking session state.", zap.Error(err))
		return status.Error(codes.Unavailable, "Session state unavailable.")
	}
	if !valid {
		if refresh {
			return status.Error(codes.Unauthenticated, "Refresh token invalid or expired.")
		}
		return status.Error(codes.Unauthenticated, "Auth token invalid")
	}
	return nil
}

func SessionRefresh(ctx context.Context, logger *zap.Logger, db *sql.DB, config Config, sessionCache SessionCache, token string) (uuid.UUID, string, map[string]string, string, int64, int64, error) {
	userID, _, vars, exp, tokenId, tokenIssuedAt, version, ok := parseToken([]byte(config.GetSession().RefreshEncryptionKey), token)
	if !ok {
		return uuid.Nil, "", nil, "", 0, 0, status.Error(codes.Unauthenticated, "Refresh token invalid or expired.")
	}
	if err := validateSessionToken(ctx, logger, sessionCache, userID, sessionTokenRef{tokenId, exp, version}, true); err != nil {
		return uuid.Nil, "", nil, "", 0, 0, err
	}

	// Look for an existing account.
	query := "SELECT username, disable_time FROM users WHERE id = $1 LIMIT 1"
	var dbUsername string
	var dbDisableTime pgtype.Timestamptz
	err := db.QueryRowContext(ctx, query, userID).Scan(&dbUsername, &dbDisableTime)
	if err != nil {
		if err == sql.ErrNoRows {
			// Account not found and creation is never allowed for this type.
			logger.With(zap.String("user_id", userID.String())).Warn("Refresh token user does not exist", zap.Error(err))
			return uuid.Nil, "", nil, "", 0, 0, status.Error(codes.NotFound, "User account not found.")
		}
		logger.Error("Error looking up user by ID.", zap.Error(err), zap.String("id", userID.String()))
		return uuid.Nil, "", nil, "", 0, 0, status.Error(codes.Internal, "Error finding user account.")
	}

	// Check if it's disabled.
	if dbDisableTime.Valid && dbDisableTime.Time.Unix() != 0 {
		logger.Info("User account is disabled.", zap.String("id", userID.String()))
		return uuid.Nil, "", nil, "", 0, 0, status.Error(codes.PermissionDenied, "User account banned.")
	}

	return userID, dbUsername, vars, tokenId, tokenIssuedAt, version, nil
}

func SessionLogout(ctx context.Context, config Config, sessionCache SessionCache, userID uuid.UUID, token, refreshToken string) error {
	var access, refresh sessionTokenRef
	if token != "" {
		var sessionUserID uuid.UUID
		var ok bool
		sessionUserID, _, _, access.expiry, access.id, _, access.version, ok = parseToken([]byte(config.GetSession().EncryptionKey), token)
		if !ok || sessionUserID != userID {
			return ErrSessionTokenInvalid
		}
	}

	if refreshToken != "" {
		var refreshUserID uuid.UUID
		var ok bool
		refreshUserID, _, _, refresh.expiry, refresh.id, _, refresh.version, ok = parseToken([]byte(config.GetSession().RefreshEncryptionKey), refreshToken)
		if !ok || refreshUserID != userID {
			return ErrRefreshTokenInvalid
		}
	}

	return sessionCache.revoke(ctx, userID, access, refresh)
}
