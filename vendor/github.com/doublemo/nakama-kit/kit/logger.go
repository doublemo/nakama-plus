// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"go.uber.org/zap"
)

type grpcCustomLogger struct {
	*zap.SugaredLogger
}

// https://github.com/grpc/grpc-go/blob/master/grpclog/loggerv2.go
func NewGrpcCustomLogger(logger *zap.Logger) grpcCustomLogger {
	sLogger := logger.Sugar()
	return grpcCustomLogger{
		sLogger,
	}
}

func (g grpcCustomLogger) Warning(args ...any) {
	g.Warn(args...)
}

func (g grpcCustomLogger) Warningln(args ...any) {
	g.Warnln(args...)
}

func (g grpcCustomLogger) Warningf(format string, args ...any) {
	g.Warnf(format, args...)
}

func (g grpcCustomLogger) V(l int) bool {
	return int(g.Level()) <= l
}
