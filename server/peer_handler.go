// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package server

import (
	"time"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-kit/kit"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *LocalPeer) handlerByPeerEnvelope(client kit.Client, msg *pb.Peer_Envelope) {
	if client != nil {
		s.logger.Debug("recv info", zap.String("name", client.Name()), zap.String("Role", client.Name()))
	} else {
		s.logger.Debug("recv info")
	}

	switch msg.Payload.(type) {
	case *pb.Peer_Envelope_NkEnvelope:
		s.ToClient(msg.GetNkEnvelope(), msg.GetRecipient())
	default:
	}
}

func (s *LocalPeer) handlerByPeerResponseWriter(client kit.Client, msg *pb.Peer_ResponseWriter) {
	if client != nil {
		s.logger.Debug("recv info", zap.String("name", client.Name()), zap.String("Role", client.Name()))
	} else {
		s.logger.Debug("recv info")
	}
	s.toClientByPeerResponseWriter(msg)
}

func (s *LocalPeer) toClientByPeerResponseWriter(w *pb.Peer_ResponseWriter) {
	msgData := &api.AnyResponseWriter{
		Header: make(map[string]string),
	}

	for k, v := range w.GetHeader() {
		msgData.Header[k] = v
	}
	msgData.Header["cid"] = w.GetCid()
	msgData.Header["name"] = w.GetName()
	switch v := w.GetPayload().(type) {
	case *pb.Peer_ResponseWriter_BytesContent:
		size := len(v.BytesContent)
		body := &api.AnyResponseWriter_BytesContent{BytesContent: make([]byte, size)}
		if size > 0 {
			copy(body.BytesContent, v.BytesContent)
		}
		msgData.Body = body

	case *pb.Peer_ResponseWriter_StringContent:
		msgData.Body = &api.AnyResponseWriter_StringContent{StringContent: v.StringContent}

	case *pb.Peer_ResponseWriter_Notifications:
		s.ToClient(&rtapi.Envelope{Message: &rtapi.Envelope_Notifications{Notifications: v.Notifications}}, w.GetRecipient())
		return
	default:
	}
	s.toClientByApiResponseWriter(msgData, w.GetRecipient())
}

func (s *LocalPeer) toClientByApiResponseWriter(w *api.AnyResponseWriter, recipients []*pb.Recipienter) {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_AnyResponseWriter{AnyResponseWriter: w},
	}

	s.ToClient(envelope, recipients)
}

func (s *LocalPeer) ToClient(envelope *rtapi.Envelope, recipients []*pb.Recipienter) {
	if envelope == nil {
		s.logger.Warn("toClient: envelope is nil")
		return
	}

	size := len(recipients)
	if size < 1 {
		s.sessionRegistry.Range(func(session Session) bool {
			_ = session.Send(envelope, true)
			return true
		})
		return
	}

	for _, recipient := range recipients {
		switch recipient.Action {
		case pb.Recipienter_USERID:
			presenceIDs := s.tracker.ListLocalPresenceIDByStream(PresenceStream{Mode: StreamModeNotifications, Subject: uuid.FromStringOrNil(recipient.GetToken())})
			s.messageRouter.SendToPresenceIDs(s.logger, presenceIDs, envelope, true)

		case pb.Recipienter_SESSIONID:
			session := s.sessionRegistry.Get(uuid.FromStringOrNil(recipient.GetToken()))
			if session != nil {
				_ = session.Send(envelope, true)
			}

		case pb.Recipienter_CHANNEL:
			fallthrough
		case pb.Recipienter_STREAM:
			presenceIDs := s.tracker.ListLocalPresenceIDByStream(pb2PresenceStream(recipient.GetStream()))
			s.messageRouter.SendToPresenceIDs(s.logger, presenceIDs, envelope, true)

		default:
		}
	}
}

func (s *LocalPeer) singleSocket(userID string) {
	sessionIDs := s.tracker.ListLocalSessionIDByStream(PresenceStream{Mode: StreamModeNotifications, Subject: uuid.FromStringOrNil(userID)})
	for _, foundSessionID := range sessionIDs {
		session := s.sessionRegistry.Get(foundSessionID)
		if session != nil {
			// No need to remove the session from the map, session.Close() will do that.
			session.Close("server-side session disconnect", runtime.PresenceReasonDisconnect,
				&rtapi.Envelope{Message: &rtapi.Envelope_Notifications{
					Notifications: &rtapi.Notifications{
						Notifications: []*api.Notification{
							{
								Id:         uuid.Must(uuid.NewV4()).String(),
								Subject:    "single_socket",
								Content:    "{}",
								Code:       NotificationCodeSingleSocket,
								SenderId:   "",
								CreateTime: &timestamppb.Timestamp{Seconds: time.Now().Unix()},
								Persistent: false,
							},
						},
					},
				}})
		}
	}
}

func (s *LocalPeer) disconnect(w *pb.Disconnect) {
	// No need to remove the session from the map, session.Close() will do that.
	reasonOverride := runtime.PresenceReasonDisconnect
	if w.Reason > 0 {
		reasonOverride = runtime.PresenceReason(w.GetReason())
	}

	session := s.sessionRegistry.Get(uuid.FromStringOrNil(w.GetSessionID()))
	if session != nil {
		if w.Ban {
			session.Close("server-side session disconnect", runtime.PresenceReasonDisconnect,
				&rtapi.Envelope{Message: &rtapi.Envelope_Notifications{
					Notifications: &rtapi.Notifications{
						Notifications: []*api.Notification{
							{
								Id:         uuid.Must(uuid.NewV4()).String(),
								Subject:    "banned",
								Content:    "{}",
								Code:       NotificationCodeUserBanned,
								SenderId:   "",
								CreateTime: &timestamppb.Timestamp{Seconds: time.Now().Unix()},
								Persistent: false,
							},
						},
					},
				}})
		} else {
			session.Close("server-side session disconnect", reasonOverride)
		}
	}
}

func newEnvelopeError(err error) *pb.Peer_Envelope_Error {
	errMessage := &rtapi.Error{
		Code:    int32(codes.Unknown),
		Message: err.Error(),
	}
	code, ok := status.FromError(err)
	if ok {
		errMessage.Code = int32(code.Code())
		errMessage.Message = code.Message()
	}
	return &pb.Peer_Envelope_Error{Error: errMessage}
}
