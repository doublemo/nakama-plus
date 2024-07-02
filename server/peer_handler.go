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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *LocalPeer) handler(client kit.Client, msg *pb.ResponseWriter) {
	s.logger.Debug("recv info", zap.String("name", client.Name()), zap.String("Role", client.Name()))
	switch msg.Payload.(type) {
	case *pb.ResponseWriter_Envelope:
		s.toClient(msg)
	default:
	}
}

func (s *LocalPeer) toClient(w *pb.ResponseWriter) {
	recipients := w.GetRecipient()
	size := len(recipients)
	if size < 1 {
		s.sessionRegistry.Range(func(session Session) bool {
			_ = session.Send(w.GetEnvelope(), true)
			return true
		})
		return
	}

	for _, recipient := range recipients {
		switch recipient.Action {
		case pb.Recipienter_USERID:
			presenceIDs := s.tracker.ListLocalPresenceIDByStream(PresenceStream{Mode: StreamModeNotifications, Subject: uuid.FromStringOrNil(recipient.GetToken())})
			s.messageRouter.SendToPresenceIDs(s.logger, presenceIDs, w.GetEnvelope(), true)

		case pb.Recipienter_SESSIONID:
			session := s.sessionRegistry.Get(uuid.FromStringOrNil(recipient.GetToken()))
			if session != nil {
				_ = session.Send(w.GetEnvelope(), true)
			}

		case pb.Recipienter_CHANNEL:

		case pb.Recipienter_STREAM:
			presenceIDs := s.tracker.ListLocalPresenceIDByStream(pb2PresenceStream(recipient.GetStream()))
			s.messageRouter.SendToPresenceIDs(s.logger, presenceIDs, w.GetEnvelope(), true)

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
