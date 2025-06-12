package server

import (
	"context"
	"database/sql"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
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

func toAnyResponseWriter(resp *pb.Peer_ResponseWriter) (*api.AnyResponseWriter, *rtapi.Notifications) {
	var notifications *rtapi.Notifications
	out := &api.AnyResponseWriter{
		Header: make(map[string]string),
	}

	if resp == nil {
		return out, notifications
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
		notifications = v.Notifications
		out.Body = &api.AnyResponseWriter_StringContent{StringContent: `{"code": 0}`}
	default:
	}
	return out, notifications
}

func sendAnyResponseWriter(ctx context.Context, logger *zap.Logger, db *sql.DB, gotracker Tracker, messageRouter MessageRouter, cid string, anyResponseWriter *api.AnyResponseWriter, notifications *rtapi.Notifications, recipients []*pb.Recipienter) error {
	recipientSize := len(recipients)
	if anyResponseWriter != nil {
		envelope := &rtapi.Envelope{
			Cid: cid,
			Message: &rtapi.Envelope_AnyResponseWriter{
				AnyResponseWriter: anyResponseWriter,
			},
		}

		if recipientSize < 1 {
			messageRouter.SendToAll(logger, envelope, true)
		} else {
			for _, recipient := range recipients {
				switch recipient.Action {
				case pb.Recipienter_USERID:
					presenceIDs := gotracker.ListPresenceIDByStream(PresenceStream{Mode: StreamModeNotifications, Subject: uuid.FromStringOrNil(recipient.GetToken())})
					messageRouter.SendToPresenceIDs(logger, presenceIDs, envelope, true)

				case pb.Recipienter_SESSIONID:
				case pb.Recipienter_CHANNEL:
					fallthrough
				case pb.Recipienter_STREAM:
					presenceIDs := gotracker.ListPresenceIDByStream(pb2PresenceStream(recipient.GetStream()))
					messageRouter.SendToPresenceIDs(logger, presenceIDs, envelope, true)
				}
			}
		}
	}

	if notifications != nil {
		if recipientSize < 1 {
			for _, notification := range notifications.GetNotifications() {
				_ = NotificationSendAll(ctx, logger, db, gotracker, messageRouter, notification)
			}
		} else {
			for _, recipient := range recipients {
				switch recipient.Action {
				case pb.Recipienter_USERID:
					_ = NotificationSend(ctx, logger, db, gotracker, messageRouter, map[uuid.UUID][]*api.Notification{uuid.FromStringOrNil(recipient.GetToken()): notifications.GetNotifications()})
				case pb.Recipienter_SESSIONID:
				case pb.Recipienter_CHANNEL:
					fallthrough
				case pb.Recipienter_STREAM:
					presenceIDs := gotracker.ListPresenceIDByStream(pb2PresenceStream(recipient.GetStream()))
					messageRouter.SendToPresenceIDs(logger, presenceIDs, &rtapi.Envelope{
						Message: &rtapi.Envelope_Notifications{Notifications: notifications},
					}, true)
				}
			}
		}
	}
	return nil
}
