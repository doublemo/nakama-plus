package server

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Keep the existing protobuf envelope wire-compatible. Only nodes that
// advertise this feature receive requests with the new context fields.
const partyStreamCapability = "party_stream_v1"

var errPartyStreamUnsupported = errors.New("session node does not support remote party stream changes")

func partyStreamChangeUncertain(err error) bool {
	if errors.Is(err, ErrNodeNotFound) || errors.Is(err, errPartyStreamUnsupported) {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown:
		return true
	default:
		return false
	}
}

func requestPartyStreamChange(ctx context.Context, peer Peer, partyID uuid.UUID, presence *Presence, join, singleParty bool) error {
	if peer == nil {
		return ErrNodeNotFound
	}
	endpoint, ok := peer.Member(presence.ID.Node)
	if !ok {
		return ErrNodeNotFound
	}
	if endpoint.GetMetadata(partyStreamCapability) != "1" {
		return errPartyStreamUnsupported
	}
	operation := "leave"
	if join {
		operation = "join"
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{Context: map[string]string{
		partyStreamCapability: operation,
		"party_id":            partyID.String(),
		"session_id":          presence.ID.SessionID.String(),
		"user_id":             presence.UserID.String(),
		"single_party":        strconv.FormatBool(singleParty),
	}})
	if err != nil {
		return err
	}
	if response.GetContext()[partyStreamCapability] != "ok" {
		return status.Error(codes.Unavailable, "remote party stream change was not acknowledged")
	}
	return nil
}

func (s *LocalPeer) applyPartyStreamChange(host string, fields map[string]string) error {
	partyID := uuid.FromStringOrNil(fields["party_id"])
	sessionID := uuid.FromStringOrNil(fields["session_id"])
	userID := uuid.FromStringOrNil(fields["user_id"])
	if host == "" || partyID.IsNil() || sessionID.IsNil() || userID.IsNil() {
		return status.Error(codes.InvalidArgument, "invalid party stream change")
	}
	session := s.sessionRegistry.Get(sessionID)
	if session != nil && session.UserID() != userID {
		return status.Error(codes.InvalidArgument, "party user does not own the session")
	}
	stream := PresenceStream{Mode: StreamModeParty, Subject: partyID, Label: host}
	switch fields[partyStreamCapability] {
	case "join":
		if session == nil {
			return status.Error(codes.NotFound, "party session not found")
		}
		ok, _, err := s.streamManager.UserJoin(stream, userID, sessionID, false, false, "")
		if err != nil || !ok {
			return status.Error(codes.FailedPrecondition, "could not track the party session")
		}
		if fields["single_party"] == "true" {
			s.tracker.UntrackByModes(sessionID, partyStreamMode, stream)
		}
		return nil
	case "leave":
		if err := s.streamManager.UserLeave(stream, userID, sessionID); err != nil {
			return status.Error(codes.FailedPrecondition, "could not untrack the party session")
		}
		return nil
	default:
		return status.Error(codes.InvalidArgument, "unknown party stream change")
	}
}
