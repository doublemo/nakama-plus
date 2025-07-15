package server

import (
	"sync"

	"github.com/doublemo/nakama-kit/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PeerInbox struct {
	inbox map[string]chan *pb.Frame
	sync.RWMutex
}

func NewPeerInbox() *PeerInbox {
	return &PeerInbox{
		inbox: make(map[string]chan *pb.Frame, 2048),
	}
}

func (s *PeerInbox) Register(inbox string, reply chan *pb.Frame) error {
	if size := len(inbox); size < 1 {
		return status.Error(codes.InvalidArgument, "inbox is empty")
	}

	s.Lock()
	defer s.Unlock()

	if _, ok := s.inbox[inbox]; ok {
		return status.Error(codes.AlreadyExists, "inbox is already exists")
	}
	s.inbox[inbox] = reply
	return nil
}

func (s *PeerInbox) Deregister(inbox string) {
	s.Lock()
	delete(s.inbox, inbox)
	s.Unlock()
}

func (s *PeerInbox) Send(frame *pb.Frame) {
	s.RLock()
	reply, ok := s.inbox[frame.Inbox]
	s.RUnlock()

	if !ok {
		return
	}

	select {
	case reply <- frame:
	default:
	}
}
