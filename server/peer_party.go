package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/zap"
)

func (s *LocalPeer) PartyCreate(entry *PartyIndexEntry) {
	s.BinaryLogBroadcast(&pb.BinaryLog{
		Node:    s.endpoint.Name(),
		Payload: &pb.BinaryLog_PartyCreate{PartyCreate: partyIndex2pb(entry)},
	}, true)
}

func (s *LocalPeer) PartyClose(id string) {
	s.BinaryLogBroadcast(&pb.BinaryLog{
		Node:    s.endpoint.Name(),
		Payload: &pb.BinaryLog_PartyClose{PartyClose: id},
	}, true)
}

func (p *LocalPartyRegistry) handleFromRemotePartyCreate(entry *PartyIndexEntry) {
	p.pendingUpdatesMutex.Lock()
	p.pendingUpdates[entry.Id] = entry
	p.pendingUpdatesMutex.Unlock()
}

func (p *LocalPartyRegistry) handleFromRemotePartyClose(id string) {
	p.pendingUpdatesMutex.Lock()
	defer p.pendingUpdatesMutex.Unlock()
	batch := bluge.NewBatch()
	batch.Delete(bluge.Identifier(id))
	if err := p.indexWriter.Batch(batch); err != nil {
		p.logger.Warn("Failed to remove remote party index entry", zap.String("partyID", id), zap.Error(err))
		p.pendingUpdates[id] = nil
		return
	}
	delete(p.pendingUpdates, id)
}

func (p *LocalPartyRegistry) ExtractLocal(ctx context.Context) ([]*PartyIndexEntry, error) {
	p.pendingUpdatesMutex.Lock()
	defer p.pendingUpdatesMutex.Unlock()

	reader, err := p.indexWriter.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	query := bluge.NewAllMatches(bluge.NewTermQuery(p.node).SetField("node"))
	iter, err := reader.Search(ctx, query)
	if err != nil {
		return nil, err
	}
	indexed, err := p.queryMatchesToEntries(iter)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]*PartyIndexEntry, len(indexed))
	for _, entry := range indexed {
		entries[entry.Id] = entry
	}
	for id, entry := range p.pendingUpdates {
		if _, owner, _ := strings.Cut(id, "."); owner != p.node {
			continue
		}
		if entry == nil {
			delete(entries, id)
		} else {
			entries[id] = entry
		}
	}
	snapshot := make([]*PartyIndexEntry, 0, len(entries))
	for _, entry := range entries {
		snapshot = append(snapshot, entry)
	}
	return snapshot, nil
}

func (p *LocalPartyRegistry) SyncData(ctx context.Context, nodeName string, data []*pb.Party_IndexEntry) error {
	p.pendingUpdatesMutex.Lock()
	defer p.pendingUpdatesMutex.Unlock()

	batch := bluge.NewBatch()
	deleted, err := p.fillDeleteAllFromNodeOptimized(ctx, batch, nodeName)
	if err != nil {
		return err
	}
	for _, item := range data {
		entry, err := pb2partyIndex(item, nodeName)
		if err != nil {
			return err
		}
		doc, err := MapPartyIndexEntry(entry.Id, entry)
		if err != nil {
			return err
		}
		batch.Update(bluge.Identifier(entry.Id), doc)
	}

	if deleted != 0 || len(data) != 0 {
		if err := p.indexWriter.Batch(batch); err != nil {
			return err
		}
	}
	p.clearPendingFromNode(nodeName)
	return nil
}

// clearPendingFromNode requires pendingUpdatesMutex to be held.
func (p *LocalPartyRegistry) clearPendingFromNode(node string) {
	for id := range p.pendingUpdates {
		if _, owner, _ := strings.Cut(id, "."); owner == node {
			delete(p.pendingUpdates, id)
		}
	}
}

func (p *LocalPartyRegistry) deleteAllFromNodeOptimized(ctx context.Context, nodeName string) error {
	p.pendingUpdatesMutex.Lock()
	defer p.pendingUpdatesMutex.Unlock()

	batch := bluge.NewBatch()
	defer batch.Reset()
	n, err := p.fillDeleteAllFromNodeOptimized(ctx, batch, nodeName)
	if err != nil {
		return err
	}

	if n != 0 {
		if err := p.indexWriter.Batch(batch); err != nil {
			return err
		}
	}
	p.clearPendingFromNode(nodeName)
	return nil
}

func (p *LocalPartyRegistry) fillDeleteAllFromNodeOptimized(ctx context.Context, batch *index.Batch, nodeName string) (n int, err error) {
	searchReq := bluge.NewAllMatches(bluge.NewTermQuery(nodeName).SetField("node"))
	reader, err := p.indexWriter.Reader()
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	iter, err := reader.Search(ctx, searchReq)
	if err != nil {
		return 0, err
	}

	for {
		match, err := iter.Next()
		if err != nil {
			return 0, err
		}
		if match == nil {
			break
		}

		err = match.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				batch.Delete(bluge.Identifier(string(value)))
				n++
			}
			return true
		})

		if err != nil {
			return 0, fmt.Errorf("error visiting stored fields: %w", err)
		}
	}
	return n, nil
}

func partyIndex2pb(index *PartyIndexEntry) *pb.Party_IndexEntry {
	labelBytes, _ := json.Marshal(index)
	return &pb.Party_IndexEntry{
		Id:          index.Id,
		Node:        index.Node,
		Open:        index.Open,
		Hidden:      index.Hidden,
		MaxSize:     int32(index.MaxSize),
		Label:       labelBytes,
		LabelString: index.LabelString,
		CreateTime:  index.CreateTime.Unix(),
	}
}

func pb2partyIndex(index *pb.Party_IndexEntry, node string) (*PartyIndexEntry, error) {
	_, owner, _ := strings.Cut(index.GetId(), ".")
	if index.GetNode() != node || owner != node {
		return nil, fmt.Errorf("party %q does not belong to node %q", index.GetId(), node)
	}
	label := index.GetLabelString()
	if label == "" {
		label = "{}"
	}
	entry := &PartyIndexEntry{
		Id:          index.Id,
		Node:        index.Node,
		Open:        index.Open,
		Hidden:      index.Hidden,
		MaxSize:     int(index.MaxSize),
		LabelString: label,
		CreateTime:  time.Unix(index.CreateTime, 0),
	}

	if err := json.Unmarshal([]byte(label), &entry.Label); err != nil {
		return nil, err
	}
	return entry, nil
}
