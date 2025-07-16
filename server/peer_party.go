package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/zap"
)

func (s *LocalPeer) PartyCreate(entry *PartyIndexEntry) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node:    s.endpoint.Name(),
		Payload: &pb.BinaryLog_PartyCreate{PartyCreate: partyIndex2pb(entry)},
	}, true)
}

func (s *LocalPeer) PartyClose(id string) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node:    s.endpoint.Name(),
		Payload: &pb.BinaryLog_PartyClose{PartyClose: id},
	}, true)
}

func (p *LocalPartyRegistry) handleFromRemotePartyCreate(entry *PartyIndexEntry) {
	p.pendingUpdatesMutex.Lock()
	p.pendingUpdates[fmt.Sprintf("%v.%v", entry.Id, entry.Node)] = entry
	p.pendingUpdatesMutex.Unlock()
}

func (p *LocalPartyRegistry) handleFromRemotePartyClose(id string) {
	batch := bluge.NewBatch()
	batch.Delete(bluge.Identifier(id))
	p.indexWriter.Batch(batch)
}

func (p *LocalPartyRegistry) SyncData(ctx context.Context, nodeName string, data []*pb.Party_IndexEntry) error {
	startTime := time.Now()
	batch := bluge.NewBatch()
	defer batch.Reset()
	deletedCount, err := p.fillDeleteAllFromNodeOptimized(ctx, batch, nodeName)
	if err != nil {
		p.logger.Error("failed to delete old data from node",
			zap.String("node", nodeName),
			zap.Error(err))
		return fmt.Errorf("failed to delete old data from node %s: %w", nodeName, err)
	}

	p.logger.Debug("deleted old records", zap.String("node", nodeName), zap.Int("count", deletedCount))
	if deletedCount == 0 && len(data) == 0 {
		return nil
	}

	successCount := 0
	for _, index := range data {
		var label map[string]any
		if err := json.Unmarshal(index.Label, &label); err != nil {
			p.logger.Warn("failed to unmarshal party label",
				zap.String("partyID", index.Id),
				zap.Error(err))
			continue
		}

		doc, err := MapPartyIndexEntry(index.Id, &PartyIndexEntry{
			Id:          index.Id,
			Node:        index.Node,
			Hidden:      index.Hidden,
			MaxSize:     int(index.MaxSize),
			LabelString: index.LabelString,
			CreateTime:  time.Unix(index.CreateTime, 0),
		})
		if err != nil {
			p.logger.Error("failed to map party index entry to document",
				zap.String("partyID", index.Id),
				zap.Error(err))
			continue
		}
		batch.Update(bluge.Identifier(index.Id), doc)
		successCount++
	}

	if err := p.indexWriter.Batch(batch); err != nil {
		p.logger.Error("failed to execute batch operation",
			zap.String("node", nodeName),
			zap.Error(err))
		return fmt.Errorf("failed to execute batch operation for node %s: %w", nodeName, err)
	}

	p.logger.Info("successfully synced node data",
		zap.String("node", nodeName),
		zap.Int("deleted", deletedCount),
		zap.Int("added", successCount),
		zap.Int("totalProcessed", len(data)),
		zap.Duration("duration", time.Since(startTime)))
	return nil
}

func (p *LocalPartyRegistry) deleteAllFromNodeOptimized(ctx context.Context, nodeName string) error {
	batch := bluge.NewBatch()
	defer batch.Reset()
	n, err := p.fillDeleteAllFromNodeOptimized(ctx, batch, nodeName)
	if err != nil {
		return err
	}

	if n == 0 {
		return nil
	}
	return p.indexWriter.Batch(batch)
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
		if err != nil || match == nil {
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

func pb2partyIndex(index *pb.Party_IndexEntry) *PartyIndexEntry {
	entry := &PartyIndexEntry{
		Id:          index.Id,
		Node:        index.Node,
		Open:        index.Open,
		Hidden:      index.Hidden,
		MaxSize:     int(index.MaxSize),
		Label:       make(map[string]any),
		LabelString: index.LabelString,
		CreateTime:  time.Unix(index.CreateTime, 0),
	}

	_ = json.Unmarshal(index.Label, &entry.Label)
	return entry
}
