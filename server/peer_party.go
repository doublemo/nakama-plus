package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/zap"
)

func (p *LocalPartyRegistry) SyncData(ctx context.Context, nodeName string, data []*pb.Party_IndexEntry) error {
	batch := bluge.NewBatch()
	if _, err := p.fillDeleteAllFromNodeOptimized(ctx, batch, nodeName); err != nil {
		return err
	}

	for _, index := range data {
		var label map[string]any
		if err := json.Unmarshal(index.Label, &label); err != nil {
			p.logger.Error("error unmarshal party label", zap.Error(err))
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
			p.logger.Error("error mapping party index entry to doc: %v", zap.Error(err))
		}
		batch.Update(bluge.Identifier(index.Id), doc)
	}

	if err := p.indexWriter.Batch(batch); err != nil {
		p.logger.Error("error processing party label updates", zap.Error(err))
	}
	batch.Reset()
	return nil
}

func (p *LocalPartyRegistry) deleteAllFromNodeOptimized(ctx context.Context, nodeName string) error {
	batch := bluge.NewBatch()
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
			return 0, err
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
