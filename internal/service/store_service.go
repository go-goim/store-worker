package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"

	messagev1 "github.com/go-goim/api/message/v1"
	"github.com/go-goim/core/pkg/log"
	"github.com/go-goim/core/pkg/util"
	"github.com/go-goim/store-worker/internal/dao"
	"github.com/go-goim/store-worker/internal/data"
)

type StoreService struct {
	storeDao *dao.MessageDao
}

var (
	storeService *StoreService
	once         sync.Once
)

func GetStoreService() *StoreService {
	once.Do(func() {
		storeService = new(StoreService)
	})

	return storeService
}

func (s *StoreService) Group() string {
	return "store_msg"
}

func (s *StoreService) Topic() string {
	return "def_topic"
}

func (s *StoreService) Consume(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	log.Info("Consume msg", "mqMessage", ext[0].String())
	// msg 实际上只有一条
	msg := ext[0]
	if err := s.storeMsg(ctx, msg); err != nil {
		log.Error("store msg error", "err", err)
		return consumer.ConsumeRetryLater, err
	}

	return consumer.ConsumeSuccess, nil
}

func (s *StoreService) storeMsg(ctx context.Context, ext *primitive.MessageExt) error {
	msg := &messagev1.Message{}

	if err := json.Unmarshal(ext.Body, msg); err != nil {
		return err
	}

	dm := &data.Message{
		RowKey: rowKey(msg.SessionId, msg.MsgId),
		Users: &data.MessageUsers{
			From: msg.From,
			To:   msg.To,
		},
		Content: &data.MessageContent{
			Type: int8(msg.GetContentType()),
			Text: msg.GetContent(),
		},
		Extra: &data.MessageExtra{
			Timestamp: msg.GetCreateTime(),
		},
	}

	return s.storeDao.Put(ctx, dm)
}

func rowKey(sessionId string, msgId int64) string {
	var zone int64
	// parse sessionId
	_, from, _, err := util.ParseSession(sessionId)
	if err != nil {
		// use default value
		log.Error("parse sessionId error", "err", err, "sessionId", sessionId)
		return fmt.Sprintf("%03d|%s|%020d", zone, sessionId, msgId)
	}
	// from always is valid
	zone = from.Int64()%128 + 1
	// sessionId % 128 + 1 as partition, 1~128
	// partition|sessionId|msgId as rowKey
	// same msg from same session will be had same partition
	// 3 bytes | 24 bytes | 20 bytes => 47 bytes
	return fmt.Sprintf("%03d|%s|%020d", zone, sessionId, msgId)
}
