package dao

import (
	"context"
	"sync"

	messagev1 "github.com/go-goim/api/message/v1"
	"github.com/go-goim/core/pkg/db"
)

type MessageDao struct {
}

var (
	msgDao *MessageDao
	once   sync.Once
)

func GetMessageDao() *MessageDao {
	once.Do(func() {
		msgDao = &MessageDao{}
	})

	return msgDao
}

func (d *MessageDao) Put(ctx context.Context, msg *messagev1.StorageMessage) error {
	if ctx == nil {
		ctx = context.Background()
	}

	result := db.GetHBaseFromCtx(ctx).
		Table(msg.TableName()).
		Key(msg.HbaseRowKey()).
		Values(msg.HbaseValues()).
		Put()
	if result.Err() != nil {
		return result.Err()
	}

	return nil
}
