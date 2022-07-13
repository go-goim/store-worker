package dao

import (
	"context"
	"sync"

	"github.com/go-goim/core/pkg/db"
	"github.com/go-goim/store-worker/internal/data"
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

func (d *MessageDao) Put(ctx context.Context, msg *data.Message) error {
	if ctx == nil {
		ctx = context.Background()
	}

	result := db.GetHBaseFromCtx(ctx).Table(msg.TableName()).Key(msg.RowKey).Values(msg.Values()).Put()
	if result.Err() != nil {
		return result.Err()
	}

	return nil
}
