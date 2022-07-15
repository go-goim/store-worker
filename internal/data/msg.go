package data

import (
	"strconv"
)

// Message is model for store data in hbase
type Message struct {
	RowKey  string          `json:"-"`       // sessionID + msgID
	Users   *MessageUsers   `json:"users"`   // column family: users
	Content *MessageContent `json:"content"` // column family: content
	Extra   *MessageExtra   `json:"extra"`   // column family: extra
}

type MessageUsers struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

type MessageContent struct {
	Type int8   `json:"type"`
	Text string `json:"text"`
}

type MessageExtra struct {
	Timestamp int64 `json:"timestamp"`
}

func (Message) TableName() string {
	return "message_history"
}

func (m *Message) Values() map[string]map[string][]byte {
	return map[string]map[string][]byte{
		"users": {
			"from": []byte(strconv.FormatInt(m.Users.From, 10)),
			"to":   []byte(strconv.FormatInt(m.Users.To, 10)),
		},
		"content": {
			"type": []byte(strconv.Itoa(int(m.Content.Type))),
			"text": []byte(m.Content.Text),
		},
		"extra": {
			"timestamp": []byte(strconv.FormatInt(m.Extra.Timestamp, 10)),
		},
	}
}
