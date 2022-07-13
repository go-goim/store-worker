package service

import (
	"testing"
)

func Test_rowKey(t *testing.T) {
	type args struct {
		sessionId int64
		msgId     int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test_rowKey",
			args: args{
				sessionId: 69852090574311423,
				msgId:     69852090574311426,
			},
			want: "128|000069852090574311423|000069852090574311426",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rowKey(tt.args.sessionId, tt.args.msgId); got != tt.want {
				t.Errorf("rowKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
