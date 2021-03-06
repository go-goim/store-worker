package service

import (
	"testing"
)

func Test_rowKey(t *testing.T) {
	type args struct {
		sessionID string
		msgID     int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test_rowKey",
			args: args{
				sessionID: "000aG9PKEB8ch0aG9PKEB8ci",
				msgID:     69852090574311426,
			},
			want: "003|000aG9PKEB8ch0aG9PKEB8ci|00069852090574311426",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rowKey(tt.args.sessionID, tt.args.msgID); got != tt.want {
				t.Errorf("rowKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
