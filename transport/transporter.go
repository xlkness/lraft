package transport

import (
	"google.golang.org/protobuf/proto"
	"lraft/message"
)

type Transporter interface {
	Send(to int64, msgID message.MessageID, payload proto.Message)
}
