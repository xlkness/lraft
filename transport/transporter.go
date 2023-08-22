package transport

import (
	"lraft/message"
)

type Transporter interface {
	Send(to int64, msg *message.Message)
}
