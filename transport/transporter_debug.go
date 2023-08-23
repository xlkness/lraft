package transport

import "lraft/message"

type writer interface {
	SendMsg(msg *message.Message)
}

type DebugTransporter struct {
	id    int64
	nodes map[int64]writer
}

func NewDebugTransporter(id int64) *DebugTransporter {
	dt := new(DebugTransporter)
	dt.id = id
	dt.nodes = make(map[int64]writer)
	return dt
}

func (dt *DebugTransporter) AddNode(id int64, w writer) {
	dt.nodes[id] = w
}

func (dt *DebugTransporter) Send(to int64, msg *message.Message) {
	w, find := dt.nodes[to]
	if find {
		w.SendMsg(msg)
	}
}
