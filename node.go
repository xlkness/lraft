package lraft

import (
	"lraft/message"
	"lraft/storage"
	"lraft/transport"
	"time"
)

type Node struct {
	Id      int64
	r       *raft
	msgChan chan *message.Message
}

func NewNode(id int64, peers []int64, tp transport.Transporter, st storage.Storage, config *Config) *Node {
	n := new(Node)
	n.Id = id

	err := config.checkAndCorrect()
	if err != nil {
		panic(err)
	}

	n.r = newRaft(id, peers, tp, st, config)
	n.msgChan = make(chan *message.Message, 20)
	return n
}

func (n *Node) Propose(originData []byte) {
	entries := make([]*message.Entry, 0, 1)
	// 先填充数据信息，不填充term index这些
	entries = append(entries, &message.Entry{Data: originData})
	n.SendMsg(&message.Message{
		MsgID:            message.MessageID_MsgAppendEntriesReq,
		AppendEntriesReq: &message.AppendEntriesReq{Entries: entries},
	})
}

func (n *Node) SendMsg(msg *message.Message) {
	select {
	case n.msgChan <- msg:
	default:
	}
}

func (n *Node) Start() {
	ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-n.msgChan:
			if !ok {
				return
			}

			n.r.HandleEvent(msg)
		case <-ticker.C:
			n.r.Tick()
		}
	}
}

func (n *Node) Stop() {
	close(n.msgChan)
}
