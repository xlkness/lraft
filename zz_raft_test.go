package lraft

import (
	"lraft/storage"
	"lraft/transport"
	"lraft/utils"
	"os"
	"testing"
	"time"
)

var commonConfig = &Config{
	TickDuration:                time.Duration(10) * time.Millisecond,
	BroadcastHeartBeatTimerTick: 1,
	CanElectionTimerTick:        3,
	RequestVoteTimeoutTick:      3,
}

func newTestNode(id int64, peers []int64) (*transport.DebugTransporter, storage.Storage, *Node) {
	tp := transport.NewDebugTransporter(id)
	st := storage.NewDebugMemoryStorage(id)
	n := NewNode(id, peers, tp, st, commonConfig)
	return tp, st, n
}

func multiNode() {
	tp1, _, n1 := newTestNode(1, []int64{1, 2, 3})
	tp2, _, n2 := newTestNode(2, []int64{1, 2, 3})
	tp3, _, n3 := newTestNode(3, []int64{1, 2, 3})

	tp1.AddNode(n1.Id, n1)
	tp1.AddNode(n2.Id, n2)
	tp1.AddNode(n3.Id, n3)

	tp2.AddNode(n1.Id, n1)
	tp2.AddNode(n2.Id, n2)
	tp2.AddNode(n3.Id, n3)

	tp3.AddNode(n1.Id, n1)
	tp3.AddNode(n2.Id, n2)
	tp3.AddNode(n3.Id, n3)

	go n1.Start()
	go n2.Start()
	go n3.Start()
}

func singleNode() {
	os.Remove("1.debug.entries")

	tp1, st1, n1 := newTestNode(1, []int64{1})
	tp1.AddNode(n1.Id, n1)

	go n1.Start()

	// 等待选主
	for n1.r.leader == None {
		time.Sleep(time.Duration(1) * time.Millisecond)
	}

	n1.Propose([]byte("set\na\n123"))
	n1.Propose([]byte("set\nb\n234"))
	n1.Propose([]byte("set\nb\n456"))

	time.Sleep(time.Second * 3)
	li, _, err := st1.LastIndex()
	if err != nil {
		panic(err)
	}
	// 选主后会提交一个空条目，再加上之后提交的3个，索引为4
	utils.Assert(li, uint64(4))

	fi, _, err := st1.FirstIndex()
	utils.Assert(fi, uint64(1))

	term, find, _, err := st1.Term(1)
	utils.Assert(err, nil)
	utils.Assert(find, true)
	utils.Assert(term, uint64(1))

	// 模拟节点挂了
	n1.Stop()

	time.Sleep(time.Second)

	tp1, st1, n1 = newTestNode(1, []int64{1})
	tp1.AddNode(n1.Id, n1)

	go n1.Start()

	// 等待选主
	for n1.r.leader == None {
		time.Sleep(time.Duration(1) * time.Millisecond)
	}

	li, _, err = st1.LastIndex()
	if err != nil {
		panic(err)
	}
	utils.Assert(li, uint64(5))

	fi, _, err = st1.FirstIndex()
	utils.Assert(fi, uint64(1))

	term, find, _, err = st1.Term(1)
	utils.Assert(err, nil)
	utils.Assert(find, true)
	utils.Assert(term, uint64(1))

	n1.Propose([]byte("set\nc\n789"))
	n1.Propose([]byte("set\nc\n890"))

	time.Sleep(time.Second)

	li, _, err = st1.LastIndex()
	if err != nil {
		panic(err)
	}
	utils.Assert(li, uint64(7))

	fi, _, err = st1.FirstIndex()
	utils.Assert(fi, uint64(1))

	term, find, _, err = st1.Term(7)
	utils.Assert(err, nil)
	utils.Assert(find, true)
	utils.Assert(term, uint64(2))

	time.Sleep(time.Second * 2)
}

func TestRaft(t *testing.T) {
	singleNode()
}
