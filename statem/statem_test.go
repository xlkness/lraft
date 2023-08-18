package statem

import (
	"fmt"
	"testing"
)

const (
	SimpleOpenCloseState_Open int = 1 + iota
	SimpleOpenCloseState_Close
)

type KVTimeoutTestValue struct {
	TimeoutType uint32
	Value       any
}

type SimpleOpenClose struct {
	sm          *Machine[int]
	tickCounter int
}

// 这里自己封装一个状态机状态获取供外部调用
func (soc *SimpleOpenClose) State() int {
	return soc.sm.State().State
}

func TestSimpleOpenClose(t *testing.T) {
	soc := &SimpleOpenClose{}
	states := []*StateConf[int]{
		{Current: SimpleOpenCloseState_Close,
			EntryCallback: soc.enterClose, ExitCallback: soc.exitClose, HandleEvent: soc.handleEventClose},
		{Current: SimpleOpenCloseState_Open,
			EntryCallback: soc.enterOpen, ExitCallback: soc.exitOpen, HandleEvent: soc.handleEventOpen},
	}
	stateM := NewStateMachine[int]().Init(SimpleOpenCloseState_Close, states)

	assert(stateM.curState.State, SimpleOpenCloseState_Close)
	assert(stateM.curState.CustomData, "test custom data")

	soc.sm = stateM

	// 打开开关
	err := soc.sm.GotoState(SimpleOpenCloseState_Open)
	if err != nil {
		t.Fatal(err)
	}

	assert(stateM.curState.State, SimpleOpenCloseState_Open)
	assert(stateM.curState.CustomData, 123)

	// tick
	soc.sm.Tick()
	soc.sm.Tick()

	// tick两次，定时器1超时，修改counter为1,customData为234
	_, find := soc.sm.GetKeyValueTimeout(fmt.Sprintf("test_key:%v", 123))
	assert(soc.tickCounter, 1)
	assert(soc.sm.State().CustomData, 234)
	assert(find, false)

	// 删除tick三次的定时器
	soc.sm.DelKeyValueTimeout(123)
	_, find = soc.sm.GetKeyValueTimeout(123)
	assert(find, false)

	assert(stateM.curState.State, SimpleOpenCloseState_Open)
	assert(stateM.curState.CustomData, 234)

	event := 124323423
	soc.sm.HandleEvent(event)
	assert(stateM.curState.CustomData, event)
}

func (oc *SimpleOpenClose) enterClose(s *StateData) {
	s.CustomData = "test custom data"
}

func (oc *SimpleOpenClose) enterOpen(s *StateData) {
	s.CustomData = 123
	// 开关进入打开状态就添加两个kv自定义超时器，如果超时之前没有被删掉的话表示触发了这个自定义动作
	err := oc.sm.InsertKeyValueTimeout(&KVTimeout{
		Key:         fmt.Sprintf("test_key:%v", 123),
		Value:       &KVTimeoutTestValue{TimeoutType: 2, Value: 123},
		TimeoutTick: 2,
		Callback: func(key, value interface{}) {
			oc.tickCounter = 1
			oc.sm.State().CustomData = 234
		},
	})
	if err != nil {
		panic(err)
	}

	_, find := oc.sm.GetKeyValueTimeout(fmt.Sprintf("test_key:%v", 123))
	assert(find, true)

	err = oc.sm.InsertKeyValueTimeout(&KVTimeout{
		Key:         123,
		Value:       "test_value",
		TimeoutTick: 3,
		Callback: func(key, value any) {
			panic(key)
		},
	})
	if err != nil {
		panic(err)
	}
}
func (oc *SimpleOpenClose) exitClose(s *StateData) {
	assert(s.CustomData, "test custom data")
}
func (oc *SimpleOpenClose) exitOpen(s *StateData) {
	s.CustomData = nil
}

func (oc *SimpleOpenClose) handleEventOpen(event int) {
	oc.sm.State().CustomData = event
}

func (oc *SimpleOpenClose) handleEventClose(event int) {

}

func assert(cur, expected any) {
	if cur != expected {
		panic(fmt.Errorf("expected value:%v, but:%v", expected, cur))
	}
}
