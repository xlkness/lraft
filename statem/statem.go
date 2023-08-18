package statem

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"lraft/message"
)

type KVTimeout struct {
	Key   any
	Value any
	// 超时的滴答时间
	TimeoutTick int
	// 超时的回调函数
	Callback func(key, value any)
	// 超时的滴答时间计数，达到TimeoutTick定时器就超时
	timeoutCounter int
}

type StateConf struct {
	// 当前状态
	Current int
	// 切换到当前状态执行的回调函数，为nil表示无
	EntryCallback func(*StateData)
	// 离开当前状态执行的回调函数，为nil表示无
	ExitCallback func(*StateData)
	// 调用方自定义的消息处理方法，保持消息处理和状态一致，为nil表示无
	HandleEvent func(msgID message.MessageID, payload proto.Message)
}

type StateData struct {
	State int
	// 当前状态维护的自定义数据，如果状态切换回调不改变，则这个值会一直保持一样
	CustomData any
}

type Machine struct {
	// 当前状态
	// todo 当前状态机只能单线程使用，如果多线程要获取当前状态，要在进入状态的回调自己封装状态标记
	curState *StateData
	// 当前状态中用于自定义的超时时间，切换状态会清空
	// 例如房间状态机中，如果某个玩家在准备阶段10s未准备则踢出去
	kvTimeouts map[any]*KVTimeout
	// 状态描述列表
	states map[int]*StateConf
}

// NewStateMachine 创建新的状态机
// 状态机不支持并发访问
func NewStateMachine(customData any) *Machine {
	m := &Machine{
		curState: &StateData{
			CustomData: customData,
		},
	}
	return m
}

func (m *Machine) Init(states []*StateConf) *Machine {
	// valid check
	stateList := make(map[int]*StateConf)
	for _, v := range states {
		if _, find := stateList[v.Current]; find {
			panic(fmt.Errorf("reduplicated state:%v", v.Current))
		}
		stateList[v.Current] = v
	}

	m.kvTimeouts = make(map[interface{}]*KVTimeout)
	m.states = stateList
	m.curState = &StateData{}

	return m
}

// State 获取当前的状态的所有信息
func (m *Machine) State() *StateData {
	return m.curState
}

// GotoState 跳指定状态
func (m *Machine) GotoState(state int) error {
	return m.gotoState(state)
}

// Tick 调用方间隔调用，超时时间为秒
func (m *Machine) Tick() error {
	// check key value timeout
	for k, v := range m.kvTimeouts {
		v.timeoutCounter++
		if v.TimeoutTick <= v.timeoutCounter {
			m.delKeyValueTimeout(k)
			v.Callback(k, v.Value)
		}
	}
	return nil
}

func (m *Machine) HandleEvent(msgID message.MessageID, payload proto.Message) {
	curStateConf, find := m.states[m.curState.State]
	if !find {
		panic(m.curState.State)
	}
	if curStateConf.HandleEvent != nil {
		curStateConf.HandleEvent(msgID, payload)
	}
}

// InsertKeyValueTimeout 插入新的kv超时提醒，存在返回错误
func (m *Machine) InsertKeyValueTimeout(kvInfo *KVTimeout) {
	m.kvTimeouts[kvInfo.Key] = kvInfo
	return
}

// UpdateKeyValueTimeout 更新旧的kv超时提醒，不存在则插入
func (m *Machine) UpdateKeyValueTimer(kvInfo *KVTimeout) {
	if oldV, find := m.kvTimeouts[kvInfo.Key]; find {
		oldV.Value = kvInfo.Value
		oldV.TimeoutTick = kvInfo.timeoutCounter
		return
	}
	m.kvTimeouts[kvInfo.Key] = kvInfo
}

func (m *Machine) ResetKeValueTimer(key any, timeoutTick int) {
	if oldV, find := m.kvTimeouts[key]; find {
		oldV.TimeoutTick = timeoutTick
		oldV.timeoutCounter = 0
	}
}

// DelKeyValueTimeout 删除kv超时提醒
func (m *Machine) DelKeyValueTimer(key any) {
	m.delKeyValueTimeout(key)
}

func (m *Machine) GetKeyValueTimer(key any) (*KVTimeout, bool) {
	v, f := m.kvTimeouts[key]
	return v, f
}

func (m *Machine) gotoState(state int) error {
	if state == 0 {
		return nil
	}
	if stateConf, find := m.states[state]; find {
		if m.curState.State == state {
			return nil
		}
		m.curState.State = stateConf.Current
		m.kvTimeouts = make(map[interface{}]*KVTimeout)
		if stateConf.EntryCallback != nil {
			stateConf.EntryCallback(m.curState)
		}
		return nil
	}
	return fmt.Errorf("not found state:%v", state)
}

// delKeyValueTimeout 删除kv超时提醒
func (m *Machine) delKeyValueTimeout(key any) {
	delete(m.kvTimeouts, key)
}
