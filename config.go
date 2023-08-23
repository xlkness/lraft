package lraft

import (
	"fmt"
	"time"
)

type Config struct {
	// 定时驱动raft算法的定时器间隔，注意raft算法要求：广播集群时间<选举超时时间
	TickDuration time.Duration
	// leader广播心跳的间隔数，最终间隔时间为：TickDuration * BroadcastHeartBeatTimerTick
	// 注意比CanElectionTimerTick小
	BroadcastHeartBeatTimerTick int
	// follower间隔时间内没收到leader心跳就发起选举的间隔数，最终间隔时间为：TickDuration * CanElectionTimerTick
	CanElectionTimerTick int
	// candiate间隔时间内还没有完成投票结果判定，就认为超时切换为follower，最终间隔时间为：TickDuration * RequestVoteTimeoutTick
	// 可以和CanElectionTimerTick一样
	RequestVoteTimeoutTick int
}

func (c *Config) checkAndCorrect() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}

	if c.TickDuration <= 0 {
		c.TickDuration = time.Millisecond * time.Duration(100)
	}
	if c.BroadcastHeartBeatTimerTick <= 0 {
		c.BroadcastHeartBeatTimerTick = 1
	}
	if c.CanElectionTimerTick <= 0 {
		c.CanElectionTimerTick = 3
	}
	if c.RequestVoteTimeoutTick <= 0 {
		c.RequestVoteTimeoutTick = 3
	}

	if c.BroadcastHeartBeatTimerTick >= c.RequestVoteTimeoutTick {
		return fmt.Errorf("config BroadcastHeartBeatTimerTick must bigger than RequestVoteTimeoutTick:%v>=%v",
			c.BroadcastHeartBeatTimerTick, c.RequestVoteTimeoutTick)
	}

	return nil
}
