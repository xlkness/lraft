package lraft

import (
	"sort"
)

type peerRecord struct {
	LogMatch uint64 // 已知复制到节点的最高日志条目号，初始为0
	LogNext  uint64 // 发送给节点下一条日志条目索引号，一般为LogMatch+1，初始为leader最高索引号+1
}

func (pr *peerRecord) goNext(index uint64) {
	pr.LogMatch = max(pr.LogMatch, index)
	pr.LogNext = max(pr.LogNext, index+1)
}

func (pr *peerRecord) goBack(reqAppendIndex, rejectedIndex uint64) (conflict bool) {
	if pr.LogMatch != 0 {
		if pr.LogMatch >= reqAppendIndex {
			return false
		}

		pr.LogNext = pr.LogMatch + 1
		return true
	}

	if pr.LogNext-1 != reqAppendIndex {
		return false
	}

	pr.LogNext = min(reqAppendIndex, rejectedIndex+1, 1)
	return true
}

type peersRecord map[int64]*peerRecord

func (psr peersRecord) findPeer(id int64) *peerRecord {
	pr, find := psr[id]
	if !find {
		return &peerRecord{}
	}
	return pr
}

func (psr peersRecord) calcQuorumLogProgress() uint64 {
	progressList := make([]uint64, 0, len(psr))
	for _, v := range psr {
		progressList = append(progressList, v.LogMatch)
	}
	sort.SliceStable(progressList, func(i, j int) bool {
		return progressList[i] > progressList[j]
	})
	return progressList[len(psr)/2]
}
