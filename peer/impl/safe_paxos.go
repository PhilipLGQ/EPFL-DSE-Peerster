package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

type SafePaxos struct {
	sync.RWMutex
	clock         uint
	MaxID         uint
	AcceptedValue types.PaxosValue
	AcceptedID    uint
	promises      []types.PaxosPromiseMessage
	accepts       map[types.PaxosValue]uint
	tlcs          map[uint][]types.TLCMessage
	consensus     map[uint]types.PaxosValue
	preparedFlag  map[uint]bool
	proposedValue types.PaxosValue
	acceptedMap   map[uint]types.PaxosValue
}

func NewSafePaxos() *SafePaxos {
	return &SafePaxos{
		clock:         0,
		MaxID:         0,
		AcceptedValue: types.PaxosValue{},
		AcceptedID:    0,
		promises:      nil,
		accepts:       map[types.PaxosValue]uint{},
		tlcs:          map[uint][]types.TLCMessage{},
		consensus:     map[uint]types.PaxosValue{},
		preparedFlag:  map[uint]bool{},
		acceptedMap:   map[uint]types.PaxosValue{},
		proposedValue: types.PaxosValue{},
	}
}

func (p *SafePaxos) Reset(val types.PaxosValue) {
	p.Lock()
	defer p.Unlock()
	p.MaxID = 0
	p.AcceptedID = 0
	p.AcceptedValue = types.PaxosValue{}
	p.proposedValue = val
	p.promises = nil
	p.accepts = map[types.PaxosValue]uint{}
}

func (p *SafePaxos) ReceivePrepare(prepare types.PaxosPrepareMessage) (types.PaxosValue, uint, bool) {
	p.Lock()
	defer p.Unlock()
	//1. Ignore messages whose Step field do not match your current logical clock
	if prepare.Step != p.clock {
		return p.AcceptedValue, p.AcceptedID, false
	}

	//2. Ignore messages whose ID is not greater than MaxID
	if prepare.ID <= p.MaxID {
		return p.AcceptedValue, p.AcceptedID, false
	}

	//3. update MaxID, and return true
	p.MaxID = prepare.ID
	return p.AcceptedValue, p.AcceptedID, true
}

func (p *SafePaxos) ReceivePropose(propose types.PaxosProposeMessage) bool {
	p.Lock()
	defer p.Unlock()
	//1. Ignore messages whose Step field do not match your current logical clock
	if propose.Step != p.clock {
		return false
	}

	//2. Ignore messages whose ID isn't equal to MaxID
	if propose.ID != p.MaxID {
		return false
	}

	//3. Update AcceptedValue and AcceptedID
	p.AcceptedValue = propose.Value
	p.AcceptedID = propose.ID
	return true
}

func (p *SafePaxos) GetStep() uint {
	p.RLock()
	defer p.RUnlock()
	return p.clock
}

func (p *SafePaxos) ReceivePromise(promise *types.PaxosPromiseMessage, threshold uint) {
	p.Lock()
	defer p.Unlock()
	//1. Ignore messages whose Step field do not match your current logical clock
	if promise.Step != p.clock {
		return
	}

	//2. Add the promise to list
	p.promises = append(p.promises, *promise)

	//3. update prepared flags
	if p.preparedFlag[p.clock] {
		return
	}
	if len(p.promises) < int(threshold) {
		return
	}
	p.preparedFlag[p.clock] = true
}

func (p *SafePaxos) ReceiveAccept(accept *types.PaxosAcceptMessage, threshold uint) (types.PaxosValue, int) {
	p.Lock()
	defer p.Unlock()
	// Ignore msg.Step != current logical clock
	if accept.Step != p.clock {
		return types.PaxosValue{}, -1
	}
	val, ok := p.acceptedMap[p.clock]
	if ok {
		return val, int(p.clock)
	}
	p.accepts[accept.Value]++
	if p.accepts[accept.Value] < threshold {
		return types.PaxosValue{}, -1
	}
	p.acceptedMap[p.clock] = accept.Value
	return accept.Value, int(p.clock)
}

func (p *SafePaxos) GetPromiseValue(step uint) (types.PaxosValue, bool) {
	p.RLock()
	defer p.RUnlock()
	_, ok := p.preparedFlag[step]
	if !ok {
		return types.PaxosValue{}, false
	}
	if p.clock != step {
		return types.PaxosValue{}, false
	}
	ret := p.proposedValue
	maxID := uint(0)
	for _, promise := range p.promises {
		if promise.AcceptedValue != nil && promise.AcceptedID > maxID {
			maxID = promise.AcceptedID
			ret = *promise.AcceptedValue
		}
	}
	return ret, true
}

func (p *SafePaxos) CheckAccept(step uint) bool {
	p.RLock()
	defer p.RUnlock()
	_, ok := p.acceptedMap[step]
	return ok
}

func (p *SafePaxos) ReceiveTLC(tlc *types.TLCMessage, threshold uint) {
	p.Lock()
	defer p.Unlock()
	if tlc.Step >= p.clock {
		p.tlcs[tlc.Step] = append(p.tlcs[tlc.Step], *tlc)
		if len(p.tlcs[tlc.Step]) >= int(threshold) {
			p.consensus[tlc.Step] = tlc.Block.Value
		}
	}
}

func (p *SafePaxos) GetBlockAndAdvance(step uint) (types.BlockchainBlock, bool) {
	p.Lock()
	defer p.Unlock()

	// 0. we ignore the messages that do not belong to current step
	if step != p.clock {
		return types.BlockchainBlock{}, false
	}

	_, ok := p.consensus[step]
	if !ok {
		return types.BlockchainBlock{}, false
	}

	// 1. it means that we have consensus and we can move on
	p.clock++
	p.MaxID = 0
	p.AcceptedID = 0
	p.AcceptedValue = types.PaxosValue{}
	p.proposedValue = types.PaxosValue{}
	p.promises = nil
	p.accepts = map[types.PaxosValue]uint{}

	// 2. return blocks in tlc
	return p.tlcs[step][0].Block, true

}
func (p *SafePaxos) GetConsensus(step uint) (types.PaxosValue, bool) {
	p.RLock()
	defer p.RUnlock()
	val, ok := p.consensus[step]
	return val, ok
}

func (p *SafePaxos) UpdateAcceptedMap(step uint, val types.PaxosValue) {
	p.Lock()
	defer p.Unlock()
	p.acceptedMap[step] = val
}

func (p *SafePaxos) Proposed() bool {
	p.RLock()
	defer p.RUnlock()
	return p.proposedValue != types.PaxosValue{}
}
