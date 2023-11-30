package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

type Paxos struct {
	sync.RWMutex
	clock         uint
	MaxID         uint
	AcceptedID    uint
	AcceptedValue types.PaxosValue
	proposedValue types.PaxosValue
	promises      []types.PaxosPromiseMessage
	accepts       map[types.PaxosValue]uint
	tlcs          map[uint][]types.TLCMessage
	consensus     map[uint]types.PaxosValue
	preparedFlag  map[uint]bool
	acceptedMap   map[uint]types.PaxosValue
}

func InitiatorPaxos() *Paxos {
	return &Paxos{
		clock:         0,
		MaxID:         0,
		AcceptedID:    0,
		AcceptedValue: types.PaxosValue{},
		proposedValue: types.PaxosValue{},
		promises:      nil,
		accepts:       map[types.PaxosValue]uint{},
		tlcs:          map[uint][]types.TLCMessage{},
		consensus:     map[uint]types.PaxosValue{},
		preparedFlag:  map[uint]bool{},
		acceptedMap:   map[uint]types.PaxosValue{},
	}
}

func (p *Paxos) CheckStepAccepted(step uint) (accepted bool) {
	p.RLock()
	defer p.RUnlock()
	_, accepted = p.acceptedMap[step]
	return accepted
}

func (p *Paxos) CheckProposed() bool {
	p.RLock()
	defer p.RUnlock()
	return p.proposedValue != types.PaxosValue{}
}

func (p *Paxos) GetCurrStep() uint {
	p.RLock()
	defer p.RUnlock()
	return p.clock
}

func (p *Paxos) GetStepConsensus(step uint) (cVal types.PaxosValue, exist bool) {
	p.RLock()
	defer p.RUnlock()
	cVal, exist = p.consensus[step]
	return cVal, exist
}

func (p *Paxos) GetPromiseVal(step uint) (types.PaxosValue, bool) {
	p.RLock()
	defer p.RUnlock()
	// If node is not ready propose the value, simply return
	_, ok := p.preparedFlag[step]
	if !ok {
		return types.PaxosValue{}, false
	}
	// If steps not match, simply return
	if step != p.clock {
		return types.PaxosValue{}, false
	}
	pVal := p.proposedValue
	maxID := uint(0)
	for _, pPromMsg := range p.promises {
		if pPromMsg.AcceptedValue != nil && pPromMsg.AcceptedID > maxID {
			maxID = pPromMsg.AcceptedID
			pVal = *pPromMsg.AcceptedValue
		}
	}
	return pVal, true
}

func (p *Paxos) MarkStepAccepted(step uint, cVal types.PaxosValue) {
	p.Lock()
	defer p.Unlock()
	p.acceptedMap[step] = cVal
}

func (p *Paxos) ProposeReset(val types.PaxosValue) {
	p.Lock()
	defer p.Unlock()
	p.MaxID = 0
	p.AcceptedID = 0
	p.AcceptedValue = types.PaxosValue{}
	p.proposedValue = val
	p.promises = nil
	p.accepts = map[types.PaxosValue]uint{}
}

func (p *Paxos) TLCStepForward(step uint) (types.BlockchainBlock, bool) {
	p.Lock()
	defer p.Unlock()
	// Ignore messages with step not matching paxos current step
	if step != p.clock {
		return types.BlockchainBlock{}, false
	}
	// If no consensus reached before at current step, stop TLC step forwarding
	_, ok := p.consensus[step]
	if !ok {
		return types.BlockchainBlock{}, false
	}

	// Reached consensus, move on to next step and refresh step attributes
	p.clock++
	p.MaxID = 0
	p.AcceptedID = 0
	p.AcceptedValue = types.PaxosValue{}
	p.proposedValue = types.PaxosValue{}
	p.promises = nil
	p.accepts = map[types.PaxosValue]uint{}

	// Return the consensus block of prev step
	return p.tlcs[step][0].Block, true
}

func (p *Paxos) ProcessPaxosAcceptMessage(pAccMsg *types.PaxosAcceptMessage, threshold uint) (types.PaxosValue, int) {
	p.Lock()
	defer p.Unlock()
	// Ignore messages with steps not match
	if pAccMsg.Step != p.clock {
		return types.PaxosValue{}, -1
	}
	// If already accepted a value at current step, return it
	val, ok := p.acceptedMap[p.clock]
	if ok {
		return val, int(p.clock)
	}
	// Otherwise we add current value count by 1 and check if we reached threshold
	p.accepts[pAccMsg.Value]++
	if p.accepts[pAccMsg.Value] < threshold {
		return types.PaxosValue{}, -1
	}
	p.acceptedMap[p.clock] = pAccMsg.Value
	return pAccMsg.Value, int(p.clock)
}
