package impl

import (
	"bytes"
	"encoding/hex"
	"strconv"
	"time"

	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
)

func (n *node) BroadcastError() {
	//do nothing
}

func (n *node) ResponsePrepare(prepare *types.PaxosPrepareMessage, val types.PaxosValue, id uint) error {
	// 1. prepare paxos promise message
	promise := types.PaxosPromiseMessage{
		Step:          prepare.Step,
		ID:            prepare.ID,
		AcceptedID:    id,
		AcceptedValue: &val,
	}
	if (val == types.PaxosValue{}) {
		promise.AcceptedValue = nil
	}
	transPromise, err := n.conf.MessageRegistry.MarshalMessage(&promise)
	if err != nil {
		return err
	}

	// 2. wrap it in private message
	private := types.PrivateMessage{
		Recipients: map[string]struct{}{prepare.Source: {}},
		Msg:        &transPromise,
	}
	transPrivate, err := n.conf.MessageRegistry.MarshalMessage(&private)
	if err != nil {
		return err
	}
	go func() {
		err := n.Broadcast(transPrivate)
		if err != nil {
			n.BroadcastError()
		}
	}()
	return nil
}

func (n *node) ResponsePropose(propose *types.PaxosProposeMessage) error {
	// 1. prepare paxos accept message
	accept := types.PaxosAcceptMessage{
		Step:  propose.Step,
		ID:    propose.ID,
		Value: propose.Value,
	}

	// 2. broadcast it
	transAccept, err := n.conf.MessageRegistry.MarshalMessage(&accept)
	if err != nil {
		return err
	}
	go func() {
		err := n.Broadcast(transAccept)
		if err != nil {
			n.BroadcastError()
		}
	}()
	return nil
}

func (n *node) Propose(round uint, proposedVal types.PaxosValue) (types.PaxosValue, error) {
	n.paxos.Reset(proposedVal)
	curStep := n.paxos.GetStep()

	// phase 1
	val, ok, err := n.ProposePhase1(round, curStep)
	if err != nil {
		return types.PaxosValue{}, err
	}
	if !ok {
		return n.Propose(round+1, proposedVal)
	}

	// phase 2
	ok, err = n.ProposePhase2(round, val, curStep)
	if err != nil {
		return types.PaxosValue{}, err
	}
	if !ok {
		return n.Propose(round+1, proposedVal)
	}

	// tcl
	for {
		consensus, ok := n.paxos.GetConsensus(curStep)
		if ok {
			return consensus, nil
		}
	}
}

func (n *node) ProposePhase1(round uint, curStep uint) (types.PaxosValue, bool, error) {
	// 1. prepare a prepare message
	prepare := types.PaxosPrepareMessage{
		Step:   curStep,
		ID:     n.PaxosID + round*n.TotalPeers,
		Source: n.myAddr,
	}

	// 2. broadcast it
	transPrepare, err := n.conf.MessageRegistry.MarshalMessage(&prepare)
	if err != nil {
		return types.PaxosValue{}, false, err
	}
	go func() {
		err := n.Broadcast(transPrepare)
		if err != nil {
			n.BroadcastError()
		}
	}()

	// 3. wait for promise message
	timeout := time.After(n.PaxosProposerRetry)
	for {
		select {
		case <-timeout:
			// try again
			return types.PaxosValue{}, false, nil
		default:
			// check if collecting enough reply
			promiseVal, ok := n.paxos.GetPromiseValue(curStep)
			if ok {
				return promiseVal, true, nil
			}
		}
	}
}

func (n *node) ProposePhase2(round uint, val types.PaxosValue, curStep uint) (bool, error) {
	// 1. prepare a propose message
	propose := types.PaxosProposeMessage{
		Step:  curStep,
		ID:    n.PaxosID + round*n.TotalPeers,
		Value: val,
	}

	// 2. broadcast it
	transPropose, err := n.conf.MessageRegistry.MarshalMessage(&propose)
	if err != nil {
		return false, err
	}
	go func() {
		err := n.Broadcast(transPropose)
		if err != nil {
			n.BroadcastError()
		}
	}()

	// 3. wait for accept messages
	timeout := time.After(n.PaxosProposerRetry)
	for {
		select {
		case <-timeout:
			// try again
			return false, nil
		default:
			// check if collecting enough reply
			if n.paxos.CheckAccept(curStep) {
				return true, nil
			}
		}
	}
}

func (n *node) GetBlock(consensus types.PaxosValue, step uint) types.BlockchainBlock {
	prevHash := n.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey)
	if prevHash == nil {
		prevHash = make([]byte, 32)
	}
	hashBuf := bytes.Buffer{}
	// error will always be nil, no need to handle
	hashBuf.WriteString(strconv.FormatUint(uint64(step), 10))
	hashBuf.WriteString(consensus.UniqID)
	hashBuf.WriteString(consensus.Filename)
	hashBuf.WriteString(consensus.Metahash)
	hashBuf.Write(prevHash)
	hash := SHA256(hashBuf.Bytes())
	return types.BlockchainBlock{
		Index:    step,
		Value:    consensus,
		PrevHash: prevHash,
		Hash:     hash,
	}
}

func (n *node) BroadcastTLC(step uint, block types.BlockchainBlock) error {
	TLCMsg := types.TLCMessage{
		Step:  step,
		Block: block,
	}
	transTLCMsg, err := n.conf.MessageRegistry.MarshalMessage(TLCMsg)
	if err != nil {
		return err
	}
	go func() {
		err := n.Broadcast(transTLCMsg)
		if err != nil {
			n.BroadcastError()
		}
	}()

	return nil
}

func (n *node) TLCProcess(step uint, catchUp bool) error {
	block, ok := n.paxos.GetBlockAndAdvance(step)
	if !ok {
		return nil
	}

	// 1. update block chain
	chain := n.conf.Storage.GetBlockchainStore()
	blockBytes, err := block.Marshal()
	if err != nil {
		return err
	}
	chain.Set(hex.EncodeToString(block.Hash), blockBytes)
	chain.Set(storage.LastBlockKey, block.Hash)

	// 2. update namestore
	n.conf.Storage.GetNamingStore().Set(block.Value.Filename, []byte(block.Value.Metahash))

	if !(catchUp || n.paxos.CheckAccept(step)) {
		// 3. update AcceptedMap
		n.paxos.UpdateAcceptedMap(step, block.Value)

		// 4. try to broadcast tlc
		err = n.BroadcastTLC(step, block)
		if err != nil {
			return err
		}
	}

	return n.TLCProcess(step+1, true)
}
