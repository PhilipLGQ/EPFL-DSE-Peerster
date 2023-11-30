package impl

import (
	"bytes"
	"encoding/hex"
	"github.com/rs/zerolog/log"
	"strconv"
	"time"

	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
)

func (n *node) BroadcastError() {
	//do nothing
}

func (n *node) BroadcastTLCMessage(step uint, block types.BlockchainBlock) error {
	tlcMsg := types.TLCMessage{Step: step, Block: block}
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(tlcMsg)
	if err != nil {
		return err
	}

	go func() {
		err := n.Broadcast(tMsg)
		if err != nil {
			log.Error().Msgf("Error occurred when broadcasting TLC Message: %v",
				err.Error())
		}
	}()
	return nil
}

func (n *node) CreateBlock(cVal types.PaxosValue, step uint) types.BlockchainBlock {
	prevHash := n.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey)
	if prevHash == nil {
		prevHash = make([]byte, 32)
	}
	buf := bytes.Buffer{}
	buf.WriteString(strconv.Itoa(int(step)))
	buf.WriteString(cVal.UniqID)
	buf.WriteString(cVal.Filename)
	buf.WriteString(cVal.Metahash)
	buf.Write(prevHash)
	h := SHA256(buf.Bytes())
	return types.BlockchainBlock{
		Index:    step,
		Value:    cVal,
		PrevHash: prevHash,
		Hash:     h,
	}
}

func (n *node) TLCForward(step uint, catchUp bool) error {
	block, forward := n.paxos.TLCStepForward(step)
	// If steps not match or no consensus value reached at current step, simply return
	if !forward {
		return nil
	}

	// If we can move on to next TLC step, add current step's block to node's blockchain
	bChain := n.conf.Storage.GetBlockchainStore()
	bBytes, err := block.Marshal()
	if err != nil {
		return err
	}
	bChain.Set(hex.EncodeToString(block.Hash), bBytes)
	bChain.Set(storage.LastBlockKey, block.Hash)
	n.conf.Storage.GetNamingStore().Set(block.Value.Filename,
		[]byte(block.Value.Metahash)) // Also update node's name store

	// If in catchup mode or we accepted before (broadcast), skip the broadcasting of TLC messages
	if !(catchUp || n.paxos.CheckStepAccepted(step)) {
		// Mark accepted & broadcast for current step
		n.paxos.MarkStepAccepted(step, block.Value)

		// Create and broadcast TLC message
		err = n.BroadcastTLCMessage(step, block)
		if err != nil {
			return err
		}
	}
	return n.TLCForward(step+1, true) // Catchup mode for future steps
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

func (n *node) ProposeConsensus(pVal types.PaxosValue, retry uint) (types.PaxosValue, error) {
	// Reset current retry attributes for a proposed value
	n.paxos.ProposeReset(pVal)

	// Paxos Phase 1
	currStep := n.paxos.GetCurrStep()
	val, next, err := n.PaxosPhase1(currStep, retry)
	if err != nil {
		return types.PaxosValue{}, err
	}
	if !next { // Retry if timeout, with retry + 1
		return n.ProposeConsensus(pVal, retry+1)
	}
	// Paxos Phase 2
	next, err = n.PaxosPhase2(val, currStep, retry)
	if err != nil {
		return types.PaxosValue{}, err
	}
	if !next { // Retry if timeout, with retry + 1
		return n.ProposeConsensus(pVal, retry+1)
	}
	// Check until a consensus value reached at the current step and return it
	for {
		cVal, reached := n.paxos.GetStepConsensus(currStep)
		if reached {
			return cVal, nil
		}
	}
}

func (n *node) PaxosPhase1(currStep, retry uint) (types.PaxosValue, bool, error) {
	// Create Paxos prepare message and broadcast
	pPrepMsg := types.PaxosPrepareMessage{
		Step:   currStep,
		ID:     n.conf.PaxosID + retry*n.conf.TotalPeers,
		Source: n.conf.Socket.GetAddress(),
	}
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(&pPrepMsg)
	if err != nil {
		return types.PaxosValue{}, false, err
	}
	go func() {
		err := n.Broadcast(tMsg)
		if err != nil {
			log.Error().Msgf("Error occurred when broadcasting Paxos propose message in"+
				"Phase 2: %v", err.Error())
		}
	}()
	// Wait for Paxos promise responses until timeout
	timeout := time.After(n.conf.PaxosProposerRetry)
	for {
		select {
		case <-timeout: // Retry if timeout
			return types.PaxosValue{}, false, nil
		default:
			promiseVal, ok := n.paxos.GetPromiseVal(currStep)
			if ok {
				return promiseVal, true, nil
			}
		}
	}
}

func (n *node) PaxosPhase2(val types.PaxosValue, currStep, retry uint) (bool, error) {
	// Create Paxos propose message and broadcast
	propose := types.PaxosProposeMessage{
		Step:  currStep,
		ID:    n.conf.PaxosID + retry*n.conf.TotalPeers,
		Value: val,
	}
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(&propose)
	if err != nil {
		return false, err
	}
	go func() {
		err := n.Broadcast(tMsg)
		if err != nil {
			log.Error().Msgf("Error occurred when broadcasting Paxos prepare message in"+
				"Phase 2: %v", err.Error())
		}
	}()
	// Wait for Paxos accept responses until timeout
	timeout := time.After(n.conf.PaxosProposerRetry)
	for {
		select {
		case <-timeout: // Retry if timeout
			return false, nil
		default: // If we have an accepted value, process forward to check the consensus value
			if n.paxos.CheckStepAccepted(currStep) {
				return true, nil
			}
		}
	}
}
