package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"regexp"
	"time"
)

// ExecChatMessage: the ChatMessage handler. This function will be called when a chat message is received.
func (n *node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type. You assume it is the right type.
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	log.Info().Msg(chatMsg.String())
	return nil
}

// ExecRumorsMessage: the RumorsMessage handler.
func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	n.rumorMu.Lock()
	// Cast the message to its actual type
	rMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		n.rumorMu.Unlock()
		return xerrors.Errorf("wrong type: %T", msg)
	}
	var rmExpected = false
	// Process all rumors in the RumorsMessage
	for _, rumor := range rMsg.Rumors {
		if !n.rumorP.CheckAddrSeq(rumor.Origin) {
			n.rumorP.InitAddrSeq(rumor.Origin)
		}
		// If expected record and process, if not save those newer rumors to buffer
		if n.rumorP.Expected(rumor.Origin, rumor.Sequence) {
			rmExpected = true
			// Process and record received rumors
			packet := transport.Packet{
				Header: pkt.Header,
				Msg:    rumor.Msg,
			}
			err := n.conf.MessageRegistry.ProcessPacket(packet)
			if err != nil {
				log.Error().Msgf("error: %v", err.Error())
			}
			n.rumorP.Record(rumor, rumor.Origin, &n.tbl, pkt.Header.RelayedBy)

			err = n.ProcessNewRumors(rumor.Origin)
			if err != nil {
				return err
			}

		} else if n.rumorP.CheckSeqNew(rumor.Origin, rumor.Sequence) {
			n.rumorB.AddRumor(rumor.Origin, DetailRumor{rumor: rumor, pkt: pkt})
		}
	}
	// Sends an AckMessage back to source
	sMsg := types.StatusMessage(n.rumorP.GetNodeView())
	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        sMsg,
	}
	tAck, err := n.conf.MessageRegistry.MarshalMessage(ack)
	if err != nil {
		n.rumorMu.Unlock()
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(),
		pkt.Header.Source, 0)
	packet := transport.Packet{Header: &header, Msg: &tAck}
	n.rumorMu.Unlock()
	err = n.conf.Socket.Send(pkt.Header.Source, packet, time.Second)
	if err != nil {
		return err
	}
	// Send the RumorMessage to another random neighbor (if >= 1 expected rumor data exist)
	if rmExpected {
		rdmNeighbor, exist := n.tbl.RandomNeighbor([]string{n.conf.Socket.GetAddress(), pkt.Header.Source})
		if !exist { // Return if no other neighbor exists
			return nil
		}
		rheader := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(),
			rdmNeighbor, 0)
		rpacket := transport.Packet{Header: &rheader, Msg: pkt.Msg}
		// Ask for ACK
		n.ackSig.Request(rpacket.Header.PacketID)
		return n.conf.Socket.Send(rdmNeighbor, rpacket, time.Second)
	}
	return nil
}

// ExecAckMessage: the AckMessage handler.
func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type
	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// Stops waiting corresponds to PacketID
	n.ackSig.Signal(ackMsg.AckedPacketID)
	// Process the status message contained
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(ackMsg.Status)
	if err != nil {
		return err
	}
	packet := transport.Packet{Header: pkt.Header, Msg: &tMsg}
	return n.conf.MessageRegistry.ProcessPacket(packet)
}

// ExecStatusMessage: the StatusMessage handler.
func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	n.rumorMu.Lock()
	defer n.rumorMu.Unlock()
	if msg == nil {
		return xerrors.Errorf("Message is nil")
	}
	// Cast the message to its actual type
	sMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// If they are identical, execute the continue-mongering mechanism
	if n.IdenticalView(*sMsg, n.rumorP.GetNodeView()) {
		// If views are identical, send this peer's view randomly to a neighbor
		if rand.Float64() < n.conf.ContinueMongering {
			return n.SendNodeView("", []string{pkt.Header.Source})
		}
		return nil
	}
	// If they are not identical, process as each situation required
	compareResult, compareDiff := n.CheckViewDiff(n.rumorP.GetNodeView(), *sMsg)
	switch compareResult {
	case 1:
		err := n.SendNodeView(pkt.Header.Source, []string{})
		if err != nil {
			return err
		}
		return nil
	case 2:
		err := n.SendMissingRumors(pkt.Header.Source, *sMsg, compareDiff)
		if err != nil {
			return err
		}
		return nil
	case 3:
		err := n.SendNodeView(pkt.Header.Source, []string{})
		if err != nil {
			return err
		}
		err = n.SendMissingRumors(pkt.Header.Source, *sMsg, compareDiff)
		if err != nil {
			return err
		}
		return nil
	}
	return xerrors.Errorf("Error occurred when checking the view difference!")
}

// ExecEmptyMessage: the EmptyMessage handler.
func (n *node) ExecEmptyMessage(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type
	_, ok := msg.(*types.EmptyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	return nil
}

// ExecPrivateMessage: the PrivateMessage handler.
func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type
	pMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// Process if peer's socket address is in the list of recipients
	if _, exist := pMsg.Recipients[n.conf.Socket.GetAddress()]; exist {
		packet := transport.Packet{Header: pkt.Header, Msg: pMsg.Msg}
		return n.conf.MessageRegistry.ProcessPacket(packet)
	}
	return nil
}

// ExecDataRequestMessage: the DataRequestMessage handler.
func (n *node) ExecDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type
	drMsg, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", drMsg)
	}
	// Check if duplicated data requests received earlier
	if !n.ntf.AddIfNotExists(drMsg.RequestID) {
		return nil
	}
	// Send response
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(),
		pkt.Header.Source, 0)
	nHop, exist := n.tbl.Check(pkt.Header.RelayedBy)
	if !exist {
		return xerrors.Errorf("Destination address is unknown!")
	}
	replyMsg := types.DataReplyMessage{
		RequestID: drMsg.RequestID,
		Key:       drMsg.Key,
		Value:     n.conf.Storage.GetDataBlobStore().Get(drMsg.Key),
	}
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(replyMsg)
	if err != nil {
		return err
	}
	packet := transport.Packet{
		Header: &header,
		Msg:    &tMsg,
	}
	err = n.conf.Socket.Send(nHop, packet, time.Second)
	if err != nil {
		return err
	}
	// Mark ongoing complete
	// n.ntf.RemoveRequest(drMsg.RequestID)
	return nil
}

// ExecDataReplyMessage: the DataReplyMessage handler.
func (n *node) ExecDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type
	drMsg, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", drMsg)
	}
	n.ntf.DataSignalNotif(drMsg.RequestID, drMsg.Value)
	return nil
}

// ExecSearchRequestMessage: the SearchRequestMessage handler.
func (n *node) ExecSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type
	srMsg, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", srMsg)
	}
	reg, err := regexp.Compile(srMsg.Pattern)
	if err != nil {
		return err
	}
	// Forward search if remaining budget permits
	if srMsg.Budget-1 > 0 {
		_, err := n.SearchNeighbor(*srMsg, srMsg.Budget-1, []string{n.conf.Socket.GetAddress(), pkt.Header.Source}, false)
		if err != nil {
			return err
		}
	}
	// Search locally
	localf := n.SearchLocal(*reg, false)
	// Send search reply back to source
	replyMsg := types.SearchReplyMessage{
		RequestID: srMsg.RequestID,
		Responses: localf,
	}
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(replyMsg)
	if err != nil {
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(),
		srMsg.Origin, 0)
	packet := transport.Packet{
		Header: &header,
		Msg:    &tMsg,
	}
	err = n.conf.Socket.Send(pkt.Header.Source, packet, time.Second)
	if err != nil {
		return err
	}
	return nil
}

// ExecSearchReplyMessage: the SearchReplyMessage handler.
func (n *node) ExecSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type
	srMsg, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", srMsg)
	}
	// Notify a search reply message has been received
	n.ntf.SearchSendNotif(srMsg.RequestID, srMsg.Responses)
	// Update naming store and catalog
	for _, file := range srMsg.Responses {
		err := n.Tag(file.Name, file.Metahash)
		if err != nil {
			return err
		}
		n.UpdateCatalog(file.Metahash, pkt.Header.Source)
		for _, chunk := range file.Chunks {
			if chunk != nil {
				n.UpdateCatalog(string(chunk), pkt.Header.Source)
			}
		}
	}
	return nil
}

func (n *node) ExecPaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
	prepare, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return xerrors.Errorf("message wrong type: %T, we expect PaxosPrepareMessage", msg)
	}
	acceptedValue, acceptedID, ok := n.paxos.ReceivePrepare(*prepare)

	// 1. ignore
	if !ok {
		return nil
	}

	//2. response with paxos promise message
	return n.ResponsePrepare(prepare, acceptedValue, acceptedID)
}

func (n *node) ExecPaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
	propose, ok := msg.(*types.PaxosProposeMessage)
	if !ok {
		return xerrors.Errorf("message wrong type: %T, we expect PaxosProposeMessage", msg)
	}
	ok = n.paxos.ReceivePropose(*propose)

	// 1. ignore
	if !ok {
		return nil
	}

	// 2. response with paxos accept message
	return n.ResponsePropose(propose)
}

func (n *node) ExecPaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
	promise, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return xerrors.Errorf("message wrong type: %T, we expect PaxosPromiseMessage", msg)
	}
	threshold := n.PaxosThreshold(n.TotalPeers)
	n.paxos.ReceivePromise(promise, uint(threshold))

	return nil
}

func (n *node) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	accept, ok := msg.(*types.PaxosAcceptMessage)
	if !ok {
		return xerrors.Errorf("message wrong type: %T, we expect PaxosAcceptMessage", msg)
	}
	threshold := n.PaxosThreshold(n.TotalPeers)
	consensus, step := n.paxos.ReceiveAccept(accept, uint(threshold))

	// 0. step != -1 if we collect enough accept messages
	if step == -1 {
		return nil
	}

	// 1. create a block
	block := n.GetBlock(consensus, uint(step))

	// 2. broadcast TLC message
	err := n.BroadcastTLC(block.Index, block)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) ExecTLCMessage(msg types.Message, pkt transport.Packet) error {
	tlc, ok := msg.(*types.TLCMessage)
	if !ok {
		return xerrors.Errorf("message wrong type: %T, we expect TLCMessage", msg)
	}
	// 1. update receied tlcs
	threshold := n.PaxosThreshold(n.TotalPeers)
	n.paxos.ReceiveTLC(tlc, uint(threshold))

	// 2. recursively update according to tcls
	return n.TLCProcess(n.paxos.GetStep(), false)
}

//func (n *node) ExecPaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
//	a := n.tlc.a
//	a.mu.Lock()
//	// cast the message to its actual type. You assume it is the right type.
//	paxosPrepareMsg, ok := msg.(*types.PaxosPrepareMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T", msg)
//	}
//	if paxosPrepareMsg.Step != a.step || paxosPrepareMsg.ID <= a.maxID {
//		a.mu.Unlock()
//		return nil
//	}
//
//	// PROMISE response
//	a.maxID = paxosPrepareMsg.ID
//	promiseMsg := types.PaxosPromiseMessage{
//		Step:          paxosPrepareMsg.Step,
//		ID:            paxosPrepareMsg.ID,
//		AcceptedID:    a.acceptedID,
//		AcceptedValue: a.acceptedValue,
//	}
//	a.mu.Unlock()
//	trPromiseMsg, err := a.conf.MessageRegistry.MarshalMessage(&promiseMsg)
//	if err != nil {
//		return err
//	}
//	privMsg := types.PrivateMessage{Msg: &trPromiseMsg, Recipients: map[string]struct{}{paxosPrepareMsg.Source: {}}}
//	respMsg, err := a.conf.MessageRegistry.MarshalMessage(privMsg)
//	if err != nil {
//		return err
//	}
//	go func() {
//		err = a.Broadcast(respMsg)
//		if err != nil {
//			log.Error().Msgf("error to broadcast promise message")
//		}
//	}()
//	return nil
//}
//
//// TBD
//func (n *node) ExecPaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
//	p := n.tlc.p
//	// cast the message to its actual type. You assume it is the right type.
//	paxosPromiseMsg, ok := msg.(*types.PaxosPromiseMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T", msg)
//	}
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	if paxosPromiseMsg.Step != p.step || p.phase != 1 {
//		return nil
//	}
//	p.nbResponses++
//	if paxosPromiseMsg.AcceptedID > p.maxAcceptedID {
//		p.maxAcceptedID = paxosPromiseMsg.AcceptedID
//		p.acceptedValue = paxosPromiseMsg.AcceptedValue
//	}
//	return nil
//}
//
//func (n *node) ExecPaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
//	a := n.tlc.a
//	// cast the message to its actual type. You assume it is the right type.
//	paxosProposeMsg, ok := msg.(*types.PaxosProposeMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T", msg)
//	}
//	a.mu.Lock()
//	if paxosProposeMsg.Step != a.step || paxosProposeMsg.ID != a.maxID {
//		a.mu.Unlock()
//		return nil
//	}
//	// ACCEPT response
//	a.acceptedID = paxosProposeMsg.ID
//	a.acceptedValue = &paxosProposeMsg.Value
//	a.mu.Unlock()
//	acceptMsg := types.PaxosAcceptMessage{
//		Step:  paxosProposeMsg.Step,
//		ID:    paxosProposeMsg.ID,
//		Value: paxosProposeMsg.Value,
//	}
//	trAcceptMsg, err := a.conf.MessageRegistry.MarshalMessage(acceptMsg)
//	if err != nil {
//		return err
//	}
//	go func() {
//		err = a.Broadcast(trAcceptMsg)
//		if err != nil {
//			log.Error().Msgf("error to broadcast promise message")
//		}
//	}()
//	return nil
//}
//
//func (n *node) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
//	tlc := n.tlc
//	// cast the message to its actual type. You assume it is the right type.
//	paxosAcceptMsg, ok := msg.(*types.PaxosAcceptMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T", msg)
//	}
//	//if n.conf.Socket.GetAddress() == "127.0.0.1:1" {
//	//	fmt.Println("ExecPaxosAccept:")
//	//}
//	tlc.p.mu.Lock()
//	if paxosAcceptMsg.Step == tlc.p.step && tlc.p.phase == 2 {
//		tlc.p.nbResponses++
//		tlc.p.acceptedValue = &paxosAcceptMsg.Value
//	} else {
//		tlc.p.mu.Unlock()
//		return nil
//	}
//	tlc.mu.Lock()
//	if int(tlc.p.nbResponses) >= tlc.p.conf.PaxosThreshold(tlc.p.conf.TotalPeers) && paxosAcceptMsg.Step == tlc.step {
//		tlc.mu.Unlock()
//		// consensus is reached for the first time!
//		v := &paxosAcceptMsg.Value
//		if n.conf.Socket.GetAddress() == "127.0.0.1:1" {
//			fmt.Printf("Node: 127.0.0.1:1, accepted value: %v\n", v.Filename)
//		}
//		tlc.p.mu.Unlock()
//		block, err := tlc.NewBlock(v)
//		if err != nil {
//			return err
//		}
//		go func() {
//			err = tlc.SendNewTLC(block)
//			if err != nil {
//				log.Error().Msgf(err.Error())
//			}
//		}()
//	} else {
//		tlc.mu.Unlock()
//		tlc.p.mu.Unlock()
//	}
//	return nil
//}
//
//func (n *node) ExecTLCMessage(msg types.Message, pkt transport.Packet) error {
//	tlc := n.tlc
//	tlc.muEx.Lock()
//	defer tlc.muEx.Unlock()
//	// cast the message to its actual type. You assume it is the right type.
//	tlcMsg, ok := msg.(*types.TLCMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T", tlcMsg)
//	}
//	tlc.mu.Lock()
//	if tlcMsg.Step < tlc.step {
//		tlc.mu.Unlock()
//		return nil
//	}
//	// store the message for corresponding step
//	v, exist := tlc.Resp[tlcMsg.Step]
//	if !exist {
//		v.nb = 1
//		v.value = tlcMsg.Block
//	} else {
//		v.nb++
//	}
//	tlc.Resp[tlcMsg.Step] = v
//	// the rest of the work is done on our current step
//	v, existCurr := tlc.Resp[tlc.step]
//	tlc.mu.Unlock()
//	if !existCurr {
//		return nil
//	}
//
//	if int(v.nb) >= tlc.conf.PaxosThreshold(tlc.conf.TotalPeers) {
//		err := n.AtThresholdTLC(false, v.value)
//		if err != nil {
//			return err
//		}
//	} else {
//		return nil
//	}
//
//	for {
//		tlc.mu.Lock()
//		v, exist := tlc.Resp[tlc.step]
//		tlc.mu.Unlock()
//		if !exist {
//			return nil
//		}
//		if int(v.nb) >= tlc.conf.PaxosThreshold(tlc.conf.TotalPeers) {
//			err := n.AtThresholdTLC(true, v.value)
//			if err != nil {
//				return err
//			}
//		} else {
//			return nil
//		}
//	}
//}

//// Paxos Message Handlers
//// ExecPaxosPrepareMessage: the PaxosPrepareMessage handler.
//func (n *node) ExecPaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
//	acc := n.tlc.acc
//	acc.mu.Lock()
//	// Cast the message to its actual type
//	pPreMsg, ok := msg.(*types.PaxosPrepareMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T, should be: *types.PaxosPrepareMessage", pPreMsg)
//	}
//
//	// If message ID is not greater than maxID or message step does not match, simply return
//	if pPreMsg.Step != acc.step || pPreMsg.ID <= acc.maxID {
//		acc.mu.Unlock()
//		return nil
//	}
//	acc.maxID = pPreMsg.ID
//
//	// Respond with a PaxosPromiseMessage matching the prepare message
//	pProMsg := types.PaxosPromiseMessage{
//		Step:          pPreMsg.Step,
//		ID:            pPreMsg.ID,
//		AcceptedID:    acc.acceptedID,
//		AcceptedValue: acc.acceptedValue,
//	}
//	acc.mu.Unlock()
//	tMsg, err := acc.conf.MessageRegistry.MarshalMessage(pProMsg)
//	if err != nil {
//		return err
//	}
//	pMsg := types.PrivateMessage{
//		Recipients: map[string]struct{}{pPreMsg.Source: {}},
//		Msg:        &tMsg,
//	}
//	response, err := acc.conf.MessageRegistry.MarshalMessage(pMsg)
//	if err != nil {
//		return err
//	}
//
//	go func() {
//		err = acc.Broadcast(response)
//		if err != nil {
//			log.Error().Msgf("Error occurred when broadcasting Paxos promise message: %v", err.Error())
//		}
//	}()
//	return nil
//}
//
//// ExecPaxosProposeMessage: the PaxosProposeMessage handler.
//func (n *node) ExecPaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
//	acc := n.tlc.acc
//	// Cast the message to its actual type
//	pPropMsg, ok := msg.(*types.PaxosProposeMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T, should be: *types.PaxosProposeMessage.", pPropMsg)
//	}
//	acc.mu.Lock()
//
//	// If ID or Step don't match, simply return
//	if pPropMsg.ID != acc.maxID || pPropMsg.Step != acc.step {
//		acc.mu.Unlock()
//		return nil
//	}
//
//	// Respond with an accept message and broadcast
//	acc.acceptedValue = &pPropMsg.Value
//	acc.acceptedID = pPropMsg.ID
//	acc.mu.Unlock()
//	pAccMsg := types.PaxosAcceptMessage{
//		Step:  pPropMsg.Step,
//		ID:    pPropMsg.ID,
//		Value: pPropMsg.Value,
//	}
//	tMsg, err := acc.conf.MessageRegistry.MarshalMessage(pAccMsg)
//	if err != nil {
//		return err
//	}
//
//	go func() {
//		err = acc.Broadcast(tMsg)
//		if err != nil {
//			log.Error().Msgf("Error occurred when broadcasting Paxos promise message: %s", err.Error())
//		}
//	}()
//	return nil
//}
//
//// ExecPaxosAcceptMessage: the PaxosAcceptMessage handler.
//func (n *node) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
//	tlc := n.tlc
//	// Cast the message to its actual type
//	pAccMsg, ok := msg.(*types.PaxosAcceptMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T, should be: *types.PaxosAcceptMessage", pAccMsg)
//	}
//	tlc.prp.mu.Lock()
//
//	if pAccMsg.Step == tlc.prp.step {
//		tlc.prp.acceptedValue = &pAccMsg.Value
//		tlc.prp.countResp++
//	}
//	tlc.mu.Lock()
//
//	if pAccMsg.Step == tlc.step && int(tlc.prp.countResp) >= tlc.prp.conf.PaxosThreshold(tlc.prp.conf.TotalPeers) {
//		tlc.mu.Unlock()
//		cVal := &pAccMsg.Value
//		tlc.prp.mu.Unlock()
//		block, err := tlc.BuildBlock(cVal)
//		if err != nil {
//			return err
//		}
//
//		go func() {
//			err := tlc.SendTLCMessage(block)
//			if err != nil {
//				log.Error().Msgf("Error occurred when sending TLC message: %v", err.Error())
//			}
//		}()
//	} else {
//		tlc.mu.Unlock()
//		tlc.prp.mu.Unlock()
//	}
//	return nil
//}
//
//// ExecPaxosPromiseMessage: the PaxosPromiseMessage handler.
//func (n *node) ExecPaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
//	prp := n.tlc.prp
//	// Cast the message to its actual type
//	pPromMsg, ok := msg.(*types.PaxosPromiseMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T, should be: *types.PaxosPromiseMessage", pPromMsg)
//	}
//	prp.mu.Lock()
//	defer prp.mu.Unlock()
//
//	// If not in phase 1 or step not matched, simply return
//	if prp.phase != 1 || pPromMsg.Step != prp.step {
//		return nil
//	}
//	prp.countResp++
//	if pPromMsg.AcceptedID > prp.maxAcceptedID {
//		prp.maxAcceptedID = pPromMsg.AcceptedID
//		prp.acceptedValue = pPromMsg.AcceptedValue
//	}
//	return nil
//}
//
//// ExecTLCMessage: the TLCMessage handler.
//// TBD
//func (n *node) ExecTLCMessage(msg types.Message, pkt transport.Packet) error {
//	tlc := n.tlc
//	tlc.muExec.Lock()
//	defer tlc.muExec.Unlock()
//
//	// Cast the message to its actual type
//	tlcMsg, ok := msg.(*types.TLCMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T, should be: *types.TLCMessage", tlcMsg)
//	}
//	// Ignore past steps
//	tlc.mu.Lock()
//	if tlcMsg.Step < tlc.step {
//		tlc.mu.Unlock()
//		return nil
//	}
//	// Save current message of corresponding step
//	val, exist := tlc.resp[tlcMsg.Step]
//	if exist { // If exists, add up count for advancing the TLC step
//		val.nb++
//	} else { // If not, create a new key-value store
//		val.nb = 1
//		val.value = tlcMsg.Block
//	}
//	tlc.resp[tlcMsg.Step] = val // update message status
//	// Check if current TLC step is valid for advancing
//	cVal, exist := tlc.resp[tlc.step]
//	tlc.mu.Unlock()
//	if !exist {
//		//log.Error().Msgf("Store value not exist for tlc step: %v", tlc.step)
//		return nil
//	}
//	if int(cVal.nb) >= tlc.conf.PaxosThreshold(tlc.conf.TotalPeers) { // If capable of advancing forward
//		err := n.ForwardTLCStep(cVal.value, false)
//		if err != nil {
//			return err
//		}
//	} else {
//		return nil
//	}
//	// When successfully step by 1, check if possible to further catchup
//	for {
//		tlc.mu.Lock()
//		val, exist := tlc.resp[tlc.step]
//		tlc.mu.Unlock()
//		if !exist {
//			return nil
//		}
//		if int(val.nb) >= tlc.conf.PaxosThreshold(tlc.conf.TotalPeers) { // If capable of advancing forward
//			err := n.ForwardTLCStep(val.value, true)
//			if err != nil {
//				return err
//			}
//		} else {
//			return nil
//		}
//	}
//}
