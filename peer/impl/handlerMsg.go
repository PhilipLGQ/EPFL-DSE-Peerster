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
	// Set flag for existence of at least 1 expected rumor
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
				return err
			}
			n.rumorP.Record(rumor, rumor.Origin, &n.tbl, pkt.Header.RelayedBy)

			noNewFlag := false
			for !noNewFlag {
				err, noNewFlag = n.ProcessSpecifiedRumors(rumor.Origin)
				if err != nil {
					return err
				}
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
