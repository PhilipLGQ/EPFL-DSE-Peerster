package impl

import (
	"context"
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.

	// Save node configs
	node := node{conf: conf}
	// Initialization
	node.rumorP.Initiator()
	node.ackSig.Initiator()
	node.tbl = ConcurrentRT{RT: make(map[string]string)}
	node.rumorB = make(BufferRumor)
	node.tbl.AddEntry(node.conf.Socket.GetAddress(), node.conf.Socket.GetAddress())
	// Register message handlers of different types of messages
	node.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, node.ExecChatMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, node.ExecRumorsMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, node.ExecAckMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, node.ExecStatusMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, node.ExecPrivateMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, node.ExecEmptyMessage)
	return &node
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// Keep the peer.Configuration on this struct:
	conf peer.Configuration
	// Group of goroutines launched by the node
	wg sync.WaitGroup
	// Node running status indicator
	cancel context.CancelFunc
	ctx    context.Context
	// Concurrent routing table
	tbl ConcurrentRT
	// Rumor message processor
	rumorP ProcessorRumor
	// Synchronization primitive to control send/receive rumor message
	rumorMu sync.Mutex
	// ACK signalization
	ackSig AckSignal
	// ackMu  sync.Mutex
	// Buffer for rumors
	rumorB   BufferRumor
	rumorBMu sync.Mutex
}

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
	var nodeAddr = n.conf.Socket.GetAddress()

	// Process all rumors in the RumorsMessage
	for _, rumor := range rMsg.Rumors {
		// Ignore non-expected rumor
		if n.rumorP.Expected(rumor.Origin, int(rumor.Sequence)) {
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
		} else {
			detail := DetailRumor{
				rumor: rumor,
				pkt:   pkt,
			}
			n.rumorB[rumor.Origin] = append(n.rumorB[rumor.Origin], detail)
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
	header := transport.NewHeader(nodeAddr, nodeAddr, pkt.Header.Source, 0)
	packet := transport.Packet{Header: &header, Msg: &tAck}
	n.rumorMu.Unlock()

	err = n.conf.Socket.Send(pkt.Header.Source, packet, time.Millisecond*100)
	if err != nil {
		return err
	}

	// Send the RumorMessage to another random neighbor (if >= 1 expected rumor data exist)
	if rmExpected {
		rdmNeighbor, exist := n.tbl.RandomNeighbor([]string{nodeAddr, pkt.Header.Source})
		if !exist { // Return if no other neighbor exists
			return nil
		}
		rheader := transport.NewHeader(nodeAddr, nodeAddr, rdmNeighbor, 0)
		rpacket := transport.Packet{Header: &rheader, Msg: pkt.Msg}

		// Ask for ACK
		n.ackSig.Request(rpacket.Header.PacketID)
		return n.conf.Socket.Send(rdmNeighbor, rpacket, time.Millisecond*100)
	}
	return nil
}

// ProcessBufferedRumors: periodically reprocess received non-ordered rumors stored in rumor buffer.
func (n *node) ProcessBufferedRumors() error {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.rumorBMu.Lock()
			for origin, rumorDetails := range n.rumorB {
				i := 0
				for _, detail := range rumorDetails {
					if n.rumorP.Expected(origin, int(detail.rumor.Sequence)) {
						// Process the rumor using the stored packet
						err := n.conf.MessageRegistry.ProcessPacket(detail.pkt)
						if err != nil {
							return err
						} else {
							// fmt.Printf("Processed buffered rumor from %s, Sequence: %d\n", origin, rumor.Sequence)
							n.rumorP.Record(detail.rumor, origin, &n.tbl, detail.pkt.Header.RelayedBy)
							// Continue to next rumor in the buffer if it's processed successfully
							continue
						}
					}
					// Keep the unprocessed rumors in the buffer
					rumorDetails[i] = detail
					i++
				}
				// Keep the unprocessed rumors in the buffer
				n.rumorB[origin] = rumorDetails[:i]
			}
			n.rumorBMu.Unlock()

		case <-n.ctx.Done():
			return nil
		}
	}
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
	// fmt.Println("ackMsg.Status.String() before Marshal:", ackMsg.Status.String())
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(ackMsg.Status)
	if err != nil {
		return err
	}
	// fmt.Println("tMsg.Payload in ExecAck:", string(tMsg.Payload))

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

// Start implements peer.Service
func (n *node) Start() error {
	// Create new context allowing the goroutine to know Stop() call
	n.ctx, n.cancel = context.WithCancel(context.Background())

	// Anti-entropy mechanism
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		err := n.AntiEntropy()
		if err != nil {
			log.Error().Msgf("Error occurred at anti-entropy mechanism: %v", err)
		}
	}()

	// Heartbeat mechanism
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		err := n.Heartbeat()
		if err != nil {
			log.Error().Msgf("Error occurred at heartbeat mechanism: %v", err)
		}
	}()

	// Process buffered rumors
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		err := n.ProcessBufferedRumors()
		if err != nil {
			log.Error().Msgf("Error occurred at processing buffered rumors: %v", err)
		}
	}()

	// Listen and receive
	n.wg.Add(1)
	go func(ctx context.Context) {
		defer n.wg.Done()
		for {
			// Return if Stop() is called
			select {
			case <-ctx.Done():
				return
			default:
				// Process when no Stop() is called
				pkt, err := n.conf.Socket.Recv(time.Millisecond * 100)
				if errors.Is(err, transport.TimeoutError(0)) {
					continue
				} else if err != nil {
					log.Error().Msgf("Error occurred at receiving message: %v", err.Error())
				}
				// Check if message should be permitted to register
				n.wg.Add(1)
				go func() {
					defer n.wg.Done()
					err := n.ProcessMessage(pkt)
					if err != nil {
						log.Error().Msgf("Error occurred at processing received message: %v", err.Error())
					}
				}()
			}
		}
	}(n.ctx)
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	// Inform all goroutine to stop
	n.cancel()
	// Wait for completion of all goroutines
	n.wg.Wait()
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	packet := transport.Packet{Header: &header, Msg: &msg}
	nHop, eHop := n.tbl.Check(dest)
	if !eHop {
		return xerrors.Errorf("Destination address unknown!")
	}
	return n.conf.Socket.Send(nHop, packet, time.Millisecond*100)
}

// Broadcast implements peer.Messaging
func (n *node) Broadcast(msg transport.Message) error {
	// Create a RumorMessage including 1 Rumor
	rumor := types.Rumor{
		Origin:   n.conf.Socket.GetAddress(),
		Sequence: n.rumorP.GetSeqNumber(),
		Msg:      &msg,
	}
	rumorMsg := types.RumorsMessage{
		Rumors: []types.Rumor{rumor}}

	// Transforms rumor message to a transport.Message
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(rumorMsg)
	if err != nil {
		return err
	}

	// Process locally the message
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(), 0)
	packet := transport.Packet{Header: &header, Msg: &msg}
	err = n.conf.MessageRegistry.ProcessPacket(packet)
	if err != nil {
		return err
	}
	n.rumorP.Record(rumor, n.conf.Socket.GetAddress(), &n.tbl, "")

	// Send 1 rumor to 1 random neighbor
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		select {
		case <-n.ctx.Done():
			return
		default:
			{
				errRN := n.BroadcastRandomNeighbor(tMsg)
				if errRN != nil {
					log.Error().Msg("Error when trying to broadcast to a random neighbor!")
				}
			}
		}
	}()
	return err
}

// BroadcastRandomNeighbor implements the functions of broadcasting a rumor to a valid random neighbor
func (n *node) BroadcastRandomNeighbor(tMsg transport.Message) error {
	preNeighbor := ""

	for {
		rdmNeighbor, exist := n.tbl.RandomNeighbor([]string{preNeighbor, n.conf.Socket.GetAddress()})
		if !exist {
			return nil
		}
		if preNeighbor == "" { // If first time broadcasting
			n.rumorP.IncreaseSeqNumber()
		}
		rheader := transport.NewHeader(n.conf.Socket.GetAddress(),
			n.conf.Socket.GetAddress(), rdmNeighbor, 0)
		rpacket := transport.Packet{Header: &rheader, Msg: &tMsg}

		// Ask for ACK
		n.ackSig.Request(rpacket.Header.PacketID)
		err := n.conf.Socket.Send(rdmNeighbor, rpacket, time.Millisecond*100)
		if err != nil {
			return err
		}
		if n.conf.AckTimeout > 0 {
			select {
			case <-n.ctx.Done():
				return nil
			case <-n.ackSig.Wait(rpacket.Header.PacketID): // Return if ACK received
				return nil
			case <-time.After(n.conf.AckTimeout): // If timeout resend to another random neighbor
				preNeighbor = rdmNeighbor
				continue
			}
		} else if n.conf.AckTimeout == 0 { // If AckTimeout is 0, then always wait
			select {
			case <-n.ctx.Done():
				return nil
			case <-n.ackSig.Wait(rpacket.Header.PacketID):
				return nil
			}
		} else { // If AckTimeout is < 0, then error occurs
			// TO DO
			return xerrors.Errorf("AckTimeout interval cannot be less than 0!")
		}
	}
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, address := range addr {
		if n.conf.Socket.GetAddress() == address { // exclude adding the peer itself
			continue
		}
		n.tbl.AddEntry(address, address)
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	return n.tbl.GetTable()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.tbl.RemoveEntry(origin)
	} else {
		n.tbl.AddEntry(origin, relayAddr)
	}
}

// ProcessMessage implements the permission check on received messages
func (n *node) ProcessMessage(pkt transport.Packet) error {
	// If the message is for this node: process
	if pkt.Header.Destination == n.conf.Socket.GetAddress() {
		// Register if no error
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Info().Msgf("Error: %v", err.Error())
			return err
		}
	} else { // Relay the message to the next hop if exists
		header := transport.NewHeader(pkt.Header.Source, n.conf.Socket.GetAddress(), pkt.Header.Destination, 0)
		packet := transport.Packet{Header: &header, Msg: pkt.Msg}
		nHop, eHop := n.tbl.Check(pkt.Header.Destination)
		if !eHop {
			return xerrors.Errorf("Destination address unknown!")
		}
		return n.conf.Socket.Send(nHop, packet, time.Millisecond*100)
	}
	return nil
}

// AntiEntropy implements the anti-entropy mechanism
func (n *node) AntiEntropy() error {
	// Anti-entropy mechanism not activated when interval = 0
	if n.conf.AntiEntropyInterval == 0 {
		return nil
	}

	// Add a ticker with triggering interval = anti-entropy interval
	ticker := time.NewTicker(n.conf.AntiEntropyInterval)
	defer ticker.Stop()

	// Initial sending of peer's view to make up the missing of initial ticking
	err := n.SendNodeView("", []string{})
	if err != nil {
		return err
	}
	for {
		select {
		case <-n.ctx.Done():
			return nil
		case <-ticker.C:
			err := n.SendNodeView("", []string{})
			if err != nil {
				return err
			}
		}
	}
}

// SendNodeView implements the function of sending a node's view to a determined/random-picked neighbor.
func (n *node) SendNodeView(dest string, exception []string) error {
	sendNeighbor := dest
	exist := false
	if sendNeighbor == "" { // If no provided destination, pick a random neighbor excluding the exceptions
		sendNeighbor, exist = n.tbl.RandomNeighbor(append(exception, n.conf.Socket.GetAddress()))
		if !exist { // If no neighbor simply return
			return nil
		}
	}
	sMsg := types.StatusMessage(n.rumorP.GetNodeView())
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(&sMsg)
	if err != nil {
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), sendNeighbor, 0)
	packet := transport.Packet{Header: &header, Msg: &tMsg}
	return n.conf.Socket.Send(sendNeighbor, packet, time.Millisecond*100)
}

// SendMissingRumors implements the functions of sending remote missing rumors to the remote peer
func (n *node) SendMissingRumors(dest string, msg types.StatusMessage, compDiff []string) error {
	var remoteMissingRumors []types.Rumor

	// Fetch remote missing rumors
	for _, addr := range compDiff {
		rumors := n.rumorP.GetAddrRumor(addr)
		remoteMissingRumors = append(remoteMissingRumors, rumors[msg[addr]:]...)
	}
	// Sort them with increasing sequence number
	sort.Slice(remoteMissingRumors, func(i, j int) bool {
		return remoteMissingRumors[i].Sequence < remoteMissingRumors[j].Sequence
	})
	// Send missing rumors to the remote peer
	rumorsMsg := types.RumorsMessage{Rumors: remoteMissingRumors}
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(rumorsMsg)
	if err != nil {
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	packet := transport.Packet{Header: &header, Msg: &tMsg}
	n.ackSig.Request(packet.Header.PacketID)
	return n.conf.Socket.Send(dest, packet, time.Millisecond*100)
}

// IdenticalView implements the function of checking if 2 views are identical
func (n *node) IdenticalView(v1, v2 types.StatusMessage) bool {
	if len(v1) != len(v2) { // v1 and v2 are not of the same size
		return false
	}
	for addr, v1seq := range v1 {
		if v2seq, exist := v2[addr]; !exist || v1seq != v2seq { // if addr in v1 doesn't exist in v2 or seq not match
			return false
		}
	}
	return true
}

// CheckViewDiff implements the view difference check and return the difference
func (n *node) CheckViewDiff(local, remote types.StatusMessage) (int, []string) {
	localMissing := false
	remoteMissing := false
	var remoteDiff []string

	// Check if local node is missing rumors
	for rAddr, rSeq := range remote {
		if lSeq, exist := local[rAddr]; !exist || lSeq < rSeq {
			localMissing = true
			break
		}
	}
	// Check if remote node is missing rumors
	for lAddr, lSeq := range local {
		if rSeq, exist := remote[lAddr]; !exist || rSeq < lSeq {
			remoteMissing = true
			remoteDiff = append(remoteDiff, lAddr)
		}
	}
	if localMissing && remoteMissing {
		return 3, remoteDiff
	} else if localMissing {
		return 1, nil
	} else if remoteMissing {
		return 2, remoteDiff
	} else {
		return 4, nil
	}
}

// Heartbeat implements the heartbeat mechanism
func (n *node) Heartbeat() error {
	// Heartbeat mechanism not activated when interval = 0
	if n.conf.HeartbeatInterval == 0 {
		return nil
	}
	tMsg, err := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
	if err != nil {
		return err
	}

	// Add a ticker with triggering interval = heartbeat interval
	ticker := time.NewTicker(n.conf.HeartbeatInterval)
	defer ticker.Stop()

	// Initial heartbeat broadcast to make up the missing of initial ticking
	err = n.Broadcast(tMsg)
	if err != nil {
		return err
	}
	for {
		select {
		case <-n.ctx.Done():
			return nil
		case <-ticker.C:
			err = n.Broadcast(tMsg)
			if err != nil {
				return err
			}
		}
	}
}

// Concurrent Routing Table
// ConcurrentRT implements the concurrent routing table
type ConcurrentRT struct {
	RT peer.RoutingTable
	mu sync.Mutex
}

// Check if the input address value has a next hop, return the value and existence
func (rt *ConcurrentRT) Check(addr string) (string, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	nAddr, exist := rt.RT[addr]
	return nAddr, exist
}

// AddEntry adds a routing entry (peer) to the routing table
func (rt *ConcurrentRT) AddEntry(addr, via string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RT[addr] = via
}

// RemoveEntry removes a routing entry (peer) from the routing table
func (rt *ConcurrentRT) RemoveEntry(key string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	delete(rt.RT, key)
}

// GetTable returns a current copy of the routing table
func (rt *ConcurrentRT) GetTable() peer.RoutingTable {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	tblCopy := make(map[string]string)
	for k, v := range rt.RT {
		tblCopy[k] = v
	}
	return tblCopy
}

// RandomNeighbor returns a randomly selected neighbor excluding the ones to remove
func (rt *ConcurrentRT) RandomNeighbor(exception []string) (string, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Use the mapping of empty struct to save space
	exceptMap := make(map[string]struct{}, len(exception))
	var neighborValid []string

	for _, exp := range exception {
		exceptMap[exp] = struct{}{}
	}
	for addr, via := range rt.RT {
		if addr == via {
			if _, found := exceptMap[addr]; !found {
				neighborValid = append(neighborValid, addr)
			}
		}
	}
	// Return if no valid neighbor left
	if len(neighborValid) == 0 {
		return "", false
	}
	// Otherwise randomly return a valid neighbor
	rdmNeighbor := neighborValid[rand.Intn(len(neighborValid))]
	return rdmNeighbor, true
}

// String implements safe access to peer.RoutingTable.String()
func (rt *ConcurrentRT) String() string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.RT.String()
}

// DisplayGraph implements safe access to display the routing table in graphviz graph
func (rt *ConcurrentRT) DisplayGraph(out io.Writer) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RT.DisplayGraph(out)
}

type BufferRumor map[string][]DetailRumor

type DetailRumor struct {
	rumor types.Rumor
	pkt   transport.Packet
}

// Processor of rumors
// ProcessRumor implements the functions
type ProcessorRumor struct {
	mu          sync.RWMutex
	seq         int
	rumorRecord map[string][]types.Rumor
	rumorSeq    map[string]int
}

// Initiator initializes the rumorRecord map and the sequence number to 1
func (pr *ProcessorRumor) Initiator() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.seq = 1
	pr.rumorRecord = make(map[string][]types.Rumor)
	pr.rumorSeq = make(map[string]int)
}

// Expected checks if current rumor's sequence corresponds to the last rumor's sequence + 1
func (pr *ProcessorRumor) Expected(addr string, sequence int) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return sequence == pr.rumorSeq[addr]+1
	//return sequence == len(pr.rumorRecord[addr])+1
}

// IncreaseSeqNumber increments the sequence number by 1
func (pr *ProcessorRumor) IncreaseSeqNumber() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.seq++
}

// GetSeqNumber returns the current sequence number
func (pr *ProcessorRumor) GetSeqNumber() uint {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return uint(pr.seq)
}

// GetNodeView returns current peer's view recording rumors previously processed
func (pr *ProcessorRumor) GetNodeView() map[string]uint {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	view := make(map[string]uint)
	for addr, seq := range pr.rumorRecord {
		view[addr] = uint(len(seq))
	}
	return view
}

// GetAddrRumor returns a list of rumors from a source address that previously processed by this peer
func (pr *ProcessorRumor) GetAddrRumor(addr string) []types.Rumor {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.rumorRecord[addr]
}

// Record logs one rumor into rumorRecord to record the process history of the given rumor
func (pr *ProcessorRumor) Record(rm types.Rumor, addr string, rt *ConcurrentRT, relay string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	// Update routing table if necessary
	if relay != "" {
		rt.AddEntry(addr, relay)
	}
	pr.rumorRecord[addr] = append(pr.rumorRecord[addr], rm)
	pr.rumorSeq[addr] = int(rm.Sequence)
}

// Acknowledge Signalization
// AckSignal implements the ACK signalization mechanism to complete the handler of AckMessage
type AckSignal struct {
	muAck  sync.Mutex
	sigAck map[string]chan struct{}
}

// Initiator initializes the sigAck map of recording the ACK status corresponding to PacketIDs
func (ac *AckSignal) Initiator() {
	ac.muAck.Lock()
	defer ac.muAck.Unlock()
	ac.sigAck = make(map[string]chan struct{})
}

// Request creates an empty struct channel for pktID to signalize a request for ACK
func (ac *AckSignal) Request(pktID string) {
	ac.muAck.Lock()
	defer ac.muAck.Unlock()
	ac.sigAck[pktID] = make(chan struct{})
}

// Signal closes pktID's empty struct channel to signalize a reception of ACK
func (ac *AckSignal) Signal(pktID string) {
	ac.muAck.Lock()
	defer ac.muAck.Unlock()

	// Check if pktID exists in the map
	ch, exists := ac.sigAck[pktID]
	if exists {
		close(ch)
	}
}

// Wait returns the corresponding closed channel after receiving ACK
func (ac *AckSignal) Wait(pktID string) chan struct{} {
	ac.muAck.Lock()
	defer ac.muAck.Unlock()
	return ac.sigAck[pktID]
}
