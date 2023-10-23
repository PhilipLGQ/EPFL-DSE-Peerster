package impl

import (
	"context"
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
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
	node.rumorB.Initiator()
	node.tbl = ConcurrentRT{RT: make(map[string]string)}
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
	// Buffer for rumors
	rumorB BufferRumor
}

// ProcessBufferedRumors: periodically reprocess received non-ordered rumors stored in rumor buffer.
func (n *node) ProcessBufferedRumors() error {
	// n.rumorB.mu.Lock()
	// defer n.rumorB.mu.Unlock()
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := n.ProcessRumors()
			if err != nil {
				return err
			}
		case <-n.ctx.Done():
			return nil
		}
	}
}

//func (n *node) ProcessRumors() error {
//	n.rumorB.mu.Lock()
//	defer n.rumorB.mu.Unlock()
//	for origin, rumorDetails := range n.rumorB.buf {
//		i := 0
//		for _, detail := range rumorDetails {
//			if n.rumorP.Expected(origin, detail.rumor.Sequence) {
//				// Process the rumor using the stored packet
//				pkt := transport.Packet{
//					Header: detail.pkt.Header,
//					Msg:    detail.rumor.Msg,
//				}
//				err := n.conf.MessageRegistry.ProcessPacket(pkt)
//				if err != nil {
//					return err
//				}
//				n.rumorP.Record(detail.rumor, origin, &n.tbl, detail.pkt.Header.RelayedBy)
//				// Continue to next rumor in the buffer if it's processed successfully
//				continue
//			} else if n.rumorP.CheckSeqNew(origin, detail.rumor.Sequence) {
//				rumorDetails[i] = detail
//				i++
//			}
//		}
//		// Keep the unprocessed rumors in the buffer
//		n.rumorB.buf[origin] = rumorDetails[:i]
//	}
//	return nil
//}

func (n *node) ProcessRumors() error {
	n.rumorB.mu.Lock()
	defer n.rumorB.mu.Unlock()

	for origin, rumorDetails := range n.rumorB.buf {
		if err := n.processSingleRumor(origin, rumorDetails); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) processSingleRumor(origin string, rumorDetails []DetailRumor) error {
	i := 0
	for _, detail := range rumorDetails {
		isProcessed, err := n.processDetail(origin, detail)
		if err != nil {
			return err
		}
		if !isProcessed {
			rumorDetails[i] = detail
			i++
		}
	}
	n.rumorB.buf[origin] = rumorDetails[:i]
	return nil
}

func (n *node) processDetail(origin string, detail DetailRumor) (bool, error) {
	if n.rumorP.Expected(origin, detail.rumor.Sequence) {
		pkt := transport.Packet{
			Header: detail.pkt.Header,
			Msg:    detail.rumor.Msg,
		}
		if err := n.conf.MessageRegistry.ProcessPacket(pkt); err != nil {
			return false, err
		}
		n.rumorP.Record(detail.rumor, origin, &n.tbl, detail.pkt.Header.RelayedBy)
		return true, nil
	}
	return false, nil
}

// Start implements peer.Service
func (n *node) Start() error {
	// Create new context allowing the goroutine to know Stop() call
	n.ctx, n.cancel = context.WithCancel(context.Background())

	n.startGoroutine("anti-entropy mechanism", n.AntiEntropy)
	n.startGoroutine("heartbeat mechanism", n.Heartbeat)
	n.startGoroutine("processing buffered rumors", n.ProcessBufferedRumors)
	n.startGoroutine("listening and receiving", n.listenAndReceive)

	return nil
}

func (n *node) startGoroutine(description string, fn func() error) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		err := fn()
		if err != nil {
			log.Error().Msgf("Error occurred at %s: %v", description, err)
		}
	}()
}

// Stop implements peer.Service
func (n *node) Stop() error {
	// Inform all goroutine to stop
	n.cancel()
	// Wait for completion of all goroutines
	n.wg.Wait()
	return nil
}

func (n *node) listenAndReceive() error {
	// Your existing logic for listening and receiving goes here
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
				pkt, err := n.conf.Socket.Recv(time.Second)
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

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	packet := transport.Packet{Header: &header, Msg: &msg}
	nHop, eHop := n.tbl.Check(dest)
	if !eHop {
		return xerrors.Errorf("Destination address unknown!")
	}
	return n.conf.Socket.Send(nHop, packet, time.Second)
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
	n.rumorP.IncreaseSeqNumber()
	// fmt.Println("IncreaseSeqNumber executed!")

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
			errRN := n.BroadcastRandomNeighbor(tMsg)
			if errRN != nil {
				log.Error().Msg("Error when trying to broadcast to a random neighbor!")
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
			//fmt.Println("!Exist rdmNeighbor!")
			return nil
		}
		//if preNeighbor == "" { // If first time broadcasting
		//	n.rumorP.IncreaseSeqNumber()
		//}
		rheader := transport.NewHeader(n.conf.Socket.GetAddress(),
			n.conf.Socket.GetAddress(), rdmNeighbor, 0)
		rpacket := transport.Packet{Header: &rheader, Msg: &tMsg}

		// Ask for ACK
		n.ackSig.Request(rpacket.Header.PacketID)
		err := n.conf.Socket.Send(rdmNeighbor, rpacket, time.Second)
		//fmt.Println("Send once!")
		if err != nil {
			return err
		}
		if n.conf.AckTimeout > 0 {
			//fmt.Println("AckTimeout > 0!")
			select {
			case <-n.ctx.Done():
				//fmt.Println("Done!")
				return nil
			case <-n.ackSig.Wait(rpacket.Header.PacketID): // Return if ACK received
				//fmt.Println("Received!")
				return nil
			case <-time.After(n.conf.AckTimeout): // If timeout resend to another random neighbor
				//fmt.Println("Waited!")
				preNeighbor = rdmNeighbor
				continue
			}
		} else if n.conf.AckTimeout == 0 { // If AckTimeout is 0, then always wait
			//fmt.Println("AckTimeout = 0!")
			select {
			case <-n.ctx.Done():
				return nil
			case <-n.ackSig.Wait(rpacket.Header.PacketID):
				return nil
			}
		} else { // If AckTimeout is < 0, then error occurs
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
	// Process if the message is for this node
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
		return n.conf.Socket.Send(nHop, packet, time.Second)
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
	return n.conf.Socket.Send(sendNeighbor, packet, time.Second)
}

// SendMissingRumors implements the functions of sending remote missing rumors to the remote peer
func (n *node) SendMissingRumors(dest string, msg types.StatusMessage, compDiff map[string]uint) error {
	var remoteMissingRumors []types.Rumor

	// Fetch remote missing rumors
	for addr, seq := range compDiff {
		rumors := n.rumorP.GetAddrRumor(addr)
		// Iterate over each rumor and check if it is missing in the remote peer
		for _, rumor := range rumors {
			if rumor.Sequence > seq {
				remoteMissingRumors = append(remoteMissingRumors, rumor)
			}
		}
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
	return n.conf.Socket.Send(dest, packet, time.Second)
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
func (n *node) CheckViewDiff(local, remote types.StatusMessage) (int, map[string]uint) {
	localMissing := false
	remoteMissing := false
	remoteDiff := make(map[string]uint)

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
			if exist {
				remoteDiff[lAddr] = rSeq
			} else {
				remoteDiff[lAddr] = 0
			}
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
