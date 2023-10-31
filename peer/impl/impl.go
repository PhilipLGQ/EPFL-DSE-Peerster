package impl

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"regexp"
	"strings"
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
	node.catalog.Initiator()
	node.ntf.Initiator()
	node.tbl = ConcurrentRT{RT: make(map[string]string)}
	node.tbl.AddEntry(node.conf.Socket.GetAddress(), node.conf.Socket.GetAddress())
	// Create new context allowing the goroutine to know Stop() call
	node.ctx, node.cancel = context.WithCancel(context.Background())
	// Register message handlers of different types of messages
	node.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, node.ExecChatMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, node.ExecRumorsMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, node.ExecAckMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, node.ExecStatusMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, node.ExecPrivateMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, node.ExecEmptyMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, node.ExecDataRequestMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, node.ExecDataReplyMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, node.ExecSearchRequestMessage)
	node.conf.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, node.ExecSearchReplyMessage)
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
	// Concurrent catalog
	catalog ConcurrentCatalog
	// Notification manager
	ntf Notif
}

// Start implements peer.Service
func (n *node) Start() error {
	n.startGoroutine("anti-entropy mechanism", n.AntiEntropy)
	n.startGoroutine("heartbeat mechanism", n.Heartbeat)
	n.startGoroutine("processing buffered rumors", n.ProcessBufferedRumors)
	n.startGoroutine("listening and receiving", n.listenAndReceive)
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

// Upload implements peer.DataSharing
func (n *node) Upload(data io.Reader) (metahash string, err error) {
	dataContent, err := io.ReadAll(data)
	if err != nil {
		return "", err
	}

	dataLength := uint(len(dataContent))
	currLength := uint(0)
	dataPtr := uint(0)

	dataStorage := n.conf.Storage.GetDataBlobStore()
	// metaKeyBuffer := bytes.Buffer{}
	metaValueBuffer := bytes.Buffer{}

	for dataLength > 0 {
		if dataLength < n.conf.ChunkSize {
			currLength = dataLength
			dataLength = 0
		} else {
			dataLength -= n.conf.ChunkSize
			currLength = n.conf.ChunkSize
		}

		chunk := make([]byte, currLength)
		copy(chunk, dataContent[dataPtr:dataPtr+currLength])
		dataPtr += currLength

		chunkSHA := SHA256(chunk)
		//_, err = metaKeyBuffer.Write(chunkSHA)
		//if err != nil {
		//	return "", err
		//}
		chunkSHAstr := hex.EncodeToString(chunkSHA)
		_, err = metaValueBuffer.WriteString(chunkSHAstr)
		if err != nil {
			return "", err
		}
		if dataLength > 0 {
			_, err := metaValueBuffer.WriteString(peer.MetafileSep)
			if err != nil {
				return "", err
			}
		}
		dataStorage.Set(chunkSHAstr, chunk)
	}
	metahash = hex.EncodeToString(SHA256(metaValueBuffer.Bytes()))
	dataStorage.Set(metahash, metaValueBuffer.Bytes())
	return metahash, nil
}

// Download implements peer.DataSharing
func (n *node) Download(metahash string) ([]byte, error) {
	var chunksDownload []byte
	metafile, err := n.DownloadSequential(metahash)
	if err != nil {
		return nil, err
	}
	chunks := strings.Split(string(metafile), peer.MetafileSep)
	for _, chunk := range chunks {
		chunkVal, err := n.DownloadSequential(chunk)
		if err != nil {
			return nil, err
		}
		chunksDownload = append(chunksDownload, chunkVal...)
	}
	return chunksDownload, nil
}

// Tag implements peer.DataSharing
func (n *node) Tag(name string, mh string) error {
	ns := n.conf.Storage.GetNamingStore()
	ns.Set(name, []byte(mh))
	return nil
}

// Resolve implements peer.DataSharing
func (n *node) Resolve(name string) (metahash string) {
	ns := n.conf.Storage.GetNamingStore()
	metahash = string(ns.Get(name))
	return metahash
}

// GetCatalog implements peer.DataSharing
func (n *node) GetCatalog() peer.Catalog {
	return n.catalog.GetCatalog()
}

// UpdateCatalog implements peer.DataSharing
func (n *node) UpdateCatalog(key string, peer string) {
	n.catalog.UpdateCatalog(key, peer)
}

// SearchAll implements peer.DataSharing
func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {
	names = []string{}
	// If budget is given as equal to 0, we raise budget error
	if budget < 1 {
		return nil, xerrors.Errorf("Initial budget config should be > 0!")
	}
	// Search in local naming store
	localMatch := n.SearchLocal(reg, true)
	for _, file := range localMatch {
		names = append(names, file.Name)
	}
	// Send request to neighbors base on budget
	srMsg := types.SearchRequestMessage{
		Origin:  n.conf.Socket.GetAddress(),
		Pattern: reg.String(),
	}
	//fmt.Println("Executed neighbor search!")
	rIDs, err := n.SearchNeighbor(srMsg, budget, []string{n.conf.Socket.GetAddress()}, true)
	if err != nil {
		return nil, err
	}
	// Wait and gather
	select {
	case <-n.ctx.Done():
		return nil, nil
	case <-time.After(timeout):
		//fmt.Println("Timeout once!")
	}
	for _, rID := range rIDs {
		closedChan := false
		for !closedChan {
			select {
			case val := <-n.ntf.SearchWaitNotif(rID):
				for _, f := range val {
					names = append(names, f.Name)
				}
			default:
				n.ntf.SearchSignalNotif(rID)
				closedChan = true
			}
		}
	}
	names = removeDupValues(names)
	return names, nil
}

// SearchFirst implements peer.DataSharing
func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {
	// Search in local naming store
	localMatch := n.SearchLocal(pattern, false)
	localFull := n.CheckFirstFullMatch(localMatch)
	if localFull != "" {
		return localFull, nil
	}
	// If no local full match found, forward to neighbors and regulate by expand-ring
	if conf.Initial < 1 {
		return "", xerrors.Errorf("Initial budget config should be > 0!")
	}
	countRetry := uint(0)
	budget := conf.Initial
	for countRetry < conf.Retry {
		// Create search request message
		srMsg := types.SearchRequestMessage{Origin: n.conf.Socket.GetAddress(), Pattern: pattern.String()}
		rIDs, err := n.SearchNeighbor(srMsg, budget, []string{n.conf.Socket.GetAddress()}, true)
		if err != nil {
			return "", err
		}

		// Concurrently and actively search for full match before timeout
		fullMatchFound := make(chan string, 1)
		ctx, cancel := context.WithTimeout(context.Background(), conf.Timeout)
		n.wg.Add(1)
		go n.checkActiveFullMatch(cancel, rIDs, fullMatchFound)

		// Wait and gather
		select {
		case <-n.ctx.Done():
			return "", nil
		case <-ctx.Done(): // If no active first full match found, we check thoroughly to ensure there's no missed full match
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				// Check if neighbor search finally gets a full match file after timeout
				neighborFull := n.CheckFinalFullMatch(rIDs)
				if neighborFull != "" {
					return neighborFull, nil
				}
				break // Retry if eventually no full match found
			}
		case neighborFull := <-fullMatchFound: // If we actively find out a full match, return it
			return neighborFull, nil
		}
		// Retry mechanism
		budget *= conf.Factor
		countRetry++
	}
	return "", nil
}
