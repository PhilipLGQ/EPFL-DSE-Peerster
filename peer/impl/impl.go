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
	// Initialize the routing table
	node.tbl = ConcurrentRT{RT: make(map[string]string)}
	node.tbl.AddEntry(node.conf.Socket.GetAddress(), node.conf.Socket.GetAddress())
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
}

// ExecChatMessage: the handler. This function will be called when a chat message is received.
func ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	log.Info().Msg(chatMsg.String())
	return nil
}

// Start implements peer.Service
func (n *node) Start() error {
	/* panic("to be implemented in HW0") */
	// Deal with possible error in gouroutine
	chanError := make(chan error, 1)
	// Create new context allowing the goroutine to know Stop() call
	n.ctx, n.cancel = context.WithCancel(context.Background())

	// Mark start of goroutine
	n.wg.Add(1)
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, ExecChatMessage)

	go func(ctx context.Context, cs chan error) {
		defer n.wg.Done()
		for {
			// Return if Stop() is called
			select {
			case <-ctx.Done():
				return
			default:
			}
			// Process when no Stop() is called
			pkt, err := n.conf.Socket.Recv(time.Millisecond * 10)
			if errors.Is(err, transport.TimeoutError(0)) {
				continue
			} else if err != nil {
				cs <- err
			}
			// Check if message should be permitted to register
			n.wg.Add(1)
			go func(cs chan error) {
				defer n.wg.Done()
				err := n.ProcessMessage(pkt)
				if err != nil {
					cs <- err
				}
			}(chanError)
		}
	}(n.ctx, chanError)

	select {
	case cError := <-chanError:
		return cError
	default:
		return nil
	}
}

// Stop implements peer.Service
func (n *node) Stop() error {
	/* panic("to be implemented in HW0") */
	// Inform all goroutine to stop
	n.cancel()
	// Wait for completion of all goroutines
	n.wg.Wait()
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	/*panic("to be implemented in HW0")*/
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	packet := transport.Packet{Header: &header, Msg: &msg}
	nHop, eHop := n.tbl.Check(dest)
	if !eHop {
		return xerrors.Errorf("Destination address unknown!")
	}
	return n.conf.Socket.Send(nHop, packet, time.Millisecond*10)
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	/* panic("to be implemented in HW0") */
	for _, address := range addr {
		if n.conf.Socket.GetAddress() == address { // exclude adding the peer itself
			continue
		}
		n.tbl.AddEntry(address, address)
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	/* panic("to be implemented in HW0") */
	return n.tbl.GetTable()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	/* panic("to be implemented in HW0") */
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
		return n.conf.Socket.Send(nHop, packet, time.Millisecond*10)
	}
	return nil
}

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
func (rt *ConcurrentRT) AddEntry(key, value string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RT[key] = value
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
