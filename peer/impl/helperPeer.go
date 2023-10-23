package impl

import (
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"io"
	"math/rand"
	"sync"
)

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

// Sequential processing buffer for rumor messages
// BufferRumor implements the rumor buffer
type BufferRumor struct {
	mu  sync.RWMutex
	buf map[string][]DetailRumor
}

// DetailRumor implements the data structure of BufferRumor.buf
type DetailRumor struct {
	rumor types.Rumor
	pkt   transport.Packet
}

// Initiator initializes the buffer map
func (rb *BufferRumor) Initiator() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.buf = make(map[string][]DetailRumor)
}

// AddRumor adds a DetailRumor to the buffer list corresponding to the rumor's origin
func (rb *BufferRumor) AddRumor(addr string, detail DetailRumor) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.buf[addr] = append(rb.buf[addr], detail)
}

// Processor of rumor messages
// ProcessRumor implements the functions
type ProcessorRumor struct {
	mu          sync.RWMutex
	seq         uint
	rumorRecord map[string][]types.Rumor
	rumorSeq    map[string]uint
}

// Initiator initializes the rumorRecord map and the sequence number to 1
func (pr *ProcessorRumor) Initiator() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.seq = 1
	pr.rumorRecord = make(map[string][]types.Rumor)
	pr.rumorSeq = make(map[string]uint)
}

// InitAddrSeq initializes the sequence number corresponding to the given address
func (pr *ProcessorRumor) InitAddrSeq(addr string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.rumorSeq[addr] = 0
}

// Expected checks if current rumor's sequence corresponds to the last rumor's sequence + 1
func (pr *ProcessorRumor) Expected(addr string, sequence uint) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return sequence == pr.rumorSeq[addr]+1
}

// CheckSeqNew checks if the given rumor's sequence number is greater than last rumor's sequence + 1
// (if yes save rumor to buffer for sequential processing)
func (pr *ProcessorRumor) CheckSeqNew(addr string, sequence uint) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return sequence > pr.rumorSeq[addr]+1
}

// CheckAddrSeq checks if current rumor's origin address is initialized in the rumorSeq map
func (pr *ProcessorRumor) CheckAddrSeq(addr string) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	_, exist := pr.rumorSeq[addr]
	return exist
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
	return pr.seq
}

// GetNodeView returns current peer's view recording rumors previously processed
func (pr *ProcessorRumor) GetNodeView() map[string]uint {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	view := make(map[string]uint)
	// Iterating over each element in pr.rumorSeq and copying it to the new map
	for addr, seq := range pr.rumorSeq {
		view[addr] = seq
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
	pr.rumorSeq[addr] = rm.Sequence
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
