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

// GetListNeighbor returns all neighbors of the peer with random shuffling
func (rt *ConcurrentRT) GetListNeighbor(exception []string) []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var nblist []string
	for _, nb := range rt.RT {
		exclude := false
		for _, except := range exception {
			if except == nb {
				exclude = true
			}
		}
		if !exclude {
			nblist = append(nblist, nb)
		}
	}
	rand.Shuffle(len(nblist), func(i, j int) { nblist[i], nblist[j] = nblist[j], nblist[i] })
	return removeDupValues(nblist)
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

// Notification Manager
// Notif implements the asynchronous notification manager
type Notif struct {
	mu       sync.Mutex
	drNotify map[string]chan []byte
	drTrace  map[string]struct{}
	srNotify map[string]chan []types.FileInfo
}

// Initiator initializes the maps managing the notifications of DataRequest/ReplyMessage and SearchRequest/ReplyMessage
// plus a map managing the current DataRequestMessage of a certain RequestID being processed
func (nt *Notif) Initiator() {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	nt.drTrace = make(map[string]struct{})
	nt.drNotify = make(map[string]chan []byte)
	nt.srNotify = make(map[string]chan []types.FileInfo)
}

// DataRequestNotif triggers the notification channel for a specific data request referenced by rID
func (nt *Notif) DataRequestNotif(rID string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	nt.drNotify[rID] = make(chan []byte, 1)
}

// DataSignalNotif notifies by sending byte-formed data to the channel associated with rID.
func (nt *Notif) DataSignalNotif(rID string, val []byte) {
	nt.mu.Lock()
	channel := nt.drNotify[rID]
	nt.mu.Unlock()
	channel <- val
}

// DataWaitNotif waits for a notification on the channel associated with rID.
func (nt *Notif) DataWaitNotif(rID string) chan []byte {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	return nt.drNotify[rID]
}

// SearchRequestNotif triggers a notification channel for a new search request.
func (nt *Notif) SearchRequestNotif(rID string, budget uint) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	nt.srNotify[rID] = make(chan []types.FileInfo, budget)
}

// SearchSignalNotif closes the notification channel adn signals the end of the search process.
func (nt *Notif) SearchSignalNotif(rID string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	close(nt.srNotify[rID])
}

// SearchWaitNotif provides access to the notification channel for waiting for search results.
func (nt *Notif) SearchWaitNotif(rID string) chan []types.FileInfo {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	return nt.srNotify[rID]
}

// SearchSendNotif sends search responses to the waiting listeners.
func (nt *Notif) SearchSendNotif(rID string, response []types.FileInfo) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	channel := nt.srNotify[rID]
	channel <- response
}

// AddIfNotExists atomically checks if a requestID exists, adds it if it doesn't
// and returns a boolean indicating whether the requestID was added.
func (nt *Notif) AddIfNotExists(requestID string) bool {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	if _, exist := nt.drTrace[requestID]; exist {
		return false // return false if requestID already exists
	}
	nt.drTrace[requestID] = struct{}{}
	return true // Return true if requestID was added
}

// DataRemoveRequest removes the entry referenced by requestID in the data request duplication managing map
func (nt *Notif) DataRemoveRequest(requestID string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	delete(nt.drTrace, requestID)
}

// Concurrent Catalog
// ConcurrentCatalog implements the concurrent catalog
type ConcurrentCatalog struct {
	catalog peer.Catalog
	mu      sync.Mutex
}

// Initiator initializes the catalog map of type peer.Catalog
func (cl *ConcurrentCatalog) Initiator() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.catalog = make(peer.Catalog)
}

// GetCatalog returns a copy of the catalog mapping
func (cl *ConcurrentCatalog) GetCatalog() peer.Catalog {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	clCopy := make(peer.Catalog)
	for k1, v1 := range cl.catalog {
		temp := make(map[string]struct{})
		for k2, v2 := range v1 {
			temp[k2] = v2
		}
		clCopy[k1] = temp
	}
	return clCopy
}

// UpdateCatalog updates the catalog mapping by recording a chunk piece referenced by key is available on peer
func (cl *ConcurrentCatalog) UpdateCatalog(key string, peer string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	_, exist := cl.catalog[key]
	if !exist {
		cl.catalog[key] = make(map[string]struct{})
	}
	cl.catalog[key][peer] = struct{}{}
}

// SelectRandomPeer returns a random peer containing the desired chunk stored by key, or empty string if non-exist
func (cl *ConcurrentCatalog) SelectRandomPeer(key string) string {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.catalog[key] == nil {
		return ""
	}
	peers := make([]string, 0, len(cl.catalog))
	for p := range cl.catalog[key] {
		peers = append(peers, p)
	}
	return peers[rand.Intn(len(peers))]
}
