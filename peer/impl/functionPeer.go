package impl

import (
	"context"
	"errors"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"time"
)

// startGoroutine implements the function of starting a goroutine to execute function concurrently
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

// ProcessBufferedRumors implements the function of periodically reprocess received non-ordered rumors
// stored in rumor buffer
func (n *node) ProcessBufferedRumors() error {
	ticker := time.NewTicker(time.Millisecond * 20)
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

// ProcessNewRumors implements the function of processing a possibly new-coming rumor
func (n *node) ProcessNewRumors(origin string) (err error) {
	noNewFlag := false
	for !noNewFlag {
		noNewFlag, err = n.ProcessSpecifiedRumors(origin)
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessSpecifiedRumors implements the function of processing a specific rumor from an origin
func (n *node) ProcessSpecifiedRumors(origin string) (noNewFlag bool, err error) {
	n.rumorB.mu.Lock()
	defer n.rumorB.mu.Unlock()

	noNewFlag, err = n.processSingleRumor(origin, n.rumorB.buf[origin])
	if err != nil {
		return false, err
	}
	return noNewFlag, nil
}

// ProcessRumors implements the function of processing initially not processed buffers from the rumor buffer
func (n *node) ProcessRumors() error {
	n.rumorB.mu.Lock()
	defer n.rumorB.mu.Unlock()
	for origin, rumorDetails := range n.rumorB.buf {
		if _, err := n.processSingleRumor(origin, rumorDetails); err != nil {
			return err
		}
	}
	return nil
}

// processSingleRumor implements the function of trying to process each rumor, update the buffer with
// those not processed
func (n *node) processSingleRumor(origin string, rumorDetails []DetailRumor) (bool, error) {
	i := 0
	for _, detail := range rumorDetails {
		isProcessed, err := n.processDetail(origin, detail)
		if err != nil {
			return false, err
		}
		if !isProcessed { // If not processed, later update to buffer
			rumorDetails[i] = detail
			i++
		}
	}
	n.rumorB.buf[origin] = rumorDetails[:i]
	return true, nil
}

// processDetail implements the function of checking if the current rumor is expected, process locally
// and record it if expected
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

// listenAndReceive implements the function of concurrently receiving and processing packets
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

// BroadcastRandomNeighbor implements the functions of broadcasting a rumor to a valid random neighbor
func (n *node) BroadcastRandomNeighbor(tMsg transport.Message) error {
	preNeighbor := ""
	for {
		rdmNeighbor, exist := n.tbl.RandomNeighbor([]string{preNeighbor, n.conf.Socket.GetAddress()})
		if !exist {
			return nil
		}
		rheader := transport.NewHeader(n.conf.Socket.GetAddress(),
			n.conf.Socket.GetAddress(), rdmNeighbor, 0)
		rpacket := transport.Packet{Header: &rheader, Msg: &tMsg}

		// Ask for ACK
		n.ackSig.Request(rpacket.Header.PacketID)
		err := n.conf.Socket.Send(rdmNeighbor, rpacket, time.Second)
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
				n.ackSig.Signal(rpacket.Header.PacketID)
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
			return xerrors.Errorf("AckTimeout interval cannot be less than 0!")
		}
	}
}

// SendNodeView implements the function of sending a node's view to a determined/random-picked neighbor
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

// DownloadSequential implements the function of sequentially retrieve chunks referenced by hash
func (n *node) DownloadSequential(hash string) ([]byte, error) {
	// If we have the corresponding element locally, get and return
	localVal := n.conf.Storage.GetDataBlobStore().Get(hash)
	if localVal != nil {
		return localVal, nil
	}
	// Else we select a random peer from the catalog
	rdmPeer := n.catalog.SelectRandomPeer(hash)
	if rdmPeer == "" {
		return nil, xerrors.Errorf("No neighboring peer found for %v", hash)
	}
	// Send 1 DataRequestMessage, if reachable using the routing table
	nHop, exist := n.tbl.Check(rdmPeer)
	if !exist {
		return nil, xerrors.Errorf("%s as destination address is unknown", nHop)
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), rdmPeer, 0)
	backDuration := n.conf.BackoffDataRequest.Initial
	countRetry := uint(0)
	for {
		// Set up DataRequestMessage and packet
		drMsg := types.DataRequestMessage{
			RequestID: xid.New().String(),
			Key:       hash,
		}
		tMsg, err := n.conf.MessageRegistry.MarshalMessage(drMsg)
		if err != nil {
			return nil, err
		}
		packet := transport.Packet{
			Header: &header,
			Msg:    &tMsg,
		}
		// Set up notification according to RequestID
		n.ntf.DataRequestNotif(drMsg.RequestID)
		// Send packet
		err = n.conf.Socket.Send(nHop, packet, time.Second)
		if err != nil {
			return nil, err
		}
		// Wait for corresponding DataReplyMessage
		select {
		case <-n.ctx.Done():
			return nil, nil
		case val := <-n.ntf.DataWaitNotif(drMsg.RequestID):
			if val == nil {
				return nil, xerrors.Errorf("Error occurred in catalog address!")
			}
			n.conf.Storage.GetDataBlobStore().Set(hash, val)
			return val, nil
		case <-time.After(backDuration):
		}
		backDuration *= time.Duration(n.conf.BackoffDataRequest.Factor)
		countRetry++
		if countRetry > n.conf.BackoffDataRequest.Retry {
			return nil, xerrors.Errorf("Reached maximum retry attempts to address: %v", rdmPeer)
		}
	}
}

// SearchLocal implements the function of regex-based searching of files in local storage
func (n *node) SearchLocal(reg regexp.Regexp, allowEmpty bool) (files []types.FileInfo) {
	localStorage := n.conf.Storage.GetDataBlobStore()
	files = []types.FileInfo{}
	n.conf.Storage.GetNamingStore().ForEach(
		func(name string, hfile []byte) bool {
			if reg.Match([]byte(name)) {
				f := types.FileInfo{
					Name:     name,
					Metahash: string(hfile),
				}
				if n.StoreMatchingFile(f, localStorage, allowEmpty, &files) {
					return true
				}
			}
			return true
		},
	)
	return files
}

// StoreMatchingFile implements the function of collecting all matching files referencing the file's metahash
func (n *node) StoreMatchingFile(f types.FileInfo, localStorage storage.Store,
	allowEmpty bool, files *[]types.FileInfo) bool {
	fContent := localStorage.Get(f.Metahash)
	if fContent == nil {
		if allowEmpty {
			*files = append(*files, types.FileInfo{
				Name:     f.Name,
				Metahash: string(fContent),
				Chunks:   nil,
			})
		} else {
			return true
		}
	} else {
		n.StoreMatchingChunks(fContent, localStorage, f.Name, f.Metahash, files)
	}
	return false
}

// StoreMatchingChunks implements the function of collecting all chunks that are referenced by file's metahash
func (n *node) StoreMatchingChunks(fContent []byte, localStorage storage.Store,
	name string, hfile string, files *[]types.FileInfo) {
	var chunks [][]byte
	chunksHex := strings.Split(string(fContent), peer.MetafileSep)
	for _, chunkHex := range chunksHex {
		chunk := localStorage.Get(chunkHex)
		if chunk != nil {
			chunks = append(chunks, []byte(chunkHex))
		} else {
			chunks = append(chunks, nil)
		}
	}
	*files = append(*files, types.FileInfo{
		Name:     name,
		Metahash: hfile,
		Chunks:   chunks,
	})
}

// SearchNeighbor implements the function of distributing budgets and sending search request to valid neighbors
// and returns a list of RequestIDs if this search is started from source
func (n *node) SearchNeighbor(srMsg types.SearchRequestMessage, budget uint, exception []string,
	isSrc bool) ([]string, error) {
	neighborValid := n.tbl.GetListNeighbor(exception)
	neighbors := uint(len(neighborValid))
	var rIDs []string

	// If no valid neighbors, simply return
	if neighbors == 0 {
		return nil, nil
	}
	// Allocate budgets for valid neighbors
	var budgets []uint
	if budget <= neighbors { // If budget less than or equals number of valid neighbors, assign each allocation to 1
		budgets = make([]uint, budget)
		for idx := range budgets {
			budgets[idx] = 1
		}
	} else { // Otherwise we average on neighbors and allocate "leftover" randomly
		budgets = make([]uint, neighbors)
		basebudget := budget / neighbors
		leftover := budget % neighbors
		for idx := range budgets {
			budgets[idx] = basebudget
		}
		// Randomly allocating the leftover budget to neighbors
		for leftover > 0 {
			randomNeighbor := rand.Intn(int(neighbors))
			// Allocating 1 unit of the leftover budget to the randomly selected neighbor
			if budgets[randomNeighbor] == basebudget { // Ensuring not giving more than one extra to the same neighbor
				budgets[randomNeighbor]++
				leftover--
			}
		}
	}
	// Activate searching with sending search request messages to neighbors
	err := n.SendSearchRequestToNeighbor(srMsg, neighborValid, budgets, isSrc, &rIDs)
	if err != nil {
		return nil, err
	}
	return rIDs, nil
}

// SendSearchRequestToNeighbor implements the function of sending search request to valid neighbors
// also trigger search request notification and collect a list of RequestIDs triggered
func (n *node) SendSearchRequestToNeighbor(srMsg types.SearchRequestMessage, neighborValid []string,
	budgets []uint, isSrc bool, rID *[]string) error {
	for idx, budget := range budgets {
		if isSrc {
			srMsg.RequestID = xid.New().String()
			n.ntf.SearchRequestNotif(srMsg.RequestID, budget)
			*rID = append(*rID, srMsg.RequestID)
		}
		srMsg.Budget = budget
		// Create and send packet
		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(),
			neighborValid[idx], 0)
		tMsg, err := n.conf.MessageRegistry.MarshalMessage(srMsg)
		if err != nil {
			return err
		}
		packet := transport.Packet{
			Header: &header,
			Msg:    &tMsg,
		}
		err = n.conf.Socket.Send(neighborValid[idx], packet, time.Second)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkActiveFullMatch implements a goroutine that actively checking for a full match before expanding-ring timeout
func (n *node) checkActiveFullMatch(cancel context.CancelFunc, rIDs []string, fullMatchFound chan string) {
	defer n.wg.Done()
	// Check if neighbor search gets a full match file
	neighborFull := n.CheckResponseFullMatch(rIDs)
	if neighborFull != "" {
		fullMatchFound <- neighborFull
		cancel()
	}
}

// CheckResponseFullMatch implements the function of periodically and actively finding a "full match" from responses
// received from neighbors that sent search requests to, with the reference of RequestIDs
func (n *node) CheckResponseFullMatch(rIDs []string) (name string) {
	for _, rID := range rIDs {
	loop:
		for {
			select {
			case response := <-n.ntf.SearchWaitNotif(rID):
				matchf := n.CheckFirstFullMatch(response)
				if matchf != "" {
					return matchf
				}
			case <-time.After(time.Millisecond * 10):
				break loop
			}
		}
	}
	return ""
}

// CheckFinalFullMatch implements the function of thoroughly checking for a full match after expanding-ring timeout
func (n *node) CheckFinalFullMatch(rIDs []string) (name string) {
	for _, rID := range rIDs {
		closedChan := false
		for !closedChan {
			select {
			case response := <-n.ntf.SearchWaitNotif(rID):
				matchf := n.CheckFirstFullMatch(response)
				if matchf != "" {
					return matchf
				}
			default:
				n.ntf.SearchSignalNotif(rID)
				closedChan = true
			}
		}
	}
	return ""
}

// CheckFirstFullMatch implements a check on list of files of finding a "full match" and returns the file name if found
func (n *node) CheckFirstFullMatch(files []types.FileInfo) (name string) {
	for _, f := range files {
		exist := true
		for _, chunk := range f.Chunks {
			if chunk == nil {
				exist = false
			}
		}
		if exist {
			return f.Name
		}
	}
	return ""
}
