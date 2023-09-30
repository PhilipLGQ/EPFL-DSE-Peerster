package udp

import (
	"errors"
	"go.dedis.ch/cs438/transport"
	"math"
	"net"
	"os"
	"sync"
	"time"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	/* panic("to be implemented in HW0") */
	udpAddr, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		return &Socket{}, err
	}
	conn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return &Socket{}, err
	}
	return &Socket{conn: *conn}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	// UDP connection status
	conn net.UDPConn
	// Lists of send and received packets
	sendlist PktList
	recvlist PktList
}

// PktList implements a protected packet lists in concurrent R/W
type PktList struct {
	pkl []transport.Packet
	mu  sync.RWMutex
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	/* panic("to be implemented in HW0") */
	err := s.conn.Close()
	return err
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	/* panic("to be implemented in HW0") */
	udpAddr, err := net.ResolveUDPAddr("udp4", dest)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		return err
	}

	// Regularize timeout if required
	if timeout <= 0 {
		timeout = math.MaxInt64
	}
	err = conn.SetWriteDeadline(time.Now().Add(timeout))
	if err != nil {
		return err
	}

	// Marshal packet
	buf, err := pkt.Marshal()
	if err != nil {
		return err
	}
	_, errSend := conn.Write(buf)
	if errSend != nil {
		return errSend
	}

	// Add sent packet to sendlist
	s.sendlist.AddPacket(pkt.Copy())
	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	/* panic("to be implemented in HW0") */
	var pkt transport.Packet

	if timeout > 0 {
		err := s.conn.SetReadDeadline(time.Now().Add(timeout))
		if err != nil {
			return transport.Packet{}, err
		}
	}

	buf := make([]byte, bufSize)
	lenRecv, _, errRecv := s.conn.ReadFromUDP(buf)
	if errors.Is(errRecv, os.ErrDeadlineExceeded) {
		return transport.Packet{}, transport.TimeoutError(0)
	} else if errRecv != nil {
		return transport.Packet{}, errRecv
	}
	err := pkt.Unmarshal(buf[:lenRecv])
	if err != nil {
		return transport.Packet{}, err
	}
	s.recvlist.AddPacket(pkt.Copy())
	return pkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	/* panic("to be implemented in HW0") */
	return s.conn.LocalAddr().String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	/* panic("to be implemented in HW0") */
	return s.recvlist.GetList()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	/* panic("to be implemented in HW0") */
	return s.sendlist.GetList()
}

// GetList implements PktList of returning a copy of the packet list
func (pl *PktList) GetList() []transport.Packet {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	pklCopy := make([]transport.Packet, len(pl.pkl))
	for idx, pkt := range pl.pkl {
		pklCopy[idx] = pkt.Copy()
	}
	return pklCopy
}

// AddPacket implements PktList of adding a packet to the packet list
func (pl *PktList) AddPacket(pkt transport.Packet) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	pl.pkl = append(pl.pkl, pkt)
}
