// Transport functions related UPR.

package goupr

import (
	"encoding/binary"
	"fmt"
	mcd "github.com/dustin/gomemcached"
	mc "github.com/dustin/gomemcached/client"
	"log"
)

const opaqueOpen = 0xBEAF0001
const opaqueFailoverRequest = 0xBEAF0002

type FailoverLog [][2]uint64

// UPR_OPEN transport function, synchronous call. Only after an open_connection
// succeeds streams can be requested.
func UprOpen(conn *mc.Client, name string, flags uint32) error {
	if len(name) > 65535 {
		log.Panicln("UprOpen: name cannot exceed 65535")
	}

	req := &mcd.MCRequest{Opcode: UPR_OPEN, Opaque: opaqueOpen}
	req.Key = []byte(name) // #Key

	req.Extras = make([]byte, 8)
	binary.BigEndian.PutUint32(req.Extras[:4], 0) // #Extras.sequenceNo
	// while consumer is opening the connection Type flag needs to be cleared.
	binary.BigEndian.PutUint32(req.Extras[4:], flags) // #Extras.flags

	if err := conn.Transmit(req); err != nil { // Transmit the request
		return err
	}

	if res, err := conn.Receive(); err != nil { // Wait for response
		return fmt.Errorf("UprOpen: connection error, %v", err)
	} else if res.Opcode != UPR_OPEN {
		return fmt.Errorf("UprOpen: unexpected #opcode %v", res.Opcode)
	} else if req.Opaque != res.Opaque {
		return fmt.Errorf(
			"UprOpen: #opaque mismatch, %v over %v", res.Opaque, res.Opaque)
	} else if res.Status != mcd.SUCCESS {
		return fmt.Errorf("UprOpen: Status %v", res.Status)
	}
	return nil
}

// UPR_FAILOVER_LOG, just post the request on the connection. Since UPR
// connection is full duplex, it is the reponsibility of the caller to detect
// the response and get the failover-log.
func RequestFailoverLog(conn *mc.Client, vb uint16, opaque uint32) error {
	req := &mcd.MCRequest{Opcode: UPR_FAILOVER_LOG, VBucket: vb, Opaque: 0x10}
	req.Key = []byte{}        // #Key
	req.Extras = []byte{}     // #Extras
	err := conn.Transmit(req) // Transmit the request
	return err
}

// Parse the failover log that is received as payload in UPR_STREAM_REQ or
// UPR_FAILOVER_LOG response.
func parseFailoverLog(body []byte) (FailoverLog, error) {
	if len(body)%16 != 0 {
		err := fmt.Errorf("Invalid body length %v, in failover-log", len(body))
		return nil, err
	}
	log := make(FailoverLog, len(body)/16)
	for i, j := 0, 0; i < len(body); i += 16 {
		vuuid := binary.BigEndian.Uint64(body[i : i+8])
		seqno := binary.BigEndian.Uint64(body[i+8 : i+16])
		log[j] = [2]uint64{vuuid, seqno}
		j++
	}
	return log, nil
}

// UPR_STREAM_REQ, just post the request on the connection. Since UPR
// connection is full duplex, it is the reponsibility of the caller to detect
// the response.
func RequestStream(conn *mc.Client, flags uint32, opq uint32, vb uint16,
	vuuid, startSeqno, endSeqno, highSeqno uint64) (err error) {

	req := &mcd.MCRequest{Opcode: UPR_STREAM_REQ, Opaque: opq, VBucket: vb}
	req.Key = []byte{} // #Keys

	req.Extras = make([]byte, 40) // #Extras
	binary.BigEndian.PutUint32(req.Extras[:4], flags)
	binary.BigEndian.PutUint32(req.Extras[4:8], uint32(0))
	binary.BigEndian.PutUint64(req.Extras[8:16], startSeqno)
	binary.BigEndian.PutUint64(req.Extras[16:24], endSeqno)
	binary.BigEndian.PutUint64(req.Extras[24:32], vuuid)
	binary.BigEndian.PutUint64(req.Extras[32:40], highSeqno)

	err = conn.Transmit(req)
	return
}
