package goupr

import (
    "encoding/binary"
    "fmt"
    mcd "github.com/dustin/gomemcached"
    //mc "github.com/dustin/gomemcached/client"
    "log"
)

const opaqueOpen            = 0xBEAF0001
const opaqueFailoverRequest = 0xBEAF0002

type transporter interface {
    Transmit(*mcd.MCRequest) error
    Receive() (*mcd.MCResponse, error)
}

// FailoverLog is a slice of 2 element array, containing a list of,
// [[vuuid, sequence-no], [vuuid, sequence-no] ...]
type FailoverLog [][2]uint64

func uprOpen(conn transporter, name string, flags uint32) error {
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
        return fmt.Errorf("connection error, %v", err)
    } else if res.Opcode != UPR_OPEN {
        return fmt.Errorf("unexpected #opcode %v", res.Opcode)
    } else if req.Opaque != res.Opaque {
        return fmt.Errorf("opaque mismatch, %v over %v", res.Opaque, res.Opaque)
    } else if res.Status != mcd.SUCCESS {
        return fmt.Errorf("status %v", res.Status)
    }
    return nil
}

func requestFailoverLog(conn transporter,
    vbucket uint16) (flog FailoverLog, err error) {

    var res *mcd.MCResponse

    req := &mcd.MCRequest{
        Opcode:  UPR_FAILOVER_LOG,
        VBucket: vbucket,
        Opaque:  uint32(vbucket),
    }
    if err = conn.Transmit(req); err != nil { // Transmit the request
        return nil, err
    }

    if res, err = conn.Receive(); err != nil { // Wait for response
        return nil, fmt.Errorf("connection error, %v", err)
    } else if res.Opcode != UPR_FAILOVER_LOG {
        return nil, fmt.Errorf("unexpected #opcode %v", res.Opcode)
    } else if req.Opaque != res.Opaque {
        err = fmt.Errorf("opaque mismatch, %v over %v", res.Opaque, res.Opaque)
        return nil, err
    } else if res.Status != mcd.SUCCESS {
        return nil, fmt.Errorf("status %v", res.Status)
    }
    if flog, err = parseFailoverLog(res.Body); err != nil {
        return nil, err
    }
    return flog, err
}

func requestStream(conn transporter, flags, opq uint32, vb uint16,
    vuuid, startSeqno, endSeqno, highSeqno uint64) (err error) {

    req := &mcd.MCRequest{Opcode: UPR_STREAM_REQ, Opaque: opq, VBucket: vb}
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

func endStream(conn transporter, flags uint32, vbucket uint16) (err error) {
    req := &mcd.MCRequest{
        Opcode:  UPR_STREAM_END,
        Opaque:  uint32(vbucket),
        VBucket: vbucket,
    }
    req.Extras = make([]byte, 4) // #Extras
    binary.BigEndian.PutUint32(req.Extras, flags)
    err = conn.Transmit(req)
    return
}

func parseFailoverLog(body []byte) (FailoverLog, error) {
    if len(body)%16 != 0 {
        err := fmt.Errorf("invalid body length %v, in failover-log", len(body))
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
