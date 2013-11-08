package goupr

import (
    "encoding/binary"
    "fmt"
    "errors"
    "log"
    "github.com/dustin/gomemcached"
)

// %UPR_OPEN response
func (client *Client) UprOpen(
    req *gomemcached.MCRequest, name string, seqNo, flags uint32) error {

    if len(name) > 65535 {
        log.Panicln("UprOpen: name cannot exceed 65535")
    }

    req.Opcode = UPR_OPEN   // #OpCode
    req.Key = []byte(name)  // #Key
    req.Extras = make([]byte, 8)
    binary.BigEndian.PutUint32(req.Extras[:4], seqNo)
                            // #Extras.sequenceNo
    // while consumer is opening the connection Type flag needs to be cleared.
    binary.BigEndian.PutUint32(req.Extras[4:], flags)
                            // #Extras.flags

    // Trasmit the request
    if err := req.Transmit(client.conn); err != nil {
        return err
    }
    client.name = name

    // Wait for response
    res := <-client.response
    if res.Opcode != UPR_OPEN {
        return fmt.Errorf("UprOpen: unexpected #opcode", res.Opcode)
    }
    if req.Opaque != res.Opaque {
        return fmt.Errorf("UprOpen: #opaque mismatch", req.Opaque, res.Opaque)
    }
    if res.Status != gomemcached.SUCCESS {
        return fmt.Errorf("UprOpen: Status", res.Status)
    }
    return nil
}

func (client *Client) UprFailOverLog(
    req *gomemcached.MCRequest) ([][2]uint64, error) {

    req.Opcode = UPR_FAILOVER_LOG   // #OpCode
    req.Opaque = 0xDEADBEEF         // #Opaque
    req.Key = []byte{}              // #Key
    req.Extras = []byte{}           // #Extras

    // Trasmit the request
    if err := req.Transmit(client.conn); err != nil {
        return nil, err
    }

    // Wait for response
    res := <-client.response
    if res.Opcode != UPR_FAILOVER_LOG {
        err := fmt.Errorf( "UprFailOverLog: unexpected #opcode", res.Opcode)
        return nil, err
    }
    if req.Opaque != res.Opaque {
        err := fmt.Errorf(
            "UprFailOverLog: #opaque mismatch", req.Opaque, res.Opaque)
        return nil, err
    }
    if len(res.Body) % 16 != 0 {
        err := fmt.Errorf(
            "UprFailOverLog: Invalide body of length", len(res.Body))
        return nil, err
    }
    if res.Status != gomemcached.SUCCESS {
        return nil, fmt.Errorf("UprOpen: Status", res.Status)
    }

    // Return the log
    log := make([][2]uint64, len(res.Body) / 16)
    for i, j := 0, 0; i < len(res.Body); i += 16 {
        vuuid := binary.BigEndian.Uint64(res.Body[i:i+8])
        seqno := binary.BigEndian.Uint64(res.Body[i+8:i+16])
        log[j] = [2]uint64{vuuid, seqno}
        j++
    }
    return log, nil
}

func (client *Client) UprStream(req *gomemcached.MCRequest, flags uint32,
    startSeqno, endSeqno, vuuid, highSeqno uint64) (*Stream, uint64, error) {

    req.Opcode = UPR_STREAM_REQ     // #OpCode
    req.Opaque = client.opqCount; client.opqCount++
                                    // #Opaque
    req.Key = []byte{}              // #Keys
    req.Extras = make([]byte, 40)
    binary.BigEndian.PutUint32(req.Extras[:4], flags)
    binary.BigEndian.PutUint32(req.Extras[4:8], uint32(0))
    binary.BigEndian.PutUint64(req.Extras[8:16], startSeqno)
    binary.BigEndian.PutUint64(req.Extras[16:24], endSeqno)
    binary.BigEndian.PutUint64(req.Extras[24:32], vuuid)
    binary.BigEndian.PutUint64(req.Extras[32:40], highSeqno)
                                    // #Extras

    stream := client.NewStream(0, req.Opaque, make(chan []interface{}))
    client.addStream(req.Opaque, stream)

    if err := req.Transmit(client.conn); err != nil { // Transmit the request
        return nil, 0, err
    }

    res := <-client.response // Wait for response
    if res == nil {
        return nil, 0, errors.New("closed")
    }
    if res.Opcode != UPR_STREAM_REQ {
        err := fmt.Errorf("UprStream: unexpected #opcode", res.Opcode)
        return nil, 0, err
    }
    if req.Opaque != res.Opaque {
        err := fmt.Errorf("UprStream: #opaque mismatch", req.Opaque, res.Opaque)
        return nil, 0, err
    }

    // If not success, remove the Stream reference from client connection,
    // that was optimistically added above.
    if res.Status != gomemcached.SUCCESS {
        client.evictStream(req.Opaque)
    }
    // Check whether it is rollback
    switch res.Status {
    case gomemcached.SUCCESS:
        return stream, 0, nil
    case ROLLBACK:
        if len(res.Extras) != 8 {
            log.Panicln("UprStream: Invalid rollback", res.Extras)
        }
        rollback := binary.BigEndian.Uint64(res.Extras)
        return nil, rollback, nil
    default:
        err := fmt.Errorf("UprStream: Status", res.Status)
        return nil, 0, err
    }

}

func NewRequest(opcode gomemcached.CommandCode, cas uint64, opaque uint32,
    vbucket uint16) *gomemcached.MCRequest {

    return &gomemcached.MCRequest{
        Opcode: opcode,
        Cas: cas,
        Opaque: opaque,
        VBucket: vbucket,
    }
}
