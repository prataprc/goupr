package goupr

import (
    "encoding/binary"
    "log"
    "github.com/dustin/gomemcached"
)

type Stream struct {
    client     *Client // Client connection handles this stream
    vbucket    uint32  // vbucket id, TODO : Should be removed ?
    vuuid      uint64  // vbucket uuid
    opaque     uint32  // all messages from producer to this stream have same
    sequenceNo uint64  // last read sequence number for `vbucket`
    consumer   chan []interface{}
}

// Stream specific messages from producer will be handled here.
func (stream *Stream) handle(req *gomemcached.MCRequest) {
    res := make(chan []interface{})
    switch req.Opcode {
    case UPR_STREAM_END:
        close(stream.consumer)
        stream.handleStreamEnd(req)
    case UPR_SNAPSHOTM:
    case UPR_MUTATION:
    case UPR_DELETION:
    case UPR_EXPIRATION:
        stream.consumer <- []interface{}{req, res}
        <-res
    }
}

// Close the stream.
func (stream *Stream) handleStreamEnd(req *gomemcached.MCRequest) {
    if len(req.Extras) != 4 {
        log.Printf("UPR_STREAM_END: Opaque(%v) extras(%v)", req.Extras)
    }
    flags := binary.BigEndian.Uint32(req.Extras)
    switch flags { // TODO: avoid magic number for flags.
    case 0x0:
        log.Printf("Closing stream %v without error", stream.vuuid)
    case 0x1:
        log.Printf("Closing stream %v, remote changed state", stream.vuuid)
    default:
        log.Panicf("Unknown flag received for stream close (%v)", stream.vuuid)
    }

    stream.client.Lock()
    delete(stream.client.streams, req.Opaque)
    stream.client.Unlock()
}
