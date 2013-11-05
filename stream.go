package goupr

import (
    "encoding/binary"
    "log"
    "github.com/dustin/gomemcached"
)

type Stream struct {
    Client     *Client // Client connection handles this stream
    Vbucket    uint32  // vbucket id, TODO : Should be removed ?
    Vuuid      uint64  // vbucket uuid
    Opaque     uint32  // all messages from producer to this stream have same
    SequenceNo uint64  // last read sequence number for `vbucket`
    Consumer   chan []interface{}
}

// Stream specific messages from producer will be handled here.
func (stream *Stream) handle(req *gomemcached.MCRequest) {
    res := make(chan []interface{})
    switch req.Opcode {
    case UPR_STREAM_END:
        close(stream.Consumer)
        stream.handleStreamEnd(req)
    case UPR_SNAPSHOTM:
    case UPR_MUTATION:
    case UPR_DELETION:
    case UPR_EXPIRATION:
        stream.Consumer <- []interface{}{req, res}
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
        log.Printf("Closing stream %v without error", stream.Vuuid)
    case 0x1:
        log.Printf("Closing stream %v, remote changed state", stream.Vuuid)
    default:
        log.Panicf("Unknown flag received for stream close (%v)", stream.Vuuid)
    }

    stream.Client.Lock()
    delete(stream.Client.streams, req.Opaque)
    stream.Client.Unlock()
}
