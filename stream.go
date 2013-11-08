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
func (stream *Stream) handle(res *gomemcached.MCResponse) {
    ch := make(chan []interface{})
    switch res.Opcode {
    case UPR_STREAM_END:
        stream.handleStreamEnd(res)
    case UPR_SNAPSHOTM, UPR_MUTATION, UPR_DELETION, UPR_EXPIRATION:
        stream.Consumer <- []interface{}{res, ch}
    }
}

// Close the stream.
func (stream *Stream) handleStreamEnd(res *gomemcached.MCResponse) {
    if len(res.Extras) != 4 {
        log.Printf("UPR_STREAM_END: Opaque(%v) extras(%v)", res.Extras)
    }
    flags := binary.BigEndian.Uint32(res.Extras)
    switch flags { // TODO: avoid magic number for flags.
    case 0x0:
        log.Printf("Closing stream %v without error", stream.Opaque)
    case 0x1:
        log.Printf("Closing stream %v, remote changed state", stream.Vuuid)
    default:
        log.Panicf("Unknown flag received for stream close (%v)", stream.Vuuid)
    }
    close(stream.Consumer)
    stream.Client.evictStream(res.Opaque)
}
