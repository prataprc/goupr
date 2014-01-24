// goupr package provides client sdk for UPR connections and streaming.
package goupr

import (
    "github.com/couchbaselabs/go-couchbase"
    "log"
)

// UprStream will maintain stream information per vbucket opened on a
// connection.
type UprStream struct {
    Vbucket  uint16 // vbucket id
    Vuuid    uint64 // vbucket uuid
    Opaque   uint32 // messages from producer to this stream have same value
    Highseq  uint64 // to be supplied by the application
    Startseq uint64 // to be supplied by the application
    Endseq   uint64 // to be supplied by the application
    Flog     FailoverLog
}

// UprEvent objects will be created for stream mutations and deletions and
// published on UprFeed:C channel.
type UprEvent struct {
    Bucket  string // bucket name for this event
    Opstr   string // TODO: Make this consistent with TAP
    Vbucket uint16 // vbucket number
    Seqno   uint64 // sequence number
    Key     []byte
    Value   []byte
}

// UprFeed is per bucket structure managing connections to all nodes and
// vbucket streams.
type UprFeed struct {
    // Exported channel where an aggregate of all UprEvent are sent to app.
    C       chan UprEvent

    bucket  *couchbase.Bucket         // upr client for bucket
    name    string                    // name of the connection used in UPR_OPEN
    vbmap   map[uint16]*uprConnection // vbucket-number->connection mapping
    streams map[uint16]*UprStream     // vbucket-number->stream mapping
    // `quit` channel is used to signal that a Close() is called on UprFeed
    quit    chan bool
}

// StartUprFeed creates a feed that aggregates all mutations for the bucket
// and publishes them as UprEvent on UprFeed:C channel.
func StartUprFeed(b *couchbase.Bucket, name string,
    streams map[uint16]*UprStream) (feed *UprFeed, err error) {

    uprconns, err := connectToNodes(b, name)
    if err == nil {
        vbmap := vbConns(b, uprconns)
        if streams == nil {
            streams = initialStreams(vbmap)
        }
        streams, err = startStreams(streams, vbmap)
        if err != nil {
            closeConnections(uprconns)
            return nil, err
        }
        feed = &UprFeed{
            bucket:  b,
            name:    name,
            vbmap:   vbmap,
            streams: streams,
            quit:    make(chan bool),
            C:       make(chan UprEvent, 16),
        }
        go feed.doSession(uprconns)
    }
    return feed, err
}

// GetFailoverLogs return a list of vuuid and sequence number for all
// vbuckets.
func GetFailoverLogs(b *couchbase.Bucket, name string) ([]FailoverLog, error) {
    var flog FailoverLog
    var err  error
    uprconns, err := connectToNodes(b, name)
    if err == nil {
        flogs := make([]FailoverLog, 0)
        vbmap := vbConns(b, uprconns)
        for vb, uprconn := range vbmap {
            if flog, err = requestFailoverLog(uprconn.conn, vb); err != nil {
                return nil, err
            }
            flogs = append(flogs, flog)
        }
        closeConnections(uprconns)
        return flogs, nil
    }
    return nil, err
}

// Close UprFeed. Does the opposite of StartUprFeed()
func (feed *UprFeed) Close() {
    if feed.hasQuit() {
        return
    }
    log.Println("Closing feed")
    close(feed.quit)
    feed.vbmap = nil
    feed.streams = nil
}

// Get stream will return the UprStream structure for vbucket `vbucket`.
// Caller can use the UprStream information to restart the stream later.
func (feed *UprFeed) GetStream(vbno uint16) *UprStream {
    return feed.streams[vbno]
}

func initialStreams(vbmaps map[uint16]*uprConnection) map[uint16]*UprStream {
    streams := make(map[uint16]*UprStream)
    for vbno := range vbmaps {
        streams[vbno] = &UprStream{
            Vbucket:  vbno,
            Vuuid:    0,
            Opaque:   uint32(vbno),
            Highseq:  0,
            Startseq: 0,
            Endseq:   0xFFFFFFFFFFFFFFFF,
        }
    }
    return streams
}
