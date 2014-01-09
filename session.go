// session between consumer and producer.

package goupr

import (
    "github.com/couchbaselabs/go-couchbase"
    mcd "github.com/dustin/gomemcached"
    mc "github.com/dustin/gomemcached/client"
    "log"
    "encoding/binary"
    "fmt"
    "time"
)

// exponential backoff while connection retry.
const kInitialRetryInterval = 1 * time.Second
const kMaximumRetryInterval = 30 * time.Second

// contains an active memcached connection.
type UprConnection struct {
    host   string              // host to which `mc` is connected.
    conn   *mc.Client
    flogch chan *mcd.MCResponse // internal channel to receive failover log
    rollch chan *mcd.MCResponse // internal channel to receive stream req
}

// Multiple streams (actually one stream per vbucket) can be opened on a
// connection.
type UprStream struct {
    vbucket  uint16  // vbucket id
    vuuid    uint64  // vbucket uuid
    opaque   uint32  // messages from producer to this stream have same value
    highseq  uint64  // to be supplied by the application
    startseq uint64  // to be supplied by the application
    endseq   uint64  // to be supplied by the application
}

var eventTypes = map[mcd.CommandCode]string{ // Refer UprEvent
    UPR_MUTATION: "Uprmutation",
    UPR_DELETION: "Uprdeletion",
}
// UprEvent is created for messages from streams.
type UprEvent struct {
    Bucket  string // bucket name for this event
    Opstr   string // TODO: Make this consistent with TAP
    Vbucket uint16 // vbucket number
    SeqNo   uint64 // sequence number
    Key     []byte
    Value   []byte
}

// per bucket structure managing all UPR streams.
type UprFeed struct {
    // Exported channel where an aggreegate of all UprEvent are sent to app.
    C       chan UprEvent

    bucket  *couchbase.Bucket         // upr client for bucket
    name    string                    // name of the connection used in UPR_OPEN
    conns   []*UprConnection          // per node memcached connections
    vbmap   map[uint16]*UprConnection // vbucket-number->connection mapping
    streams map[uint16]*UprStream     // vbucket-number->stream mapping
    // `quit` channel is used to signal that a Close() is called on UprFeed
    quit    chan bool
    // channels that receives memcached messages
    msgch   chan []interface{}
}

// this is the first call to be made the by application to start UPR
// connection.
func StartUprFeed(b *couchbase.Bucket, name string) *UprFeed {
    feed := &UprFeed {
        C:       make(chan UprEvent, 16),
        bucket:  b,
        name:    name,
        streams: make(map[uint16]*UprStream),
        quit:    make(chan bool),
        msgch:   make(chan []interface{}, 16),
    }
    killSwitch := make(chan bool)
    go feed.run(name, killSwitch)

    return feed
}

func (feed *UprFeed) FailoverLog(vb uint16) ([][2]uint64, error) {
    uprconn := feed.vbmap[vb]
    drainChan(uprconn.flogch)
    if err := RequestFailoverLog(uprconn.conn, vb, uint32(vb)); err != nil {
        return nil, err
    }
    res := <-uprconn.flogch // Block until failover logs received from server
    if flogs, err := parseFailoverLog(res.Body); err != nil {
        return nil, err
    } else {
        return flogs, nil
    }
}

// subsequently application must call StartStream() to start individual streams
// over the connection. If successful returns the opaque number or rollback
// sequence, else return error.
func (feed *UprFeed) StartStream(vbucket uint16,
    vuuid, startSeqno, endSeqno, highSeqno uint64) (uint64, [][2]uint64, error) {

    var err error

    uprconn := feed.vbmap[vbucket]
    opaque := uint32(vbucket)
    err = RequestStream(uprconn.conn, uint32(0) /*flags*/,
        opaque, vuuid, startSeqno, endSeqno, highSeqno)
    if err != nil {
        return 0, nil, err
    }
    drainChan(uprconn.rollch)
    res := <-uprconn.rollch // Block until stream-request acknowleged from server
    switch {
    case res.Opaque == opaque:
        err = fmt.Errorf("Unexpected stream request for vbucket %v", vbucket)
        return 0, nil, err
    case res.Status == ROLLBACK && len(res.Extras) != 8:
        err = fmt.Errorf("UprStream: Invalid rollback %v\n", res.Extras)
        return 0, nil, err
    case res.Status == ROLLBACK:
        rollback := binary.BigEndian.Uint64(res.Extras)
        return rollback, nil, nil
    }
    feed.streams[vbucket] = &UprStream{
        vbucket:  vbucket,
        vuuid:    vuuid,
        opaque:   opaque,
        highseq:  highSeqno,
        startseq: startSeqno,
        endseq:   endSeqno,
    }
    flogs, err := parseFailoverLog(res.Body)
    if err != nil {
        return 0, nil, err
    }
    return 0, flogs, nil
}

//---- Local functions.

// connect with all servers holding data for `feed.bucket` and try
// reconnecting until `feed` is closed.
func (feed *UprFeed) run(name string, killSwitch chan bool) {
    retryInterval := kInitialRetryInterval
    bucketOK := true
    for {
        // Connect to the TAP feed of each server node
        if bucketOK {
            err := feed.connectToNodes(name)
            if err == nil {
                go feed.doSession(killSwitch)
                // Run until a feed to one of the node fails or the
                // bucket-feed is closed
                select {
                case <-killSwitch:
                case <-feed.quit:
                    return
                }
                feed.closeNodes()
                retryInterval = kInitialRetryInterval
            }
        }

        // On error, try to refresh the bucket in case the list of nodes changed:
        log.Printf("go-couchbase: UPR connection lost; reconnecting to bucket %q in %v",
            feed.bucket.Name, retryInterval)
        err := feed.bucket.Refresh()
        bucketOK = err == nil

        select {
        case <-time.After(retryInterval):
        case <-feed.quit:
            return
        }
        if retryInterval *= 2; retryInterval > kMaximumRetryInterval {
            retryInterval = kMaximumRetryInterval
        }
    }
}

func (feed *UprFeed) doSession(killSwitch chan bool) {
loop:
    for {
        select {
        case mcevent := <-feed.msgch:
            uprconn := mcevent[0].(*UprConnection)
            if _, ok := mcevent[1].(error); ok {
                log.Printf("Received error: %v", uprconn.host)
                killSwitch <- true
                break loop
            } else if req, ok := mcevent[1].(mcd.MCRequest); ok {
                feed.handleUprMessage(&req)
            }
        case <-feed.quit:
            break loop
        }
    }
}

func (feed *UprFeed) handleUprMessage(req *mcd.MCRequest) {
    switch req.Opcode {
    case UPR_FAILOVER_LOG:
        res := req2res(req)
        vb := uint16(res.Opaque)
        feed.vbmap[vb].flogch <- res
    case UPR_STREAM_REQ:
        res := req2res(req)
        vb := uint16(res.Opaque)
        feed.vbmap[vb].rollch <- res
    case UPR_STREAM_END:
        res := req2res(req)
        vb := uint16(res.Opaque)
        delete(feed.streams, vb)
        delete(feed.vbmap, vb)
    case UPR_MUTATION, UPR_DELETION:
        feed.C <- feed.makeUprEvent(req)
    case UPR_CLOSE_STREAM, UPR_SNAPSHOTM, UPR_EXPIRATION,
         UPR_FLUSH, UPR_ADD_STREAM:
        log.Printf("Opcode %v not implemented", req.Opcode)
    default:
        log.Println("ERROR: un-known opcode received", req)
    }
}

func (feed *UprFeed) makeUprEvent(req *mcd.MCRequest) UprEvent {
    by_seqno := binary.BigEndian.Uint64(req.Extras[:8])
    e := UprEvent{
        Bucket:  feed.bucket.Name,
        Opstr:   eventTypes[req.Opcode],
        Vbucket: req.VBucket,
        SeqNo: by_seqno,
        Key: req.Key,
        Value: req.Body,
    }
    return e
}

func (feed *UprFeed) connectToNodes(name string) (err error) {
    b := feed.bucket
    feed.conns = make([]*UprConnection, 0)
    feed.vbmap = make(map[uint16]*UprConnection)
    servers, vbmaps := b.VBSMJson.ServerList, b.VBSMJson.VBucketMap

loop:
    for _, cp := range b.GetConnPools() {
        if cp == nil {
            err = fmt.Errorf("go-couchbase: no connection pool")
            break loop
        }
        conn, err := cp.Get()
        if err != nil {
            break loop
        }

        // A connection can't be used after UPR; Dont' count it against the
        // connection pool capacity

        //<-cp.createsem
        cp.Createsem()

        // Do UPR_OPEN
        err = UprOpen(conn, name, uint32(0x1) /*flags*/)
        if err != nil {
            break loop
        }

        uprconn := &UprConnection{
            host:   cp.Host(),
            conn:   conn,
            flogch: make(chan *mcd.MCResponse),
            rollch: make(chan *mcd.MCResponse),
        }
        feed.conns = append(feed.conns, uprconn)
        // Collect vbuckets under this connection
        for vbno, _ := range vbmaps {
            host := servers[vbmaps[int(vbno)][0]]
            if uprconn.host == host {
                feed.vbmap[uint16(vbno)] = uprconn
            }
        }
    }
    if err != nil {
        feed.closeNodes()
    } else {
        for _, uprconn := range feed.conns {
            go feed.doRecieve(uprconn, feed.msgch)
        }
    }
    return
}

func (feed *UprFeed) doRecieve(uprconn *UprConnection, ch chan<- []interface{}) {
    var hdr [mcd.HDR_LEN]byte
    var msg []interface{}
    var err error

    mcconn := uprconn.conn.GetMC()
loop:
    for {
        var pkt mcd.MCRequest
        if err = pkt.Receive(mcconn, hdr[:]); err != nil {
            msg = []interface{}{uprconn, err}
        } else {
            msg = []interface{}{uprconn, pkt}
        }
        select {
        case ch <- msg:
            if err != nil {
                break loop
            }
        case <-feed.quit:
            break loop
        }
    }
}

func (feed *UprFeed) Close() {
    select {
    case <-feed.quit:
        return
    default:
    }
    feed.closeNodes()
    close(feed.quit)
    close(feed.C)
    close(feed.msgch)
}

func (feed *UprFeed) closeNodes() {
    for _, uprconn := range feed.conns {
        uprconn.conn.Close()
        close(uprconn.flogch)
        close(uprconn.rollch)
    }
    feed.vbmap = nil
    feed.conns = nil
}

func req2res(req *mcd.MCRequest) *mcd.MCResponse {
    return &mcd.MCResponse{
        Opcode:  req.Opcode,
        Cas:     req.Cas,
        Opaque:  req.Opaque,
        Status:  mcd.Status(req.VBucket),
        Extras:  req.Extras,
        Key:     req.Key,
        Body:    req.Body,
    }
}

func drainChan(ch chan *mcd.MCResponse) {
    for {
        select {
        case <-ch:
        default:
            return
        }
    }
}
