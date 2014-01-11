// session between consumer and producer.

package goupr

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	mcd "github.com/dustin/gomemcached"
	mc "github.com/dustin/gomemcached/client"
	"log"
	"sync"
	"time"
)

// exponential backoff while connection retry.
const initialRetryInterval = 1 * time.Second
const maximumRetryInterval = 30 * time.Second

// uprConnection contains an active memcached connection.
type uprConnection struct {
	host   string // host to which `mc` is connected.
	conn   *mc.Client
	flogch chan *mcd.MCResponse // internal channel to receive failover log
	sreqch chan *mcd.MCResponse // internal channel to receive stream req
}

// uprStream maintain stream information per vbucket that opened on a
// connection.
type uprStream struct {
	vbucket  uint16 // vbucket id
	vuuid    uint64 // vbucket uuid
	opaque   uint32 // messages from producer to this stream have same value
	highseq  uint64 // to be supplied by the application
	startseq uint64 // to be supplied by the application
	endseq   uint64 // to be supplied by the application
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
	Seqno   uint64 // sequence number
	Key     []byte
	Value   []byte
}

// UprFeed is per bucket structure managing all UPR streams.
type UprFeed struct {
	// Exported channel where an aggreegate of all UprEvent are sent to app.
	C chan UprEvent

	bucket  *couchbase.Bucket         // upr client for bucket
	name    string                    // name of the connection used in UPR_OPEN
	conns   []*uprConnection          // per node memcached connections
	vbmap   map[uint16]*uprConnection // vbucket-number->connection mapping
	streams map[uint16]*uprStream     // vbucket-number->stream mapping
	mu      sync.Mutex
	// `quit` channel is used to signal that a Close() is called on UprFeed
	quit chan bool
	// channels that receives memcached messages
	msgch chan []interface{}
}

// StartUprFeed is the first call to be made the by application to start UPR
// connection.
func StartUprFeed(b *couchbase.Bucket, name string) (*UprFeed, error) {
	feed := &UprFeed{
		C:       make(chan UprEvent, 16),
		bucket:  b,
		name:    name,
		streams: make(map[uint16]*uprStream),
		quit:    make(chan bool),
		msgch:   make(chan []interface{}, 16),
	}
	killSwitch := make(chan bool)

	err := feed.connectToNodes(name)
	go feed.run(name, killSwitch)

	return feed, err
}

// FailoverLog returns list of vuuid and sequence number for specified
// vbucket.
func (feed *UprFeed) FailoverLog(vb uint16) (FailoverLog, error) {
	uprconn := feed.vbmap[vb]
	drainChan(uprconn.flogch)
	if err := RequestFailoverLog(uprconn.conn, vb, uint32(vb)); err != nil {
		return nil, err
	}
	res := <-uprconn.flogch // Block until failover logs received from server
	flogs, err := parseFailoverLog(res.Body)
	if err != nil {
		return nil, err
	}
	return flogs, nil
}

// StartStream must be called by application after StartUprFeed(). A call to
// this function opens a streams for every vbucket
// If successful returns the opaque number or rollback sequence, else return
// error.
//
// IMPORTANT: this function must execute in a go-routine different from
// the go-routine that waits on feed.C channel for UprEvent.
func (feed *UprFeed) StartStream(vbucket uint16,
	vuuid, startSeqno, endSeqno, highSeqno uint64) (uint64, FailoverLog, error) {

	var err error

	uprconn := feed.vbmap[vbucket]
	opaque := uint32(vbucket)
	err = RequestStream(uprconn.conn, uint32(0) /*flags*/, opaque,
		uint16(0) /*vbucket*/, vuuid, startSeqno, endSeqno, highSeqno)
	if err != nil {
		return 0, nil, err
	}
	drainChan(uprconn.sreqch)
	res := <-uprconn.sreqch // Block until stream-request acknowleged from server
	switch {
	case res.Status == ROLLBACK && len(res.Extras) != 8:
		err = fmt.Errorf("invalid rollback %v\n", res.Extras)
		return 0, nil, err
	case res.Status == ROLLBACK:
		rollback := binary.BigEndian.Uint64(res.Extras)
		return rollback, nil, nil
	case res.Opaque != opaque:
		err = fmt.Errorf("unexpected stream request for vbucket %v", vbucket)
		return 0, nil, err
	}
	stream := &uprStream{
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
	feed.mu.Lock()
	feed.streams[vbucket] = stream
	feed.mu.Unlock()
	return 0, flogs, nil
}

//---- Local functions.

// connect with all servers holding data for `feed.bucket` and try
// reconnecting until `feed` is closed.
func (feed *UprFeed) run(name string, killSwitch chan bool) {
	var err error
	retryInterval := initialRetryInterval
	bucketOK := true
	for {
		// Connect to the UPR feed of each server node
		if bucketOK {
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
				retryInterval = initialRetryInterval
			}
		}

		// On error, try to refresh the bucket in case the list of nodes changed:
		log.Printf("go-couchbase: UPR connection lost; reconnecting to bucket %q in %v",
			feed.bucket.Name, retryInterval)
		err = feed.bucket.Refresh()
		bucketOK = err == nil

		select {
		case <-time.After(retryInterval):
		case <-feed.quit:
			return
		}
		if retryInterval *= 2; retryInterval > maximumRetryInterval {
			retryInterval = maximumRetryInterval
		}
		if bucketOK {
			err = feed.connectToNodes(name)
		}
	}
}

func (feed *UprFeed) doSession(killSwitch chan bool) {
loop:
	for {
		select {
		case mcevent := <-feed.msgch:
			uprconn := mcevent[0].(*uprConnection)
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
		feed.vbmap[vb].sreqch <- res
	case UPR_STREAM_END:
		res := req2res(req)
		vb := uint16(res.Opaque)
		feed.mu.Lock()
		delete(feed.streams, vb)
		feed.mu.Unlock()
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
	bySeqno := binary.BigEndian.Uint64(req.Extras[:8])
	e := UprEvent{
		Bucket:  feed.bucket.Name,
		Opstr:   eventTypes[req.Opcode],
		Vbucket: req.VBucket,
		Seqno:   bySeqno,
		Key:     req.Key,
		Value:   req.Body,
	}
	return e
}

func (feed *UprFeed) connectToNodes(name string) (err error) {
	b := feed.bucket
	feed.conns = make([]*uprConnection, 0)
	feed.vbmap = make(map[uint16]*uprConnection)
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

		uprconn := &uprConnection{
			host:   cp.Host(),
			conn:   conn,
			flogch: make(chan *mcd.MCResponse),
			sreqch: make(chan *mcd.MCResponse),
		}
		feed.conns = append(feed.conns, uprconn)
		// Collect vbuckets under this connection
		for vbno := range vbmaps {
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

func (feed *UprFeed) doRecieve(uprconn *uprConnection, ch chan<- []interface{}) {
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

// Close UprFeed. Does the opposite of StartUprFeed()
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
		close(uprconn.sreqch)
	}
	feed.vbmap = nil
	feed.conns = nil
}

func req2res(req *mcd.MCRequest) *mcd.MCResponse {
	return &mcd.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
		Opaque: req.Opaque,
		Status: mcd.Status(req.VBucket),
		Extras: req.Extras,
		Key:    req.Key,
		Body:   req.Body,
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
