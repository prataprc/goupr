package goupr

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	mcd "github.com/dustin/gomemcached"
	mc "github.com/dustin/gomemcached/client"
	"log"
	"time"
)

// exponential backoff while connection retry.
const initialRetryInterval = 1 * time.Second
const maximumRetryInterval = 30 * time.Second

// uprConnection contains an active memcached connection.
type uprConnection struct {
	host string // host to which `mc` is connected.
	conn *mc.Client
}

var eventTypes = map[mcd.CommandCode]string{ // Refer UprEvent
	UPR_MUTATION: "UPR_MUTATION",
	UPR_DELETION: "UPR_DELETION",
}

// connect with all servers holding data for `feed.bucket` and try reconnecting
// until `feed` is closed.
func (feed *UprFeed) doSession(uprconns []*uprConnection) {
	var q bool

	msgch := make(chan []interface{})
	for {
		if feed.bucket.Refresh() == nil {
			killSwitch := make(chan bool)
			for _, uprconn := range uprconns {
				go doReceive(uprconn, uprconn.host, msgch, killSwitch)
			}
			q = feed.doEvents(msgch)
			close(killSwitch)
			if q == false {
				uprconns, q = feed.retryConnections(uprconns)
			}
		}
		if q {
			break
		}
	}
	closeConnections(uprconns)
	close(feed.C)
}

func (feed *UprFeed) retryConnections(
	uprconns []*uprConnection) ([]*uprConnection, bool) {

	var err error

	log.Println("Retrying connections ...")
	retryInterval := initialRetryInterval
	for {
		closeConnections(uprconns)
		uprconns, err = connectToNodes(feed.bucket, feed.name)
		if err == nil {
			feed.vbmap = vbConns(feed.bucket, uprconns)
			feed.streams, err = startStreams(feed.streams, feed.vbmap)
			if err == nil {
				return uprconns, false
			}
		}
		log.Printf("Retrying after %v seconds ...", retryInterval)
		select {
		case <-time.After(retryInterval):
		case <-feed.quit:
			return nil, true
		}
		if retryInterval *= 2; retryInterval > maximumRetryInterval {
			retryInterval = maximumRetryInterval
		}
	}
	return uprconns, false
}

func (feed *UprFeed) doEvents(msgch chan []interface{}) bool {
	for {
		select {
		case mcevent, ok := <-msgch:
			if !ok {
				return false
			}
			host := mcevent[0].(string)
			if err, ok := mcevent[1].(error); ok {
				log.Printf("doSession: Received error from %v: %v\n", host, err)
				return false
			} else if req, ok := mcevent[1].(mcd.MCRequest); ok {
				if err := handleUprMessage(feed, &req); err != nil {
					log.Println(err)
					return false
				}
			}
		case <-feed.quit:
			return true
		}
	}
}

func handleUprMessage(feed *UprFeed, req *mcd.MCRequest) (err error) {
	vb := uint16(req.Opaque)
	stream := feed.streams[uint16(req.Opaque)]
	switch req.Opcode {
	case UPR_STREAM_REQ:
		rollb, flog, err := handleStreamResponse(req2res(req))
		if err == nil {
			if flog != nil {
				stream.Flog = flog
			} else if rollb > 0 {
				uprconn := feed.vbmap[vb]
				log.Println("Requesting a rollback for %v to sequence %v",
					vb, rollb)
				flags := uint32(0)
				err = requestStream(
					uprconn.conn, flags, req.Opaque, vb, stream.Vuuid,
					rollb, stream.Endseq, stream.Highseq)
			}
		}
	case UPR_MUTATION, UPR_DELETION:
		e := feed.makeUprEvent(req)
		stream.Startseq = e.Seqno
		feed.C <- e
	case UPR_STREAM_END:
		res := req2res(req)
		err = fmt.Errorf("Stream %v is ending", uint16(res.Opaque))
	case UPR_SNAPSHOTM:
	case UPR_CLOSE_STREAM, UPR_EXPIRATION, UPR_FLUSH, UPR_ADD_STREAM:
		err = fmt.Errorf("Opcode %v not implemented", req.Opcode)
	default:
		err = fmt.Errorf("ERROR: un-known opcode received %v", req)
	}
	return
}

func handleStreamResponse(res *mcd.MCResponse) (uint64, FailoverLog, error) {
	var rollback uint64
	var err error
	var flog FailoverLog

	switch {
	case res.Status == ROLLBACK && len(res.Extras) != 8:
		err = fmt.Errorf("invalid rollback %v\n", res.Extras)
	case res.Status == ROLLBACK:
		rollback = binary.BigEndian.Uint64(res.Extras)
	case res.Status != mcd.SUCCESS:
		err = fmt.Errorf("Unexpected status %v", res.Status)
	}
	if err == nil {
		if rollback > 0 {
			return rollback, flog, err
		} else {
			flog, err = parseFailoverLog(res.Body[:])
		}
	}
	return rollback, flog, err
}

func connectToNodes(b *couchbase.Bucket, name string) ([]*uprConnection, error) {
	var conn *mc.Client
	var err error

	uprconns := make([]*uprConnection, 0)
	for _, cp := range b.GetConnPools() {
		if cp == nil {
			return nil, fmt.Errorf("go-couchbase: no connection pool")
		}
		if conn, err = cp.Get(); err != nil {
			return nil, err
		}
		// A connection can't be used after UPR; Dont' count it against the
		// connection pool capacity
		//<-cp.createsem
		cp.Createsem()
		if uprconn, err := connectToNode(b, conn, name, cp.Host()); err != nil {
			return nil, err
		} else {
			uprconns = append(uprconns, uprconn)
		}
	}
	return uprconns, nil
}

func connectToNode(b *couchbase.Bucket, conn *mc.Client,
	name, host string) (uprconn *uprConnection, err error) {
	if err = uprOpen(conn, name, uint32(0x1) /*flags*/); err != nil {
		return
	}
	uprconn = &uprConnection{
		host: host,
		conn: conn,
	}
	log.Println("Connected to host:", host)
	return
}

func vbConns(b *couchbase.Bucket,
	uprconns []*uprConnection) map[uint16]*uprConnection {

	servers, vbmaps := b.VBSMJson.ServerList, b.VBSMJson.VBucketMap
	vbconns := make(map[uint16]*uprConnection)
	for _, uprconn := range uprconns {
		// Collect vbuckets under this connection
		for vbno := range vbmaps {
			host := servers[vbmaps[int(vbno)][0]]
			if uprconn.host == host {
				vbconns[uint16(vbno)] = uprconn
			}
		}
	}
	return vbconns
}

func startStreams(streams map[uint16]*UprStream,
	vbmap map[uint16]*uprConnection) (map[uint16]*UprStream, error) {

	for vb, uprconn := range vbmap {
		stream := streams[vb]
		flags := uint32(0)
		err := requestStream(
			uprconn.conn, flags, uint32(vb), vb, stream.Vuuid,
			stream.Startseq, stream.Endseq, stream.Highseq)
		if err != nil {
			return nil, err
		}
		log.Printf(
			"Posted stream request for vbucket %v from %v\n",
			vb, stream.Startseq)
	}
	return streams, nil
}

func doReceive(uprconn *uprConnection, host string,
	msgch chan []interface{}, killSwitch chan bool) {

	var hdr [mcd.HDR_LEN]byte
	var msg []interface{}
	var pkt mcd.MCRequest
	var err error

	mcconn := uprconn.conn.GetMC()

loop:
	for {
		if err = pkt.Receive(mcconn, hdr[:]); err != nil {
			msg = []interface{}{host, err}
		} else {
			msg = []interface{}{host, pkt}
		}
		fmt.Println(msg)
		select {
		case msgch <- msg:
		case <-killSwitch:
			break loop
		}
	}
	fmt.Println("doReceive: exiting", host)
	return
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

func (feed *UprFeed) hasQuit() bool {
	select {
	case <-feed.quit:
		return true
	default:
		return false
	}
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

func closeConnections(uprconns []*uprConnection) {
	for _, uprconn := range uprconns {
		log.Println("Closing connection for:", uprconn.host)
		if uprconn.conn != nil {
			uprconn.conn.Close()
		}
		uprconn.conn = nil
	}
}
