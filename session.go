// session between consumer and producer.

package goupr

import (
	"github.com/couchbaselabs/go-couchbase"
	mcd "github.com/dustin/gomemcached"
	mc "github.com/dustin/gomemcached/client"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	OPEN_OPAQUE     uint32 = 0xDADADADA
	RESPONSE_BUFFER int    = 10
)

type Client struct {
	bucket  *couchbase.Bucket // upr client for bucket.
	host    string            // host to connect with
	name    string            // name of the connection
	conn    *mc.Client        // client side socket
	healthy bool              // whether the connection is still valid
	eventch chan *StreamEvent

	// A map of `Opaque` value to Stream consumer.
	streams      map[uint32]*Stream
	sync.RWMutex // lock to access `streams` field.

	// when received message, by doRecieve(), is a response to a client request
	// pass them to this channel
	response chan *mcd.MCResponse

	// automatically restart the connection and open streams when a disconnect
	// happens in the connection
	autoRestart bool
	backoff     time.Duration
}

// Create a new consumer instance.
func NewClient(bucket *couchbase.Bucket, eventch chan *StreamEvent) *Client {
	return &Client{
		bucket:   bucket,
		healthy:  false,
		eventch:  eventch,
		streams:  make(map[uint32]*Stream),
		response: make(chan *mcd.MCResponse),
		backoff:  1,
	}
}

// Connect to producer identified by `port` and `dest`.
// - `open` and `name` the connection.
func (client *Client) Connect(dest, name string, restart bool) (err error) {
	if client.conn, err = defaultMkConn(client.bucket, dest); err != nil {
		return
	}
	client.healthy = true
	client.name = name
	req := NewRequest(0, 0, OPEN_OPAQUE, 0)
	flags := uint32(0x1)
	go doRecieve(client) // Don't reshuffle this line
	client.UprOpen(req, name, 0, flags)
	client.host, client.name = dest, name

	streams := make(map[uint32]*Stream)
	for opaque, stream := range client.streams {
		streams[opaque] = stream
	}
	for restart && (len(streams) > 0) { // Whether to restart the streams
		log.Printf("There are %v streams to restart\n", len(streams))
		time.Sleep(client.backoff * time.Second)
		client.backoff <<= 1
		for opaque, stream := range streams {
			log.Println("Trying to reconnect", opaque)
			if err = client.restartStream(stream); err != nil {
				log.Println("ERROR: restarting stream :", err)
			} else {
				delete(streams, opaque)
			}
		}
	}
	return
}

func (client *Client) AutoRestart(what bool) {
	client.autoRestart = what
}

func (client *Client) IsHealthy() bool {
	return client.healthy
}

// Hang-up
func (client *Client) Close() (ret bool) {
	if client.healthy {
		client.conn.Close()
	}
	ret, client.healthy = client.healthy, false
	return ret
}

// Go routine that will wait for new messages from producer and handle them.
func doRecieve(client *Client) {
	var err error
	var res *mcd.MCResponse
	for {
		// We generally call the message as request.
		if res, err = client.conn.Receive(); err != nil {
			// TODO: temporary fix. because UPR_MUTATION gives non SUCCESS status
			if _, ok := err.(*mcd.MCResponse); !ok {
				if client.tryError(err) {
					break
				}
			}
		}

		switch res.Opcode {
		case UPR_OPEN, UPR_FAILOVER_LOG, UPR_STREAM_REQ:
			client.response <- res

		case UPR_STREAM_END:
			stream := client.getStream(res.Opaque)
			if stream.autoRestart {
				stream.healthy = false
				go client.restartStream(stream)
			} else {
				client.evictStream(res.Opaque)
			}

		case UPR_MUTATION, UPR_DELETION:
			stream := client.getStream(res.Opaque)
			if res.Opaque > 0 && stream != nil {
				if stream.IsHealthy() {
					stream.Handlecommand(mkRequest(res))
				} else {
					stream.Queuecommand(mkRequest(res))
				}
			} else {
				log.Println("ERROR: un-known opaque received %v", res)
			}

		case UPR_CLOSE_STREAM:
			panic("UPR_CLOSE_STREAM not yet implemented")

		case UPR_SNAPSHOTM, UPR_EXPIRATION, UPR_FLUSH, UPR_ADD_STREAM:
			panic("UPR_ADD_STREAM not yet implemented")

		default:
			log.Println("ERROR: un-known opcode received %v", res)
		}
	}

	if client.autoRestart { // auto restart this connection
		log.Printf("Client %v attempting autorestart", client.name)
		client.Connect(client.host, client.name, true)
	}
}

func (client *Client) tryError(err error) bool {
	if err == io.EOF {
		log.Printf("ERROR: Connection closed by remote %v", client.name)
		client.Close()
		return true
	} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		log.Println("ERROR: Connection timeout ...")
		client.Close()
		return true
	} else if err != nil {
		log.Println("ERROR: Unknown connection error :")
	}
	return false
}

func mkRequest(res *mcd.MCResponse) *mcd.MCRequest {
	req := mcd.MCRequest{
		Opcode:  res.Opcode,
		Cas:     res.Cas,
		Opaque:  res.Opaque,
		VBucket: uint16(res.Status),
		Extras:  res.Extras,
		Key:     res.Key,
		Body:    res.Body,
	}
	return &req
}

func defaultMkConn(b *couchbase.Bucket, h string) (*mc.Client, error) {
	if conn, err := mc.Connect("tcp", h); err != nil {
		return nil, err
	} else {
		if _, err = conn.Auth(b.Name, ""); err != nil {
			conn.Close()
			return nil, err
		}
		return conn, nil
	}
}
