// session between producer and consumer.

package goupr

import (
    "log"
    "io"
    "net"
    "sync"
    "time"
    "github.com/dustin/gomemcached"
)

const (
    OPEN_OPAQUE uint32 = 0xDADADADA
    RESPONSE_BUFFER int = 10
)

type Client struct {
    conn      net.Conn // client side socket
    name      string   // name of the connection
    uuid      []byte   // connection-uuid, used while closing the connection.
    healthy   bool     // whether the connection is still valid
    onClose   func(*Client)
                    // application side callback when the connection is closed
    onTimeout func(*Client)
                    // application side callback when the connection timeouts
    response  chan *gomemcached.MCResponse
                    // if received message is a response to a client request
                    // pass them to this channel
    streams   map[uint32]*Stream
                    // A map of `Opaque` value to Stream consumer.
    opqCount  uint32 // Opaque counter.
    sync.RWMutex     // lock to access `streams` field.
}

// Connect to producer identified by `port` and `dest`.
// - `name` the connection.
// - launch a goroutine to recieve data for producer.
func Connect(protocol, dest string, name string) (*Client, error) {
    conn, err := net.Dial(protocol, dest)
    if err != nil {
        return nil, err
    }
    client := Client{
        conn: conn,
        healthy: true,
        streams: make(map[uint32]*Stream),
        opqCount: 1,
        onClose: nil,
        onTimeout: nil,
        response: make(chan *gomemcached.MCResponse),
    }
    go doRecieve(&client)
    req := NewRequest(0, 0, OPEN_OPAQUE, 0)
    // TODO: Sequence no. can be ZERO ?
    flags := uint32(0x1)
    if err := client.UprOpen(req, name, 0, flags); err == nil {
        return &client, err
    } else {
        return nil, err
    }
}

// When `client` detects that the other end has hung up, an optional `onClose`
// callback can be issued.
func (client *Client) SetOnClose(onClose func(*Client)) bool {
    if client.healthy {
        client.onClose = onClose
        return true
    }
    return false
}

// When `client` detects that socket operation failed because of a timeout,
// an optional `onTimeout` callback can be issued.
func (client *Client) SetOnTimeout(onTimeout func(*Client)) bool {
    if client.healthy {
        client.onTimeout = onTimeout
        return true
    }
    return false
}

// Set timeout for socket read operation.
func (client *Client) SetReadDeadline(t time.Time) error {
    if client.healthy {
        return client.conn.SetReadDeadline(t)
    }
    return nil
}

// Set timeout for socket write operation.
func (client *Client) SetWriteDeadline(t time.Time) error {
    if client.healthy {
        return client.conn.SetWriteDeadline(t)
    }
    return nil
}

// Instantiate a new `Stream` structure. To request a new stream, call
// client.UprStream().
func (client *Client) NewStream(vuuid uint64,
    opaque uint32, consumer chan []interface{}) *Stream {

    stream := &Stream {
        Client: client,
        Vuuid: vuuid,
        Opaque: opaque,
        Consumer: consumer,
    }
    return stream
}

// Hang-up
func (client *Client) Close() (ret bool) {
    if client.healthy {
        client.conn.Close()
    }
    if client.onClose != nil {
        client.onClose(client)
    }
    close(client.response)
    ret, client.healthy = client.healthy, false
    return ret
}

// Go routine that will wait for new messages from producer and handle them.
func doRecieve(client *Client) {
    for {
        hdr := make([]byte, gomemcached.HDR_LEN)
        // We generally call the message as request.
        res := &gomemcached.MCResponse{}
        if err := (&res).Receive(client.conn, hdr); err != nil {
            if client.tryError(err) {
                break
            }
        }

        switch res.Opcode {
        case UPR_OPEN, UPR_FAILOVER_LOG, UPR_STREAM_REQ:
            client.response <- res
        default: // Otherwise let the stream or client handle it.
            stream := client.getStream(res.Opaque)
            if res.Opaque > 0 && stream != nil {
                go stream.handle(res)
            } else {
                go client.handle(res)
            }
        }
    }
}

func (client *Client) handle(res *gomemcached.MCResponse) {
    log.Panicln("Unknown response", res)
    // TODO: Fill this up
}

func (client *Client) tryError(err error) bool {
    // TODO: Check whether the err is because of timeout.
    if err == io.EOF {
        client.Close()
        return true
    }
    return false
}

// Operations to stream structure.
func (client *Client) addStream(opaque uint32, stream *Stream) {
    client.Lock()
    defer client.Unlock()
    client.streams[opaque] = stream
}

func (client *Client) getStream(opaque uint32) (stream *Stream) {
    client.RLock()
    defer client.RUnlock()
    stream = client.streams[opaque]
    return stream
}

func (client *Client) evictStream(opaque uint32) {
    client.Lock()
    defer client.Unlock()
    delete(client.streams, opaque)
}
