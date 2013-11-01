// session between producer and consumer.

package goupr

import (
    "log"
    "net"
    "sync"
    "time"
)

const (
    OPEN_OPAQUE uint32 = 0xDADADADA
    RESPONSE_BUFFER int = 10
)

type Client struct {
    conn      net.Conn // client side socket
    name      string   // name of the connection
    uuid      []byte   // connection-uuid, used while closing the connection.
    healthy   bool
                  // whether the connection is still valid
    onClose   func(*Client)
                  // application side callback when the connection is closed
    onTimeout func(*Client)
                  // application side callback when the connection timeouts
    response  chan *Response
                  // if received message is a response to a client request
                  // pass them to this channel
    streams   map[uint32]*Stream
                  // A map of `Opaque` value to Stream consumer.
    sync.RWMutex        // lock to access `streams` field.
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
        onClose: nil,
        onTimeout: nil,
        healthy: true,
        response: make(chan *Response, RESPONSE_BUFFER),
    }
    req := NewRequest(0, 0, OPEN_OPAQUE, 0)
    err = client.UprOpen(req, name, 0, 0)
    if err == nil {
        go doRecieve(&client)
    }
    return &client, err
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
// client.UprOpen().
func (client *Client) NewStream(vuuid uint64,
    opaque uint32, consumer chan []interface{}) *Stream {

    stream := &Stream {
        client: client,
        vuuid: vuuid,
        opaque: opaque,
        consumer: consumer,
    }
    return stream
}

// Hang-up
func (client *Client) Close() bool {
    if client.healthy {
        client.conn.Close()
        client.healthy = false
        return true
    }
    if client.onClose != nil {
        client.onClose(client)
    }
    return false
}

// Go routine that will simple wait for new messages from producer and handler
// them.
func doRecieve(client *Client) {
    for {
        hdr := make([]byte, HDR_LEN)
        // We generally call the message as request.
        req := &Request{}
        if err := (&req).Receive(client, hdr); err != nil {
            if client.tryError(err) {
                break
            }
        }

        // Get the stream, if available.
        client.RLock()
        stream := client.streams[req.Opaque]
        client.RUnlock()

        switch req.Opcode {
        // message is a response to one of the following client request.
        case UPR_OPEN:
        case UPR_FAILOVER_LOG:
        case UPR_STREAM_REQ:
            res := &Response{
                Opcode: req.Opcode,
                Opaque: req.Opaque,
                Cas: req.Cas,
                Status: Status(req.VBucket),
                Extras: req.Extras,
                Key: req.Key,
                Body: req.Body,
            }
            client.response <- res
        // Otherwise let the stream or client handle it.
        default:
            if req.Opaque != 0 && stream != nil {
                go stream.handle(req)
            } else {
                go client.handle(req)
            }
        }
    }
}

func (client *Client) handle(req *Request) {
    log.Panicln("Unknown request", req)
    // TODO: Fill this up
}

func (client *Client) tryError(err error) bool {
    // TODO: Check whether the err is because the server has closed the
    // connection.
    // TODO: Check whether the err is because of timeout.
    client.healthy = false
    return false
}
