// session between producer and consumer.

package goupr

import (
    "github.com/dustin/gomemcached"
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

type Callback func(*Client)
type ClientCallback func(*Client, *gomemcached.MCRequest)
type StreamCallback func(*Stream, *gomemcached.MCRequest)


type Client struct {
    conn    net.Conn // client side socket
    name    string   // name of the connection
    healthy bool     // whether the connection is still valid

    // application side callback when the connection is closed
    onClose Callback

    // application side callback when the connection timeouts
    onTimeout Callback

    // application side callback when connection level callbacks like
    // UPR_ADD_STREAM, UPR_STREAM_END
    onStream ClientCallback

    // if received message is a response to a client request
    // pass them to this channel
    response chan *gomemcached.MCResponse

    // A map of `Opaque` value to Stream consumer.
    streams map[uint32]*Stream

    sync.RWMutex        // lock to access `streams` field.
}

// Create a new consumer instance.
func NewClient(onClose, onTimeout Callback, onStream ClientCallback) *Client {
    client := Client{
        healthy:   false,
        streams:   make(map[uint32]*Stream),
        onClose:   onClose,
        onTimeout: onTimeout,
        onStream:  onStream,
        response:  make(chan *gomemcached.MCResponse),
    }
    return &client
}

// Connect to producer identified by `port` and `dest`.
// - `open` and `name` the connection.
func (client *Client) Connect(protocol, dest string, name string) (err error) {
    if client.conn, err = net.Dial(protocol, dest); err != nil {
        return err
    }
    client.healthy = true
    client.name = name
    req := NewRequest(0, 0, OPEN_OPAQUE, 0)
    flags := uint32(0x1)
    go doRecieve(client)
    err = client.UprOpen(req, name, 0, flags)
    return
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
        case UPR_ADD_STREAM:
            go client.onStream(client, Request(res))
        case UPR_STREAM_END:
            client.evictStream(res.Opaque)
            go client.onStream(client, Request(res))
        case UPR_CLOSE_STREAM:
            panic("UPR_CLOSE_STREAM not yet implemented")
        case UPR_SNAPSHOTM, UPR_MUTATION, UPR_DELETION, UPR_EXPIRATION, UPR_FLUSH:
            stream := client.getStream(res.Opaque)
            if res.Opaque > 0 && stream != nil {
                go stream.OnCommand(stream, Request(res))
            } else {
                log.Printf("un-known message received %v", res)
            }
        default:
            log.Printf("un-known opcode received %v", res)
        }
    }
}

func (client *Client) tryError(err error) bool {
    // TODO: Check whether the err is because of timeout.
    if err == io.EOF {
        client.Close()
        return true
    }
    return false
}

// Operations to client structure.
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

func Request(res *gomemcached.MCResponse) *gomemcached.MCRequest {
    req := gomemcached.MCRequest {
        Opcode: res.Opcode,
        Cas: res.Cas,
        Opaque: res.Opaque,
        VBucket: uint16(res.Status),
        Extras: res.Extras,
        Key: res.Key,
        Body: res.Body,
    }
    return &req
}
