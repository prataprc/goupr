// session between producer and consumer.

package goupr

import (
    "io"
    "log"
    "sync"
    "github.com/couchbaselabs/go-couchbase"
    mcd "github.com/dustin/gomemcached"
    mc "github.com/dustin/gomemcached/client"
)

const (
    OPEN_OPAQUE     uint32 = 0xDADADADA
    RESPONSE_BUFFER int    = 10
)

type Callback func(*Client)
type ClientCallback func(*Client, *mcd.MCRequest)
type StreamCallback func(*Stream, *mcd.MCRequest)

type Client struct {
    bucket  *couchbase.Bucket // bucket name for this connection
    conn    *mc.Client        // client side socket
    name    string            // name of the connection
    healthy bool              // whether the connection is still valid

    // application side callback when the connection is closed
    onClose Callback

    // application side callback when the connection timeouts
    onTimeout Callback

    // application side callback when connection level callbacks like
    // UPR_ADD_STREAM, UPR_STREAM_END
    onStream ClientCallback

    // if received message is a response to a client request
    // pass them to this channel
    response chan *mcd.MCResponse

    // A map of `Opaque` value to Stream consumer.
    streams map[uint32]*Stream

    sync.RWMutex        // lock to access `streams` field.
}

// Create a new consumer instance.
func NewClient(bucket *couchbase.Bucket,
    onClose, onTimeout Callback, onStream ClientCallback) *Client {

    client := Client{
        bucket:    bucket,
        healthy:   false,
        streams:   make(map[uint32]*Stream),
        onClose:   onClose,
        onTimeout: onTimeout,
        onStream:  onStream,
        response:  make(chan *mcd.MCResponse),
    }
    return &client
}

// Connect to producer identified by `port` and `dest`.
// - `open` and `name` the connection.
func (client *Client) Connect(dest, name string) (err error) {
    if client.conn, err = defaultMkConn(client.bucket, dest); err != nil {
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
    var err error
    var res *mcd.MCResponse
    for {
        // We generally call the message as request.
        if res, err = client.conn.Receive(); err != nil {
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
        //client.Close()
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

func Request(res *mcd.MCResponse) *mcd.MCRequest {
    req := mcd.MCRequest {
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
