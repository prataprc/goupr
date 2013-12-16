package goupr

type Stream struct {
    Client     *Client // Client connection handles this stream
    Vbucket    uint16  // vbucket id
    Vuuid      uint64  // vbucket uuid
    Opaque     uint32  // all messages from producer to this stream have same
    Log        [][2]uint64 // failoverlog
    OnCommand  StreamCallback
}

// Instantiate a new `Stream` structure. To request a new stream, call
// client.UprStream().
func (client *Client) NewStream(vbucket uint16, vuuid uint64,
    opaque uint32, callback StreamCallback) *Stream {

    stream := &Stream{
        Client:   client,
        Vuuid:    vuuid,
        Opaque:   opaque,
        OnCommand: callback,
    }
    return stream
}

