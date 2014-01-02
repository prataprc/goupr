package goupr

import (
	"encoding/binary"
	"fmt"
	mcd "github.com/dustin/gomemcached"
)

type Stream struct {
	Client      *Client // Client connection handles this stream
	Vbucket     uint16  // vbucket id
	Vuuid       uint64  // vbucket uuid
	Opaque      uint32  // all messages from producer to this stream have same
	Highseq     uint64
	Startseq    uint64
	Endseq      uint64
	Log         [][2]uint64      // failoverlog
	autoRestart bool             // auto restart the stream when producer ends it.
	healthy     bool             // whether the stream is active
	queue       []*mcd.MCRequest // a queue of request on the stream.
}

type StreamEvent struct {
	Bucket  string
	Opstr   string
	Vbucket uint16
	SeqNo   uint64
	Key     []byte
	Value   []byte
}

var uprop2strop = map[mcd.CommandCode]string{
	UPR_MUTATION: "INSERT",
	UPR_DELETION: "DELETE",
}

// Instantiate a new `Stream` structure. To request a new stream, call
// client.UprStream().
func (client *Client) NewStream(vbucket uint16, vuuid uint64,
	opaque uint32) *Stream {

	stream := &Stream{
		Client:      client,
		Vuuid:       vuuid,
		Opaque:      opaque,
		autoRestart: false,
		healthy:     false,
		queue:       make([]*mcd.MCRequest, 0),
	}
	return stream
}

func (client *Client) restartStream(stream *Stream) (err error) {
	vbucket, vuuid := stream.Vbucket, stream.Vuuid
	req := NewRequest(0, 0, stream.Opaque, vbucket)
	high, start, end := stream.Highseq, stream.Startseq, stream.Endseq
	flags := uint32(0)

	stream, _, err = client.UprStream(req, flags, start, end, vuuid, high)
	if err == nil {
		stream.healthy = true
	} else {
		stream.healthy = false
		err = fmt.Errorf("Unable to restart upr stream:", err)
	}
	return
}

// Adding, evicting and the obtaining stream from client strucuture based on
// Opaque-id.
func (client *Client) addStream(opaque uint32, stream *Stream) {
	client.Lock()
	client.streams[opaque] = stream
	stream.healthy = true
	defer client.Unlock()
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

func (stream *Stream) AutoRestart(what bool) {
	stream.autoRestart = what
}

func (stream *Stream) IsHealthy() bool {
	return stream.healthy
}

func (stream *Stream) Queuecommand(req *mcd.MCRequest) {
	stream.queue = append(stream.queue, req)
}

func (stream *Stream) Handlecommand(req *mcd.MCRequest) {
	if len(stream.queue) > 0 {
		for _, oldreq := range stream.queue {
			stream.handleCommand(oldreq)
		}
		stream.queue = make([]*mcd.MCRequest, 0)
	}
	stream.handleCommand(req)
}

func (stream *Stream) handleCommand(req *mcd.MCRequest) {
	by_seqno := binary.BigEndian.Uint32(req.Extras[:4])
	rev_seqno := binary.BigEndian.Uint32(req.Extras[4:8])
	fmt.Println(by_seqno, rev_seqno)
	stream.Client.eventch <- &StreamEvent{
		Bucket:  stream.Client.bucket.Name,
		Opstr:   uprop2strop[req.Opcode],
		Vbucket: stream.Vbucket,
		SeqNo:   uint64(rev_seqno),
		Key:     req.Key,
		Value:   req.Body,
	}
}
