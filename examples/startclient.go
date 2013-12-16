package main

import (
	"github.com/prataprc/goupr"
	"log"
)

func main() {
	client, err := goupr.Connect("tcp", "everest", "indexer")
	if err != nil {
		log.Panicln("Couldnot connect")
	}

	// Get the vuuids and corresponding seqNo from where the indexer switched
	// out. indexer.Logs() returns [][2]uint64, where each element in the
	// slice is a tuple of two uint64 specifying vuuid and seqNo
	for vbucket, vseq := range make([][2]uint64, 10) { //indexer.Logs() {
		go runStream(client, vbucket, vseq[0], vseq[1])
	}
}

// Start a new stream for vuuid and seqNo
func runStream(client *goupr.Client, vbucket int, ivuuid, iseqNo uint64) {
	req := goupr.NewRequest(0, 0, 0, uint16(vbucket))
	flogs, err := client.UprFailOverLog(req)
	if err != nil {
		log.Panicln(err)
	}

	// We do not handle the scenario of re-building the index.
	req = goupr.NewRequest(0, 0, 0, uint16(vbucket))
	vuuid, highVUUID, highSeqNo := uint64(0), ivuuid, iseqNo
	for _, flog := range flogs {
		if flog[0] == ivuuid { // Pick matching vuuid from failover-logs
			vuuid = ivuuid
			break
		}
		if flog[1] > highSeqNo { // Or the highest sequence number
			highVUUID, highSeqNo = flog[0], flog[1]
		}
	}

	var stream *goupr.Stream
	var rollb, seqNo uint64
	if vuuid == 0 {
		vuuid, seqNo = highVUUID, highSeqNo
	} else {
		seqNo = iseqNo
	}

	// In either case, try to get a stream for (vuuid,seqNo)
	for {
		stream, rollb, err = client.UprStream(req, 0, seqNo, 0, vuuid, seqNo)
		if stream == nil && err == nil && rollb > 0 {
			seqNo = rollb
			continue
		} else if err != nil {
			log.Panicln(err)
		} else if stream != nil {
			break
		}
	}

	// Notify indexer about the sequence number that we will start receiving.
	for {
		_ = <-stream.Consumer // Start receiving stream commands.
		// Handle UPR commands.
	}
}
