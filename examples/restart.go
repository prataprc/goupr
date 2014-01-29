package main

import (
	"fmt"
	"github.com/prataprc/go-couchbase"
	"github.com/prataprc/goupr"
	"log"
	"time"
)

var vbcount = 8

const TESTURL = "http://localhost:9000"

// Flush the bucket before trying this program
func main() {
	// get a bucket and mc.Client connection
	bucket, err := getTestConnection("default")
	if err != nil {
		panic(err)
	}
	cp := bucket.GetConnPools()[0]
	conn, err := cp.Get()
	if err != nil {
		panic(err)
	}
	defer cp.Return(conn)

	// start upr feed
	feed, err := goupr.StartUprFeed(bucket, "index" /*name*/, nil)
	if err != nil {
		panic(err)
	}

	// add mutations to the bucket
	var mutationCount = 100
	addKVset(bucket, mutationCount)

	vbseqNo := receiveMutations(feed, mutationCount)
	for vbno, seqno := range vbseqNo {
		stream := feed.GetStream(uint16(vbno))
		if stream.Startseq != seqno {
			panic(fmt.Errorf(
				"For vbucket %v, stream seqno is %v, received is %v",
				vbno, stream.Startseq, seqno))
		}
	}

	streams := make(map[uint16]*goupr.UprStream, 0)
	for vb := range vbseqNo {
		stream := feed.GetStream(uint16(vb))
		streams[uint16(vb)] = stream
		stream.Vuuid = stream.Flog[0][0]
	}
	feed.Close()

	feed, err = goupr.StartUprFeed(bucket, "index" /*name*/, streams)
	if err != nil {
		panic(err)
	}

	bucket.Refresh()
	bucket.Set("newkey", 0, "new mutations")
	bucket.Set("newkey1", 0, "new mutations")
	bucket.Set("newkey2", 0, "new mutations")

	fmt.Println(10)
	e := <-feed.C
	fmt.Println(e.Opstr, e.Seqno, string(e.Key), string(e.Value))

	//if e.Seqno != startSeq+1 {
	//    err := fmt.Errorf("Expected seqno %v, received %v", startSeq+1, e.Seqno)
	//    panic(err)
	//}
	//if string(e.Key) != string(key) {
	//    err := fmt.Errorf("Expected key %v, received %v", string(key), string(e.Key))
	//    panic(err)
	//}
	//if string(e.Value) != newvalue {
	//    err := fmt.Errorf("Expected value %v, received %v", newvalue, string(e.Value))
	//    panic(err)
	//}
	feed.Close()
}

func addKVset(b *couchbase.Bucket, count int) {
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key%v", i)
		value := fmt.Sprintf("Hello world%v", i)
		if err := b.Set(key, 0, value); err != nil {
			panic(err)
		}
	}
}

func receiveMutations(feed *goupr.UprFeed, mutationCount int) []uint64 {
	var vbseqNo = make([]uint64, vbcount)
	var mutations = 0
	var e goupr.UprEvent
loop:
	for {
		select {
		case e = <-feed.C:
		case <-time.After(time.Second):
			break loop
		}
		if vbseqNo[e.Vbucket] == 0 {
			vbseqNo[e.Vbucket] = e.Seqno
		} else if vbseqNo[e.Vbucket]+1 == e.Seqno {
			vbseqNo[e.Vbucket] = e.Seqno
		} else {
			log.Printf(
				"sequence number for vbucket %v, is %v, expected %v",
				e.Vbucket, e.Seqno, vbseqNo[e.Vbucket]+1)
		}
		mutations += 1
	}
	count := 0
	for _, seqNo := range vbseqNo {
		count += int(seqNo)
	}
	if count != mutationCount {
		panic(fmt.Errorf("Expected %v mutations", mutationCount))
	}
	return vbseqNo
}

func getTestConnection(bucketname string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect(TESTURL)
	if err != nil {
		fmt.Println("Make sure that couchbase is at", TESTURL)
		return nil, err
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketname)
	return bucket, err
}
