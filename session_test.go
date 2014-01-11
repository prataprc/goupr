package goupr

import (
	"fmt"
	"testing"
	"time"
)

func TestFailoverLog(t *testing.T) {
	bucket, err := getTestConnection("beer-sample")
	if err != nil {
		t.Fatal(err)
	}
	cp := bucket.GetConnPools()[0]
	conn, err := cp.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Return(conn)

	name := fmt.Sprintf("%v", time.Now().UnixNano())
	feeds, err := StartUprFeed(bucket, name)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		rollb, flogs, err := feeds.StartStream(uint16(i), 0, 0, 0, 0)
		fmt.Println("vb", i, rollb, flogs, err)
	}
}

func TestFeed(t *testing.T) {
	bucket, err := getTestConnection("beer-sample")
	if err != nil {
		t.Fatal(err)
	}
	cp := bucket.GetConnPools()[0]
	conn, err := cp.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Return(conn)

	name := fmt.Sprintf("%v", time.Now().UnixNano())
	feeds, err := StartUprFeed(bucket, name)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := 0; i < 8; i++ {
			rollb, flogs, err :=
				feeds.StartStream(uint16(i), 0, 0, 0xFFFFFFFFFFFFFFFF, 0)
			fmt.Println("vb", i, rollb, flogs, err)
		}
	}()
	for {
		e := <-feeds.C
		fmt.Println(e.Opstr, e.Vbucket, e.Seqno, string(e.Key))
	}
}
