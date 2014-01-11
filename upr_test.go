package goupr

import (
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	mcd "github.com/dustin/gomemcached"
	mc "github.com/dustin/gomemcached/client"
	"testing"
	"time"
)

const TESTURL = "http://localhost:9000"

func TestUprOpen(t *testing.T) {
	bucket, err := getTestConnection("default")
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
	err = UprOpen(conn, name, uint32(0x1) /*flags*/)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUprRequestFailoverLog(t *testing.T) {
	bucket, err := getTestConnection("default")
	if err != nil {
		t.Fatal(err)
	}
	cp := bucket.GetConnPools()[0]
	conn, err := cp.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Return(conn)

	err = UprOpen(conn, "upr-unitest", uint32(0x1) /*flags*/)
	if err != nil {
		t.Fatal(err)
	}

	if err = RequestFailoverLog(conn, 0, 0x10); err != nil {
		t.Fatal(err)
	}

	if res, err := getResponse(conn); err != nil {
		t.Fatal(err)
	} else if flogs, err := parseFailoverLog(res.Body); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(flogs)
	}
}

func TestUprRequestStream(t *testing.T) {
	bucket, err := getTestConnection("default")
	if err != nil {
		t.Fatal(err)
	}
	cp := bucket.GetConnPools()[0]
	conn, err := cp.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Return(conn)

	err = UprOpen(conn, "upr-unitest", uint32(0x1) /*flags*/)
	if err != nil {
		t.Fatal(err)
	}

	err = RequestStream(conn, uint32(0) /*flags*/, 0x10, uint16(0), /*vbucket*/
		0 /*vuuid*/, 0 /*startSeqno*/, 0xFFFFFFFFFFFFFFFF, /*endSeqno*/
		0 /*highSeqno*/)
	if err != nil {
		t.Fatal(err)
	}

	if res, err := getResponse(conn); err != nil {
		t.Fatal(err)
	} else if res.Status != mcd.SUCCESS {
		t.Fatal(fmt.Errorf("Request stream returned with %v status", res.Status))
	}
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

func getResponse(conn *mc.Client) (mcd.MCResponse, error) {
	var hdr [mcd.HDR_LEN]byte
	var pkt mcd.MCResponse
	mcconn := conn.GetMC()
	err := pkt.Receive(mcconn, hdr[:])
	return pkt, err
}
