package goupr

import (
    "testing"
    "github.com/couchbaselabs/go-couchbase"
    mcd "github.com/dustin/gomemcached"
    mc "github.com/dustin/gomemcached/client"
    "fmt"
    "time"
)

const URL = "http://localhost:9000"

func TestUprOpen(t *testing.T) {
    conn, err := getConnection()
    if err != nil {
        t.Fatal(err)
    }

    name := fmt.Sprintf("%v", time.Now().UnixNano())
    err = UprOpen(conn, name, uint32(0x1) /*flags*/)
    if err != nil {
        t.Fatal(err)
    }
}

func TestUprRequestFailoverLog(t *testing.T) {
    conn, err := getConnection()
    if err != nil {
        t.Fatal(err)
    }

    if err = RequestFailoverLog(conn, 0, 0); err != nil {
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
    conn, err := getConnection()
    if err != nil {
        t.Fatal(err)
    }

    err = RequestStream(conn, uint32(0) /*flags*/, 0x10,
        0 /*vuuid*/, 0 /*startSeqno*/, 0 /*endSeqno*/, 0 /*highSeqno*/)
    if err != nil {
        t.Fatal(err)
    }

    if res, err := getResponse(conn); err != nil {
        t.Fatal(err)
    } else if res.Status != mcd.SUCCESS {
        t.Fatal(fmt.Errorf("Request stream returned with %v status", res.Status))
    }
}

func getConnection() (*mc.Client, error) {
    couch, err := couchbase.Connect(URL)
    if err != nil {
        fmt.Println("Make sure that couchbase is at", URL)
        return nil, err
    }
    pool, err := couch.GetPool("default")
    if err != nil {
        return nil, err
    }
    bucket, err := pool.GetBucket("default")
    cp := bucket.GetConnPools()
    if len(cp) < 1 {
        return nil, fmt.Errorf("Empty connection pool")
    }
    if conn, err := cp[0].Get(); err != nil {
        return nil, err
    } else {
        return conn, err
    }
}

func getResponse(conn *mc.Client) (mcd.MCResponse, error) {
    var hdr [mcd.HDR_LEN]byte
    var pkt mcd.MCResponse
    mcconn := conn.GetMC()
    err := pkt.Receive(mcconn, hdr[:])
    return pkt, err
}
