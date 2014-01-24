package main

import (
    "log"
    "github.com/couchbaselabs/go-couchbase"
    "github.com/prataprc/goupr"
)

const TESTURL = "http://localhost:9000"
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

    // Get failover log for a vbucket
    flogs, err := goupr.GetFailoverLogs(bucket, "failoverlog")
    if err != nil {
        panic(err)
    }
    for vbno, flog := range flogs {
        log.Printf("Failover logs for vbucket %v: %v", vbno, flog)
    }
}

func getTestConnection(bucketname string) (*couchbase.Bucket, error) {
    couch, err := couchbase.Connect(TESTURL)
    if err != nil {
        log.Println("Make sure that couchbase is at", TESTURL)
        return nil, err
    }
    pool, err := couch.GetPool("default")
    if err != nil {
        return nil, err
    }
    bucket, err := pool.GetBucket(bucketname)
    return bucket, err
}
