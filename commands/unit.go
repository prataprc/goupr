package main

import (
    "flag"
    "github.com/couchbaselabs/go-couchbase"
    "github.com/dustin/gomemcached/client"
    "github.com/dustin/gomemcached"
    "github.com/prataprc/goupr"
    "log"
    "time"
    "strconv"
)

var host *string = flag.String("host", "localhost", "Port to connect")
var port *int = flag.Int("port", 11212, "Host to connect")

func main() {
    flag.Parse()
    // dest := *host + ":" + strconv.Itoa(*port)

    // Couchbase client, pool and default bucket
    couch, err := couchbase.Connect("http://" + *host +":"+ strconv.Itoa(*port))
    if err != nil {
        log.Fatalf("Error connecting:  %v", err)
    }

    pool, err := couch.GetPool("default")
    if err != nil {
        log.Fatalf("Error getting pool:  %v", err)
    }

    bucket, err := pool.GetBucket("users")
    if err != nil {
        log.Fatalf("Error getting bucket:  %v", err)
    }


    args := memcached.TapArguments{
        Dump: false,
        SupportAck: false,
        KeysOnly: false,
        Checkpoint: true,
        ClientName: "indexer",
    }
    if feed, err := bucket.StartTapFeed(&args); err != nil {
        log.Println(err)
    } else {
        for {
            log.Printf("%+v", <-feed.C)
        }
    }

    // openStreams(bucket.VBSMJson.ServerList, bucket.VBSMJson.VBucketMap)
    for {
        time.Sleep(100 * time.Millisecond)
    }
}

func openStreams(servers []string, vbmaps [][]int) {
    nodes := make(map[string]*goupr.Client)
    for _, node := range servers {
        client := goupr.NewClient(onClose, onTimeout, onStream)
        if err := client.Connect("tcp", node, "indexer"); err != nil {
            log.Println("Not able to connect with", node, ":", err)
        } else {
            log.Println("Connected to", node, "...")
        }
        nodes[node] = client
    }

    opaqCount := uint32(1)
    for vbucket, maps := range vbmaps {
        client := nodes[servers[maps[0]]]
        req := goupr.NewRequest(0, 0, opaqCount, uint16(vbucket))
        flags := uint32(0)
        vuuid, high, start := uint64(0), uint64(0), uint64(0)
        end := uint64(0xFFFFFFFFFFFFFFFF)
        log.Println("opening stream for vbucket", vbucket)
        _, _, err := client.UprStream(req, flags, start, end, vuuid, high, onCommand)
        if err != nil {
            log.Println(err)
        }
    }
}

func onClose(client *goupr.Client) {
    log.Println("Recevied on close !!")
}

func onTimeout(client *goupr.Client) {
    log.Println("Recevied on timeout !!")
}

func onStream(client *goupr.Client, req *gomemcached.MCRequest) {
    switch req.Opcode {
    case goupr.UPR_STREAM_END:
        log.Printf("Closing stream %v", req.Opaque)
    default:
        log.Printf("onStream:", req.Opcode, req.Opaque)
    }
}

func onCommand(stream *goupr.Stream, req *gomemcached.MCRequest) {
    log.Printf("%+v", req)
}
