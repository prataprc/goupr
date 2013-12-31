package main

import (
    "flag"
    "time"
    "github.com/couchbaselabs/go-couchbase"
    mc "github.com/dustin/gomemcached"
    "github.com/prataprc/goupr"
    "log"
    "fmt"
    "strconv"
)

var host *string = flag.String("host", "localhost", "Port to connect")
var port *int = flag.Int("port", 11211, "Host to connect")

var opaqCount uint32 = 1
type BucketStreams struct {
    name    string
    bucket  *couchbase.Bucket
    clients map[string]*goupr.Client
    streams map[string][]*goupr.Stream
}

func main() {
    flag.Parse()
    dest := *host + ":" + strconv.Itoa(*port)

    // Couchbase client, pool and default bucket
    couch, err := couchbase.Connect("http://" + dest)
    if err != nil {
        log.Fatalf("Error connecting:  %v", err)
    }

    pool, err := couch.GetPool("default")
    if err != nil {
        log.Fatalf("Error getting pool:  %v", err)
    }

    bstreams := make([]BucketStreams, 0)
    for name, _ := range pool.BucketMap {
        bucket, _ := pool.GetBucket(name)
        if name == "gamesim-sample" {
            var val map[string]interface{}
            bucket.Get("Aaron0", &val)
            clients := connectNodes(bucket)
            bs := BucketStreams{
                name: name,
                bucket: bucket,
                clients: clients,
                streams: openStreams(bucket, clients),
            }
            bstreams = append(bstreams, bs)
            go populate(bucket)
        }
    }
    log.Println(bstreams)
    for {
    }
}

func connectNodes(bucket *couchbase.Bucket) map[string]*goupr.Client {
    servers := bucket.VBSMJson.ServerList
    nodes := make(map[string]*goupr.Client)
    for _, hostname := range servers {
        log.Println(hostname)
        client := goupr.NewClient(bucket, onClose, onTimeout, onStream)
        if err := client.Connect(hostname, "indexer"); err != nil {
            log.Println("Not able to connect with", hostname, ":", err)
        } else {
            log.Println("Connected to", hostname, "...")
        }
        nodes[hostname] = client
    }
    return nodes
}

func openStreams(bucket *couchbase.Bucket,
    nodes map[string]*goupr.Client) map[string][]*goupr.Stream {

    servers, vbmaps := bucket.VBSMJson.ServerList, bucket.VBSMJson.VBucketMap
    streams := make(map[string][]*goupr.Stream)
    vbs_highseq_no := bucket.GetStats("vbucket-seqno")
    for vbucket, maps := range vbmaps {
        key := fmt.Sprintf("vb_%v_high_seqno", vbucket)
        server := servers[maps[0]]
        client := nodes[server]
        req := goupr.NewRequest(0, 0, opaqCount, uint16(vbucket))
        flags := uint32(0)
        vuuid, high, start := uint64(0), uint64(0), uint64(0)
        strconv.Atoi(vbs_highseq_no[server][key])
        end := uint64(0xFFFFFFFFFFFFFFFF)
        if stream, _, err := client.UprStream(
            req, flags, start, uint64(end), vuuid, high, onCommand); err != nil {
            log.Println("Error opening stream for vbucket", vbucket, err)
        } else {
            log.Println("Opened a stream for vbucket", vbucket)
            ls := streams[servers[maps[0]]] 
            if ls == nil {
                ls = make([]*goupr.Stream, 0)
            }
            streams[servers[maps[0]]] = append(ls, stream)
        }
        opaqCount += 1
    }
    return streams
}

func onClose(client *goupr.Client) {
    log.Println("Recevied on close !!")
}

func onTimeout(client *goupr.Client) {
    log.Println("Recevied on timeout !!")
}

func onStream(client *goupr.Client, req *mc.MCRequest) {
    switch req.Opcode {
    case goupr.UPR_STREAM_END:
        log.Printf("Closing stream %v", req.Opaque)
    default:
        log.Printf("onStream:", req.Opcode, req.Opaque)
    }
}

var mutations = 0
func onCommand(stream *goupr.Stream, req *mc.MCRequest) {
    if req.Opcode == goupr.UPR_MUTATION {
        mutations += 1
    }
    log.Println("command for stream,", req.Opcode, stream.Vbucket, stream.Vuuid,
        stream.Opaque, mutations)
}

//---- populate
func populate(bucket *couchbase.Bucket) {
    log.Println(bucket.VBServerMap())
    for {
        key := "key" + strconv.Itoa(time.Now().Nanosecond())
        value := "value" + strconv.Itoa(time.Now().Nanosecond())
        fmt.Println(key)
        bucket.Set(key, 0, value)
        time.Sleep(4 * time.Millisecond)
    }
}
