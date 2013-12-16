package main

import (
    "flag"
    "fmt"
    "github.com/couchbaselabs/go-couchbase"
    "log"
)

var options struct {
    host    string
    port    int
    pools   bool
    nodes   bool
    buckets bool
}

func argParse() {
    flag.StringVar(&options.host, "host", "localhost", "Port to connect")
    flag.IntVar(&options.port, "port", 11212, "Host to connect")
    flag.BoolVar(&options.pools, "pools", false, "Show the list of pools")
    flag.BoolVar(&options.nodes, "nodes", false, "Show the list of nodes")
    flag.BoolVar(&options.buckets, "buckets", false, "Show the list of buckets")
    flag.Parse()
}

func main() {
    argParse()

    url := fmt.Sprintf("http://Administrator:asdasd@%v:%v",
        options.host, options.port)

    // Couchbase client, pool and default bucket
    couch, err := couchbase.Connect(url)
    if err != nil {
        log.Fatalf("Error connecting:  %v", err)
    }

    pool, err := couch.GetPool("default")
    if err != nil {
        log.Fatalf("Error getting pool:  %v", err)
    }

    _, err = pool.GetBucket("default")
    if err != nil {
        log.Fatalf("Error getting bucket:  %v", err)
    }

    if options.pools {
        showPools(&couch)
    }
    if options.nodes {
        showNodes(&couch, &pool)
    }
    if options.buckets {
        showBuckets(&couch, &pool)
    }
}

func showPools(couch *couchbase.Client) {
    info := couch.Info
    fmt.Printf("(admin:%v) (UUID:%v) %v\n",
        info.IsAdmin, info.UUID, info.ImplementationVersion)
    fmt.Println("ComponentVersion :")
    for k, v := range info.ComponentsVersion {
        fmt.Printf("  %v : %v\n", k, v)
    }
    for _, restpool := range info.Pools {
        fmt.Printf("RestPool--`%v`:\n", restpool.Name)
        fmt.Println("  ", restpool.StreamingURI)
        fmt.Println("  ", restpool.URI)
        fmt.Println()
    }
}

func showNodes(couch *couchbase.Client, pool *couchbase.Pool) {
    for _, node := range pool.Nodes {
        fmt.Printf("%+v\n", node)
    }
}

func showBuckets(couch *couchbase.Client, pool *couchbase.Pool) {
    for name, bucket := range pool.BucketMap {
        fmt.Printf("(%v)\n", name)
        fmt.Printf("  %+v\n", bucket)
        fmt.Println()
    }
}
