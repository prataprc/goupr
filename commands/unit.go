package main

import (
    "log"
    "strconv"
    "fmt"
    "flag"
    "github.com/prataprc/goupr"
)

var host *string = flag.String("host", "localhost", "Port to connect")
var port *int = flag.Int("port", 11212, "Host to connect")

func main() {
    flag.Parse()
    dest := *host + ":" + strconv.Itoa(*port)
    if client, err := goupr.Connect("tcp", dest, "indexer"); err == nil {
        client.SetOnClose(func(client *goupr.Client) {
            log.Println("Connection", dest, "closed")
        })
        getStream(client)
    } else {
        log.Println("Not able to connect with", dest, ":", err)
    }
}

func getStream(client *goupr.Client) {
    req := goupr.NewRequest(0, 0, 0, uint16(0))
    if stream, _, err := client.UprStream(req, 0, 0, 0, 0, 0); err == nil {
        res := <-stream.Consumer
        for len(res) > 0  {
            res := <-stream.Consumer
            fmt.Println(res)
        }
    } else {
        log.Println("Error opening stream")
    }
}
