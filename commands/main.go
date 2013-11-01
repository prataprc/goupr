package main

import (
    "fmt"
    "log"
    "net"
)

var port *int = flag.Int("port", 11212, "Port on which to listen")

type chanReq struct {
    req *goupr.Request
    res chan *goupr.Response
}

func RunServer(input chan chanReq) {
    var s storage
    s.data = make(map[string]gomemcached.MCItem)
    for {
        req := <-input
        log.Printf("Got a request: %s", req.req)
        req.res <- dispatch(req.req, &s)
    }
}

func waitForConnections(ls net.Listener) {
    reqChannel := make(chan chanReq)

    go RunServer(reqChannel)
    handler := &reqHandler{reqChannel}

    log.Printf("Listening on port %d", *port)
    for {
        s, e := ls.Accept()
        if e == nil {
            log.Printf("Got a connection from %v", s.RemoteAddr())
            go memcached.HandleIO(s, handler)
        } else {
            log.Printf("Error accepting from %s", ls)
        }
    }
}

func main() {
    flag.Parse()
    ls, e := net.Listen("tcp", fmt.Sprintf(":%d", *port))
    if e != nil {
        log.Fatalf("Listen error:  %s", e)
    }
    waitForConnections(ls)
}

