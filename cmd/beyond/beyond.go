package main

import (
	"beyond/pkg/grpcservice"
	"beyond/pkg/restapiservice"
	"flag"
	"log"
	_ "net/http/pprof"
	"strconv"
	"sync"
)

func main() {
	// parse parameters
	grpcport := flag.Int("gport", 7777, "grpc server port")
	restport := flag.Int("rport", 7778, "restapi port")
	flag.Parse()

	// set log format
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Println("hello, beyond stars")

	var wg sync.WaitGroup

	// start grpc service.
	wg.Add(1)
	gs := grpcservice.NewGRPCService(":" + strconv.Itoa(*grpcport))
	go gs.Run(&wg)

	// start rest server.
	wg.Add(1)
	rs := restapiservice.NewRESTAPIService(":" + strconv.Itoa(*restport))
	go rs.Run(&wg)

	// wait grpc and restapi service exit.
	wg.Wait()
}
