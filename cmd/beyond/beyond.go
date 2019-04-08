package main

import (
	"beyond/pkg/grpcserver"
	"log"
)

func main() {
	log.Println("hello, beyond stars")
	gs := grpcserver.NewGPRCServer("7777")
	gs.Start()
}
