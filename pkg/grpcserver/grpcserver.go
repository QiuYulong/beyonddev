package grpcserver

// gRPC server.

import (
	"google.golang.org/grpc"
	"log"
	"net"
	pb "beyond/grpc"
)

// GRPCServer defines the GPRC Server.
type GRPCServer struct {
	address string
	server *grpc.Server
}

// NewGPRCServer create GPRCServer instance.
func NewGPRCServer(address string) *GRPCServer {
	return &GRPCServer{
		address: address,
		server: grpc.NewServer(),
	}
}

// Start GRPCServer. it's blocking call.
func (g *GRPCServer) Start() {
	lis, err := net.Listen("tcp", g.address)
	if err != nil {
		log.Fatalf("grpc server failed to listen on %v: %v", g.address, err)
	}
	pb.RegisterBeyondServer(g.server, g)
	if err := g.server.Server(lis); err != nil {
		log.Fatalf("grpc server failed to serve: %v", err)
	}
}

// Stop GRPCServer.
func (g *GRPCServer) Stop() {
	g.server.Stop()
}