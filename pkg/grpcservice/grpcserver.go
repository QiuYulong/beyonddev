package grpcservice

// gRPC server.

import (
	"beyond/pkg/beyond"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	pb "beyond/grpc"
	"sync"
)

// GRPCService defines the GPRC Server.
type GRPCService struct {
	address string
	server *grpc.Server
}

// NewGRPCService create GPRCServer instance.
func NewGRPCService(address string) *GRPCService {
	return &GRPCService{
		address: address,
		server: grpc.NewServer(),
	}
}

// Run GRPCService. it's blocking call.
func (g *GRPCService) Run(wg *sync.WaitGroup) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	lis, err := net.Listen("tcp", g.address)
	if err != nil {
		log.Fatalf("grpc service failed to listen on %v: %v", g.address, err)
	}
	pb.RegisterBeyondServer(g.server, g)
	log.Printf("start grpc service on %s", g.address)
	go func(){
		if err := g.server.Server(lis); err != nil {
			log.Fatalf("grpc server failed to serve: %v", err)
		}
	}()
	sig := <- sigs
	log.Printf("signal %v received, gracefully shutting down grpc service", sig)
	g.server.GracefulStop()
	wg.Done()
}
