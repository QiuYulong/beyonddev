package grpcservice

import (
	"beyond/pkg/beyond"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	pb "beyond/grpc"
)

// List list all data structures' name & type in beyond.
func (g *GRPCService) List(in *pb.Empty, stream pb.Beyond_ListServer) error {
	ds := beyond.GetBeyond().List()
	for _, nametype := range ds {
		err := stream.Send(&pb.NameType{
			Name: nametype[0],
			Type: nametype[1],
		})
		if err != nil {
			msg := fmt.Sprintf("failed to send List stream, nametype is %v", nametype)
			log.Println(msg)
			return status.Errorf(codes.Unknown, msg)
		}
	}
	return nil
}