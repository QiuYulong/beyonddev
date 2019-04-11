package grpcservice

import (
	"beyond/pkg/beyond"
	"beyond/pkg/ds"
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	pb "beyond/grpc"
)

// SMCreate creates sorted map with given name.
func (g *GRPCService) SMCreate(ctx context.Context, in *pb.SM_Name) (*pb.Empty, error) {
	if in.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "CreateSM failed, name must not be empty")
	}
	err := beyond.GetInstance().CreateSortedMap(in.Name)
	if err != nil {
		return nil, err
	}
	log.Printf("SMCreate succeed, %s", in.Name)
	return &pb.Empty{}, nil // create successfully.
}

// SMDrop drops sorted map with given name.
func (g *GRPCService) SMDrop(ctx context.Context, in *pb.SM_Name) (*pb.Empty, error) {
	if in.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "DropSM failed, name must not be empty")
	}
	err := beyond.GetInstance().DropSortedMap(in.Name)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}
	log.Printf("SMDrop success, %s", in.Name)
	return &pb.Empty{}, nil // drop successfully.
}

// SMLen gets length of given sorted map.
func (g *GRPCService) SMLen(ctx context.Context, in *pb.SM_Name) (*pb.SM_Length, error) {
	if in.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "SMLen failed, name must not be empty")
	}
	sm, err := beyond.GetInstance().GetSortedMap(in.Name)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}
	return &pb.SM_Length{Length: sm.Len()}, nil
}

// SMPut puts key-value into given sorted map.
// edge case: value is nil. (so nil result means either a nil value or not exists).
func (g *GRPCService) SMPut(ctx context.Context, in *pb.SM_NameKeyValue) (*pb.Empty, error) {
	if in.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "SMPut failed, name must not be empty")
	}
	if in.Key == nil || len(in.Key) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "SMPut failed, key must not be empty")
	}
	sm, ok := beyond.GetInstance().smmap[in.Name]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "SMPut failed, sorted map '%s' not found", in.Name)
	}
	// sorted map Put.
	var value []byte
	var err error
	if in.Replace {
		value, err = sm.Put(in.Key, in.Value)
	} else {
		value, err = sm.PutIfAbsent(in.Key, in.Value)
	}
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "SMPut failed, %s", err.Error())
	}
	return &pb.SM_Value{Value: value}, nil
}

// SMRemove removes key from given sorted map.
// edge case: value is nil. (so nil result means either a nil value or not exists).
func (g *GRPCService) SMRemove(ctx context.Context, in *pb.SM_NameKey) (*pb.Empty, error) {
	log.Printf("SMRemove request received. name='%s'", in.Name)
	if in.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "SMRemove failed, name must not be empty")
	}
	if in.Key == nil || len(in.Key) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "SMRemove failed, key must not be empty")
	}
	sm, ok := beyond.GetInstance().smmap[in.Name]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "SMRemove failed, sorted map '%s' not found", in.Name)
	}
	value, err := sm.Remove(in.Key)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "SMRemove failed, %s", err.Error())
	}
	return &pb.SM_Value{Value: value}, nil
}

// SMTransaction do transaction of operations atomically.
func (g *GRPCService) SMTransaction(ctx context.Context, in *pb.SM_NameTransaction) (*pb.Empty, error) {
	log.Printf("SMTransaction request received. name='%s'", in.Name)
	if in.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "SMTransaction failed, name must not be empty")
	}
	sm, ok := beyond.GetInstance().smmap[in.Name]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "SMTransaction failed, sorted map '%s' not found", in.Name)
	}
	ops := make([][3][]byte, 0, len(in.Op))
	for _, smOp := range in.Op {
		ops = append(ops, [3][]byte{
			smOp.Op,
			smOp.Key,
			smOp.Value,
		})
	}
	err := sm.Transaction(ops)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "SMTransaction failed, %v", err)
	}
	return &pb.SM_Empty{}, nil
}

// SMOPStream put a stream of key-value into sorted map.
func (g *GRPCService) SMOPStream(stream pb.Beyond_SMOPStreamServer) error {
	for {
		nokv, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.SM_Empty{})
		}
		if err != nil {
			return status.Errorf(codes.Unknown, "SMOPStream failed to receive from client, %v", err)
		}
		if nokv.Name == "" {
			return status.Errorf(codes.InvalidArgument, "SMOPStream failed, name must not be empty")
		}
		if nokv.Op == nil || len(nokv.Op) == 0 {
			return status.Errorf(codes.InvalidArgument, "SMOPStream failed, op must not be empty")
		}
		if nokv.Key == nil || len(nokv.Key) == 0 {
			return status.Errorf(codes.InvalidArgument, "SMOPStream failed, key must not be empty")
		}
		sm, ok := beyond.GetInstance().smmap[nokv.Name]
		if !ok {
			return status.Errorf(codes.InvalidArgument, "SMOPStream failed, sorted map '%s' not found", nokv.Name)
		}
		switch opName := nokv.Op[0]; opName {
		case ds.OPPUT:
			_, err = sm.Put(nokv.Key, nokv.Value)
			if err != nil {
				return status.Errorf(codes.Unknown, "SMOPStream failed, '%v'", err)
			}
		case ds.OPPUTIFABSENT:
			_, err = sm.PutIfAbsent(nokv.Key, nokv.Value)
			if err != nil {
				return status.Errorf(codes.Unknown, "SMOPStream failed, '%v'", err)
			}
		case ds.OPREMOVE:
			_, err = sm.Remove(nokv.Key)
			if err != nil {
				return status.Errorf(codes.Unknown, "SMOPStream failed, '%v'", err)
			}
		default:
			return status.Errorf(codes.InvalidArgument, "SMOPStream failed, invalid operation %v", nokv.Op)
		}
	}
}

// SMIteratorStream returns a stream of key-value with the ceil conditions.
func (g *GRPCService) SMIteratorStream(in *pb.SM_NameKeyForwardOffsetLimit, stream pb.Beyond_SMIteratorStreamServer) error {
	log.Printf("SMIterator request received. name='%s', key='%v', reverse='%v', offset='%v', length='%v'", in.Name, in.Key, in.Reverse, in.Offset, in.Limit)
	if in.Name == "" {
		return status.Errorf(codes.InvalidArgument, "SMIterator failed, name must not be empty")
	}
	if in.Key == nil {
		return status.Errorf(codes.InvalidArgument, "SMIterator failed, key must not be empty")
	}
	sm, ok := beyond.GetInstance().smmap[in.Name]
	if !ok {
		return status.Errorf(codes.InvalidArgument, "SMIterator failed, sorted map '%s' not found", in.Name)
	}
	it, err := sm.Iterator(in.Key, in.Reverse, in.Offset, in.Limit)
	if err != nil {
		return status.Errorf(codes.Unknown, "SMIterator failed to get iterator, %v", err)
	}
	for k, v, err := it(); !(k == nil && v == nil && err == nil); k, v, err = it() {
		err2 := stream.Send(&pb.SM_KeyValue{
			Key:   k,
			Value: v,
		})
		if err2 != nil {
			return status.Errorf(codes.Unknown, "SMIterator failed to send key-value to client, %v", err2)
		}
	}
	return nil
}