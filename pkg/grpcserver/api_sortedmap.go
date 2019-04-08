package grpcserver

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

// CreateSM creates sorted map with given name.
func (g *GRPCServer) CreateSM(ctx context.Context, in *pb.SM_Name) (*pb.SM_Empty, error) {
	if in.Name == "" {
		log.Printf("CreateSM failed, name cannot be empty")
		return nil, status.Errorf(codes.InvalidArgument, "CreateSM failed, name must not be empty")
	}
	err := beyond.GetInstance().CreateSortedMap(in.Name)
	if err != nil {
		return nil, err
	}
	log.Printf("CreateSM succeed, %s", in.Name)
	return &pb.SM_Empty{}, nil // create successfully.
}

// DropSM drops sorted map with given name.
func (g *GRPCServer) DropSM(ctx context.Context, in *pb.SM_Name) (*pb.SM_Empty, error) {
	if in.Name == "" {
		log.Printf("DropSM failed, name must not be empty")
		return nil, status.Errorf(codes.InvalidArgument, "DropSM failed, name must not be empty")
	}
	err := beyond.GetInstance().DropSortedMap(in.Name)
	if err != nil {
		return nil, err
	}
	if _, ok := beyond.GetInstance().smmap[in.Name]; ok {
		delete(beyond.GetInstance().smmap, in.Name)
		log.Printf("DropSM succeed, '%s'", in.Name)
		return &pb.SM_Empty{}, nil // drop successfully.
	}
	log.Printf("DropSM abort, '%s' not found", in.Name)
	return nil, status.Errorf(codes.InvalidArgument, "sorted map '%s' not found in DropSM", in.Name)
}

// ListSM returns list of all sorted map names.
func (g *GRPCServer) ListSM(ctx context.Context, in *pb.SM_Empty) (*pb.SM_Names, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Printf("ListSM request received.")
	names := make([]string, 0, len(beyond.GetInstance().smmap))
	for n := range beyond.GetInstance().smmap {
		names = append(names, n)
	}
	log.Printf("ListSM returns %v", names)
	return &pb.SM_Names{Names: names}, nil
}

// SMLen gets length of given sorted map.
func (g *GRPCServer) SMLen(ctx context.Context, in *pb.SM_Name) (*pb.SM_Length, error) {
	log.Printf("SMLen request received. name='%s'", in.Name)
	if in.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "SMLen failed, name must not be empty")
	}
	if sm, ok := beyond.GetInstance().smmap[in.Name]; ok {
		length, err := sm.Len()
		if err == nil {
			return &pb.SM_Length{Length: length}, nil
		}
		return nil, status.Errorf(codes.Unknown, "SMLen failed, %s", err.Error())
	}
	return nil, status.Errorf(codes.InvalidArgument, "SMLen failed, sorted map '%s' not found", in.Name)
}

// SMPut puts key-value into given sorted map.
// edge case: value is nil. (so nil result means either a nil value or not exists).
func (g *GRPCServer) SMPut(ctx context.Context, in *pb.SM_NameKeyValueReplace) (*pb.SM_Value, error) {
	log.Printf("SMPut request received. name='%s'", in.Name)
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
func (g *GRPCServer) SMRemove(ctx context.Context, in *pb.SM_NameKey) (*pb.SM_Value, error) {
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

// SMOPStream put a stream of key-value into sorted map.
func (g *GRPCServer) SMOPStream(stream Pooh_SMOPStreamServer) error {
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
func (g *GRPCServer) SMIteratorStream(in *pb.SM_NameKeyReverseOffsetLimit, stream Pooh_SMIteratorStreamServer) error {
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

// SMTransaction do transaction of operations atomically.
func (g *GRPCServer) SMTransaction(ctx context.Context, in *pb.SM_NameTransaction) (*pb.SM_Empty, error) {
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
