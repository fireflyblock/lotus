package miner

import (
	"context"
	"github.com/filecoin-project/sector-storage/grpc/config"
	"github.com/filecoin-project/sector-storage/grpc/proto"
	pb "github.com/filecoin-project/sector-storage/grpc/proto"
	logging "github.com/ipfs/go-log/v2"
	"google.golang.org/grpc"
	"time"
)

const MsgSize = 4 * 1024 * 1024 * 7

var log = logging.Logger("miner_worker")

func init() {
	config.Init()
}

func C2RPC(phase1Out []byte, Number uint64, Miner uint64) (*pb.Reply, error) {
	log.Info("c2 grpc start")
	address := config.C.GRPC.IP + config.C.GRPC.Port
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MsgSize)))
	if err != nil {
		log.Infof("did not connect: %v", err)
		return &pb.Reply{}, err
	}
	defer conn.Close()
	c := pb.NewC2Client(conn)

	ctx := context.Background()

	r, err := c.C2Client(ctx, &pb.Request{
		ActorID:      Miner,
		SectorNumber: Number,
		Commit1Out:   phase1Out,
	})
	return r, err
}

func ConnectTest() {
	address := config.C.GRPC.IP + config.C.GRPC.Port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Errorf("did not connect: %v", err)
		panic(err)
	}

	defer conn.Close()
	c := proto.NewConnectClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.ConnectTest(ctx, &proto.ConnectRequest{})
	if err != nil {
		log.Errorf("could not request: %v", err)
		panic(err)
	}
	if r.Message {
		log.Infof("Connect service successful, grpc: %s%s\n", config.C.GRPC.IP, config.C.GRPC.Port)
	} else {
		panic("Can not connect service")
	}
}
