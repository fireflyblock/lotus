package service

import (
	"context"
	"net"
	"os"
	"time"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/grpc/config"
	"github.com/filecoin-project/sector-storage/grpc/proto"
	pb "github.com/filecoin-project/sector-storage/grpc/proto"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"google.golang.org/grpc"
)

var log = logging.Logger("docker_worker")

var c2 = &ffiwrapper.Sealer{}

type Server struct {
	proto.UnimplementedC2Server
}

type ConnectServer struct {
	proto.ConnectServer
}

func (c ConnectServer) ConnectTest(ctx context.Context, in *proto.ConnectRequest) (*proto.ConnectReply, error) {
	return &proto.ConnectReply{
		Message: true,
	}, nil
}

func (s *Server) C2Client(ctx context.Context, in *proto.Request) (*proto.Reply, error) {
	log.Info("machine Received")
	message, err := c2.SealCommit2(ctx, abi.SectorID{
		Miner:  abi.ActorID(in.ActorID),
		Number: abi.SectorNumber(in.SectorNumber),
	}, in.Commit1Out)

	if err != nil {
		log.Errorf("c2 func err: %s", err.Error())
		return &pb.Reply{
			Message: message,
			Error:   err.Error(),
		}, err
	}

	log.Info("c2 func success")
	return &pb.Reply{
		Message: message,
		Error:   "",
	}, nil
}

func Registered() {
	var ip string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Errorf("get machine ip failed, err: %s", err.Error())
		panic(err)
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = ipnet.IP.String()
				break
			}
		}
	}
	address := config.C.GRPC.IP + config.C.GRPC.Port
	hostName, err := os.Hostname()
	if err != nil {
		log.Errorf("get machine hostname failed: %s", err.Error())
		panic(err)
	}
connect:
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Errorf("did not connect: %s", err.Error())
		time.Sleep(1 * time.Second)
		goto connect
	}
	defer conn.Close()
	c := proto.NewMachineClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	log.Infof("machine ip --- %s port --- %s host --- %s", ip, config.C.LOCALHOST.Port, hostName)
	r, err := c.Registered(ctx, &proto.RegisteredRequest{
		IP:   ip,
		Port: config.C.LOCALHOST.Port,
		Host: hostName,
	})

	if err != nil {
		log.Errorf("could not request: %s", err.Error())
		time.Sleep(1 * time.Second)
		goto connect
	}

	if !r.Message {
		time.Sleep(1 * time.Second)
		goto connect
	}

	log.Info("machine connect success! ")
}
