package docker

import (
	"net"

	"github.com/filecoin-project/sector-storage/docker/service"

	"github.com/filecoin-project/sector-storage/grpc/config"

	logging "github.com/ipfs/go-log/v2"

	pb "github.com/filecoin-project/sector-storage/grpc/proto"
	"google.golang.org/grpc"
)

func init() {
	config.Init()
}

var log = logging.Logger("docker_worker")

var options = []grpc.ServerOption{
	grpc.MaxRecvMsgSize((4 * 1024 * 1024) * 7),
	grpc.MaxSendMsgSize((4 * 1024 * 1024) * 7),
}

func Init() {
	log.Info("grpc server docker worker start!")
	service.Registered()

	port := config.C.LOCALHOST.Port

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
	}
	s := grpc.NewServer(options...)
	pb.RegisterC2Server(s, &service.Server{})
	pb.RegisterConnectServer(s, &service.ConnectServer{})
	if err := s.Serve(lis); err != nil {
		log.Errorf("failed to serve: %v", err)
	}
}
