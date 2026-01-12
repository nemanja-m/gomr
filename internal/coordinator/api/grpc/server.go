package grpc

import (
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/logging"
	"github.com/nemanja-m/gomr/internal/shared/proto"
)

type Server struct {
	proto.UnimplementedCoordinatorServiceServer

	addr       string
	grpcServer *grpc.Server
	logger     logging.Logger
}

func NewServer(
	addr string,
	enableReflection bool,
	workerService core.WorkerService,
	logger logging.Logger,
) *Server {
	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	proto.RegisterCoordinatorServiceServer(grpcServer, NewCoordinatorService(workerService, logger))

	if enableReflection {
		reflection.Register(grpcServer)
	}

	return &Server{
		addr:       addr,
		grpcServer: grpcServer,
		logger:     logger,
	}

}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
