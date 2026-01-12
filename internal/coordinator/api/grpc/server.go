package grpc

import (
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/config"
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
	cfg config.GRPCConfig,
	workerService core.WorkerService,
	jobService core.JobService,
	logger logging.Logger,
) *Server {
	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             cfg.KeepaliveMinTime,
			PermitWithoutStream: true,
		}),
	)

	proto.RegisterCoordinatorServiceServer(
		grpcServer,
		NewCoordinatorService(
			cfg.HeartbeatInterval,
			workerService,
			jobService,
			logger,
		),
	)

	if cfg.EnableReflection {
		reflection.Register(grpcServer)
	}

	return &Server{
		addr:       cfg.Addr,
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
