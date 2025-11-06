package grpc

import (
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/nemanja-m/gomr/internal/coordinator/service"
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
	workerService service.WorkerService,
	logger logging.Logger,
) *Server {
	grpcServer := grpc.NewServer()

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

	s.logger.Info("Coordinator gRPC server started", "address", s.addr)

	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
