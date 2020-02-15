package main

import (
	"log"
	"math/rand"
	"net"
	"time"

	pb "grpc-cache/proto"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

type server struct {
	MaxTimeout       int
	MinTimeout       int
	NumberOfRequests int
	URLs             []string
}

func main() {

	listener, err := net.Listen("tcp", ":5300")
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}

	opts := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(opts...)

	srv := &server{}
	srv.initConfig()

	pb.RegisterStreamServer(grpcServer, srv)
	grpcServer.Serve(listener)
}

func (s *server) initConfig() {

	viper.SetConfigName("config") // Name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")      // Optionally look for config in the working directory
	err := viper.ReadInConfig()   // Find and read the config file
	if err != nil {
		log.Fatal(err)
	}

	s.MaxTimeout = viper.GetInt("MaxTimeout")
	s.MinTimeout = viper.GetInt("MinTimeout")
	s.NumberOfRequests = viper.GetInt("NumberOfRequests")
	s.URLs = viper.GetStringSlice("URLs")
}

func (s *server) GetRandomDataStream(r *pb.Request, stream pb.Stream_GetRandomDataStreamServer) error {

	rand.Seed(time.Now().Unix()) // Initialize global pseudo random generator.

	for i := 0; i < s.NumberOfRequests; i++ {
		stream.Send(&pb.Response{
			Message: s.URLs[rand.Intn(len(s.URLs))],
		})
	}

	return nil
}
