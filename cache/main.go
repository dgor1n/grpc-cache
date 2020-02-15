package main

import (
	"log"
	"net"

	pb "grpc-cache/proto"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

// Config ...
type Config struct {
	MaxTimeout       int
	MinTimeout       int
	NumberOfRequests int
	URLs             []string
}

func main() {
	log.Println(initConfig())

	listener, err := net.Listen("tcp", ":5300")

	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}

	opts := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(opts...)

	pb.RegisterStreamServer(grpcServer, &server{})
	grpcServer.Serve(listener)
}

func initConfig() *Config {

	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")      // optionally look for config in the working directory
	err := viper.ReadInConfig()   // Find and read the config file
	if err != nil {               // Handle errors reading the config file
		log.Fatal(err)
	}

	return &Config{
		MaxTimeout:       viper.GetInt("MaxTimeout"),
		MinTimeout:       viper.GetInt("MinTimeout"),
		NumberOfRequests: viper.GetInt("NumberOfRequests"),
		URLs:             viper.GetStringSlice("URLs"),
	}
}

type server struct{}

func (s *server) GetRandomDataStream(r *pb.Request, stream pb.Stream_GetRandomDataStreamServer) error {

	stream.Send(&pb.Response{
		Message: "Hey Jude",
	})

	return nil
}
