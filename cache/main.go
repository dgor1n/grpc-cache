package main

import (
	"log"
	"math/rand"
	"net"
	"time"

	pb "github.com/dgor1n/grpc-cache/proto"

	"github.com/go-redis/redis"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

type server struct {
	MaxTimeout       int
	MinTimeout       int
	NumberOfRequests int
	URLs             []string

	Redis *redis.Client
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

	// Init redis connection.
	client := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("Redis.host") + ":" + viper.GetString("Redis.port"),
		Password: viper.GetString("Redis.password"),
		DB:       viper.GetInt("Redis.db"),
	})

	_, err = client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	s.Redis = client

	rand.Seed(time.Now().Unix()) // Initialize global pseudo random generator.
}

func (s *server) GetRandomDataStream(r *pb.Request, stream pb.Stream_GetRandomDataStreamServer) error {

	defer log.Println("Done")

	ch := make(chan string)

	for i := 0; i < s.NumberOfRequests; i++ {
		go s.processData(ch)
	}

	for i := 0; i < s.NumberOfRequests; i++ {
		message := <-ch
		stream.Send(&pb.Response{
			Message: message,
		})
	}

	return nil
}

func (s *server) processData(ch chan<- string) {

	url := s.URLs[rand.Intn(len(s.URLs))]

	pid, err := s.Redis.Get(url).Result()
	if err == redis.Nil {
		ttl := (rand.Intn(s.MaxTimeout-s.MinTimeout+1) + s.MinTimeout) * 1000 * 1000 * 1000
		if err := s.Redis.SetNX(url, "pid", time.Duration(ttl)).Err(); err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}

	//if !res {
	//	url, err = s.Redis.Get(url).Result()

	//	log.Println("url from redis", url)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//}

	log.Println(url, pid)
	ch <- url
}
