package main

import (
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"time"

	pb "github.com/dgor1n/grpc-cache/proto"

	"github.com/go-redis/redis"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

const queue string = "q"

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

	srv := initServer()

	pb.RegisterStreamServer(grpcServer, srv)
	grpcServer.Serve(listener)
}

// Initialize server configuration.
func initServer() *server {

	s := &server{}

	// Init configuration.
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

	return s
}

// GetRandomDataStream starts URL processing and
// returns result to the client.
func (s *server) GetRandomDataStream(r *pb.Request, stream pb.Stream_GetRandomDataStreamServer) error {

	ch := make(chan string)

	for i := 0; i < s.NumberOfRequests; i++ {
		go s.processURL(ch)
	}

	for i := 0; i < s.NumberOfRequests; i++ {
		message := <-ch
		if err := stream.Send(&pb.Response{Message: message}); err != nil {
			return err
		}
	}

	return nil
}

func (s *server) processURL(ch chan<- string) {

	url := s.URLs[rand.Intn(len(s.URLs))] // Get random URL.

LOOP:

	response, err := s.Redis.Get(url).Result()
	if err == redis.Nil {
		// Check if URL exists in the queue.
		exist, err := s.Redis.SIsMember(queue, url).Result()
		if err != nil {
			log.Fatal(err)
		}

		if !exist {
			// Add url to the queue and start processing.
			res, err := s.Redis.SAdd(queue, url).Result()
			if err != nil || res == 0 {
				goto LOOP
			}

			if !s.checkCache(url) {
				s.setCache(url)
			}

			if err := s.Redis.SRem(queue, url).Err(); err != nil {
				log.Fatal(err)
			}
		}

		goto LOOP

	} else if err != nil {
		log.Fatal(err)
	}

	ch <- response
}

// checkCache returns true if URL exists in the cache.
func (s *server) checkCache(url string) bool {
	res, err := s.Redis.Exists(url).Result()
	if err != nil {
		log.Fatal(err)
	} else if res == 0 {
		return false
	}

	return true
}

// Set URL data to redis with expiration time.
func (s *server) setCache(url string) {

	response := curl(url)

	// TTL in ns
	ttl := (rand.Intn(s.MaxTimeout-s.MinTimeout+1) + s.MinTimeout)
	ttlns := ttl * 1000 * 1000 * 1000

	if err := s.Redis.SetNX(url, response, time.Duration(ttlns)).Err(); err != nil {
		log.Fatal(err)
	}

	log.Println("curl:", url, "TTL:", ttl)
}

// curl returns specified URL data.
func curl(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		return err.Error()
	}

	// Emulate reading response body.
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return "Internal server error"
	}

	return url + ": Success!"
}
