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

const queue string = "queue"

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
	srv.Init()

	pb.RegisterStreamServer(grpcServer, srv)
	grpcServer.Serve(listener)
}

func (s *server) Init() {

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

	from := "redis cache"

	// Can produce 2-3 requests with the same URL
	// which will deadlock queue handler
	url := s.URLs[rand.Intn(len(s.URLs))]

	// Check if record exist in cache.
	response, err := s.Redis.Get(url).Result()
	if err == redis.Nil {

	LOOP:
		// Check queue
		ok, err := s.Redis.SIsMember(queue, url).Result()
		if err != nil {
			log.Fatal(err)
		}

		// Start DDOS
		if ok {
			time.Sleep(time.Second * 1)
			//log.Println("Already in queue", url)
			goto LOOP
		}

		// todo check 1/0/err
		// log.Println("Add to queue", url)
		res, err := s.Redis.SAdd(queue, url).Result()
		if err != nil || res == 0 {
			goto LOOP
		}

		response = curl(url)
		ttl := (rand.Intn(s.MaxTimeout-s.MinTimeout+1) + s.MinTimeout) * 1000 * 1000 * 1000

		// Set record to redis with expiration time.
		if err := s.Redis.SetNX(url, response, time.Duration(ttl)).Err(); err != nil {
			response = err.Error()
		}

		// todo check 1/0/err
		//log.Println("Remove from queue", url)
		s.Redis.SRem(queue, url)

		from = "curl"
	} else if err != nil {
		log.Fatal(err)
	}

	log.Println("---->", from, url)
	ch <- from + ":" + response
}

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

	//return "Response content"
	return url
}
