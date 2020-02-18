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

const (
	queue  string = "queue"
	worker string = "processing_queue"
)

type server struct {
	MaxTimeout       int
	MinTimeout       int
	NumberOfRequests int
	URLs             []string

	Redis *redis.Client

	queue chan string
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

// Init ...
func initServer() *server {

	s := &server{}

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

	s.queue = make(chan string)
	urls := make(chan string)

	go s.processQueue(urls)
	go s.worker(urls)

	return s
}

func (s *server) GetRandomDataStream(r *pb.Request, stream pb.Stream_GetRandomDataStreamServer) error {

	// defer log.Println("Done")

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

// Init queue watcher.
func (s *server) processQueue(urls chan<- string) {
	for url := range s.queue {

		cnt, err := s.Redis.LLen(worker).Result()
		if err != nil || cnt > 0 {
			continue
		}

		s.Redis.LPush(queue, url)

		u, err := s.Redis.BRPopLPush(queue, worker, time.Duration(time.Second*1)).Result()
		if err != nil {
			log.Println(err)
			continue
		}

		//log.Println("Add to worker queue", url)

		urls <- u
	}
}

func (s *server) worker(urls <-chan string) {
	for url := range urls {
		err := s.Redis.Get(url).Err()
		if err == redis.Nil {
			s.setCache(url)
		} else if err != nil {
			log.Fatal(err)
		}

		err = s.Redis.LPop(worker).Err()
		if err != nil {
			log.Fatal(err)
		}
		//log.Println("Remove from worker queue", res, err)
	}

	log.Println("how?")
}

// Set record to redis with expiration time.
func (s *server) setCache(url string) string {
	response := curl(url)
	ttl := (rand.Intn(s.MaxTimeout-s.MinTimeout+1) + s.MinTimeout) * 1000 * 1000 * 1000

	if err := s.Redis.SetNX(url, response, time.Duration(ttl)).Err(); err != nil {
		return err.Error()
	}

	return response
}

func (s *server) processData(ch chan<- string) {

	from := "redis cache"

	// Can produce 2-3 requests with the same URL
	// which will deadlock queue handler.
	url := s.URLs[rand.Intn(len(s.URLs))]

LOOP:
	// Check if record exist in cache.
	response, err := s.Redis.Get(url).Result()
	if err == redis.Nil {
		from = queue
		s.queue <- url
		goto LOOP
	} else if err != nil {
		log.Fatal(err)
	}

	//log.Println("---->", from, url)
	ch <- from + ":" + response
}

func curl(url string) string {

	log.Println("Curl: ", url)

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
