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
	queue           string = "queue"
	queueProcessing string = "queue_processing"
	queueDone       string = "queue_done"
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

// Initialize server's configuration.
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
	s.queue = make(chan string)

	rand.Seed(time.Now().Unix()) // Initialize global pseudo random generator.

	go s.processQueue()

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
	// Check if record exist in the redis cache.
	response, err := s.Redis.Get(url).Result()
	if err == redis.Nil {
		// If records does not exist, add URL
		// to the queue sand repeat checking.
		s.queue <- url
		time.Sleep(time.Millisecond * 1)
		goto LOOP
	} else if err != nil {
		log.Fatal(err)
	}

	ch <- response
}

// Init queue watcher.
func (s *server) processQueue() {
LOOP:
	for url := range s.queue {

		// Get number of URLs in the main queue.
		//cnt, err := s.Redis.LLen(worker).Result()
		//if err != nil || cnt > 0 {
		//	log.Println(err, cnt)
		//	continue
		//}

		urls, err := s.Redis.LRange(queueProcessing, 0, -1).Result()
		if err != nil {
			log.Println(err)
			continue
		}

		for i := range urls {
			if urls[i] == url {
				time.Sleep(time.Millisecond * 1)
				goto LOOP
			}
		}

		// Add URL to the queue.
		if err := s.Redis.LPush(queue, url).Err(); err != nil {
			log.Println(err)
			continue
		}

		// Blocking read from the main queue and pushing
		// to the worker queue (queue_processing).
		res, err := s.Redis.BRPopLPush(queue, queueProcessing, time.Duration(time.Second*1)).Result()
		if err != nil {
			log.Println(err)
			continue
		}

		if res != url {
			log.Fatal(res, url) // Something bad just happened.
		}

		go s.worker(url)
	}
}

// Worker recieves URL for processing from queue_processing.
// First it checks if URL exist in the cache.
// If not, worker starts processing.
func (s *server) worker(url string) {
	err := s.Redis.Get(url).Err()
	if err == redis.Nil {
		s.setCache(url)
	} else if err != nil {
		log.Fatal(err)
	}

	// Remove URL from queue_processing.
	err = s.Redis.LPop(queueProcessing).Err()
	if err != nil {
		log.Fatal(err)
	}
}

// Set URL data to redis with expiration time.
func (s *server) setCache(url string) {

	response := curl(url)

	// TTL in ns
	ttl := (rand.Intn(s.MaxTimeout-s.MinTimeout+1) + s.MinTimeout) * 1000 * 1000 * 1000

	if err := s.Redis.SetNX(url, response, time.Duration(ttl)).Err(); err != nil {
		log.Fatal(err)
	}
}

// curl returns specified URL data.
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

	return url
}
