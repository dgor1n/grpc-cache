package main

import (
	"context"
	"fmt"
	"io"
	"log"

	pb "github.com/dgor1n/grpc-cache/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func main() {

	defer log.Println("Done")

	opts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial("127.0.0.1:5300", opts...)

	if err != nil {
		grpclog.Fatalf("fail to dial: %v", err)
	}

	defer conn.Close()

	for i := 0; i < 100; i++ {
		go func() {
			request := &pb.Request{}
			client := pb.NewStreamClient(conn)

			response, err := client.GetRandomDataStream(context.Background(), request)
			if err != nil {
				grpclog.Fatalf("fail to dial: %v", err)
			}

			for {
				r, err := response.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					grpclog.Fatalf("fail to read response: %v", err)
				}

				fmt.Println(r.Message)
			}
		}()
	}

}
