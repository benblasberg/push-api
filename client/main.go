package main

import (
	"context"
	"io"
	"log"
	"os"

	proto "github.com/benblasberg/push-api/server/protobuf/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()
	conn, err := grpc.NewClient("server:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewPushServiceClient(conn)

	token := os.Getenv("AUTH_TOKEN")
	streamType := os.Getenv("STREAM_TYPE")

	if streamType == "teams" {
		streamTeams(ctx, c, token)
	} else {
		streamEvents(ctx, c, token)
	}

}

func streamTeams(ctx context.Context, client proto.PushServiceClient, token string) {
	stream, err := client.StreamTeams(ctx, &proto.StreamTeamRequest{Token: token})
	if err != nil {
		log.Fatalf("failed reading: %v", err)
	}

	log.Println("Connected to team stream. Waiting...")

	receiveCount := 0
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("Stream closed")
				break
			}
			if err != nil {
				log.Fatalf("failed reading: %v", err)
				break
			}
		}

		log.Printf("%+v\n", resp)

		receiveCount += 1
		log.Printf("Successfully received teams data message #%d\n", receiveCount)
	}
}

func streamEvents(ctx context.Context, client proto.PushServiceClient, token string) {
	stream, err := client.StreamEvents(ctx, &proto.StreamEventRequest{Token: token})
	if err != nil {
		log.Fatalf("failed reading: %v", err)
	}

	log.Println("Connected to event stream. Waiting...")

	receiveCount := 0
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("Stream closed")
				break
			}
			if err != nil {
				log.Fatalf("failed reading: %v", err)
				break
			}
		}

		log.Printf("%+v\n", resp)

		receiveCount += 1
		log.Printf("Successfully received teams data message #%d\n", receiveCount)
	}
}
