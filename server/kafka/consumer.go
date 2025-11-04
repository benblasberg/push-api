package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	proto "github.com/benblasberg/push-api/server/protobuf/gen"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type TopicName string

const (
	TEAMS TopicName = "teams"
)

type Consumer struct {
	Topic       TopicName
	Brokers     []string
	clientConns map[string]chan []*proto.Team
	connsMut    sync.RWMutex
}

func NewConsumer(topic TopicName, brokers []string) *Consumer {
	return &Consumer{
		Topic:       topic,
		Brokers:     brokers,
		clientConns: map[string]chan []*proto.Team{},
		connsMut:    sync.RWMutex{},
	}
}

func (c *Consumer) AddConnection() (string, chan []*proto.Team, error) {
	conn := make(chan []*proto.Team)
	id := uuid.New().String()
	c.connsMut.Lock()
	defer c.connsMut.Unlock()
	c.clientConns[id] = conn
	return id, conn, nil
}

func (c *Consumer) RemoveConnection(id string) {
	c.connsMut.Lock()
	defer c.connsMut.Unlock()
	delete(c.clientConns, id)
}

func (c *Consumer) Start(ctx context.Context) error {
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		<-signals
		cancel()
	}()

	config := kafka.ReaderConfig{
		Brokers:        c.Brokers,
		Topic:          string(c.Topic),
		GroupID:        "",
		CommitInterval: 0, // We don't need to commit our offset since we're only interested in live data
	}

	reader := kafka.NewReader(config)

	go func() {
		running := true
		for running {
			select {
			case <-ctx.Done():
				err := reader.Close()
				if err != nil {
					slog.ErrorContext(ctx, "Error closing reader")
				}
				slog.Info(fmt.Sprintf("Closing reader for topic: %s", string(c.Topic)))
				running = false
			default:
				message, err := reader.ReadMessage(ctx)
				if err != nil {
					if err == io.EOF {
						running = false
						break
					}
					slog.ErrorContext(ctx, fmt.Sprintf("Failed to read message from %s: %v\n", string(c.Topic), err))

					time.Sleep(1 * time.Second)
					continue
				}

				err = c.handleMessage(message)
				if err != nil {
					slog.ErrorContext(ctx, fmt.Sprintf("Failed to deserialize message from %s: %v\n", string(c.Topic), err))
				}
			}
		}
	}()
	return nil
}

func (c *Consumer) handleMessage(message kafka.Message) error {
	if message.Topic == string(TEAMS) {
		converter := TeamsConverter{}

		teams, err := converter.Convert(message)
		if err != nil {
			return err
		}

		fmt.Printf("Received from partition: %d\n", message.Partition)

		c.connsMut.RLock()
		defer c.connsMut.RUnlock()

		for _, conn := range c.clientConns {
			conn <- teams
		}

		return nil
	} else {
		return errors.New("Received message from topic with no handler: " + message.Topic)
	}
}
