package kafka

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type TopicName string

const (
	TEAMS  TopicName = "teams"
	EVENTS TopicName = "events"
)

type Consumer[E any] struct {
	Topic       TopicName
	Brokers     []string
	clientConns map[string]chan []*E
	connsMut    sync.RWMutex
	converter   Converter[E]
}

func NewConsumer[E any](topic TopicName, brokers []string, converter Converter[E]) *Consumer[E] {
	return &Consumer[E]{
		Topic:       topic,
		Brokers:     brokers,
		clientConns: map[string]chan []*E{},
		connsMut:    sync.RWMutex{},
		converter:   converter,
	}
}

func (c *Consumer[E]) AddConnection() (string, chan []*E, error) {
	conn := make(chan []*E)
	id := uuid.New().String()
	c.connsMut.Lock()
	defer c.connsMut.Unlock()
	c.clientConns[id] = conn
	return id, conn, nil
}

func (c *Consumer[E]) RemoveConnection(id string) {
	c.connsMut.Lock()
	defer c.connsMut.Unlock()
	delete(c.clientConns, id)
}

func (c *Consumer[E]) Start(ctx context.Context) error {
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
		CommitInterval: 0, // We don't need to commit our offset since we're only interested in live data
		StartOffset:    kafka.LastOffset,
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

					// Should use exponential backoff here so we don't overload kafka with requests
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

func (c *Consumer[E]) handleMessage(message kafka.Message) error {
	convertedMsg, err := c.converter.Convert(message)
	if err != nil {
		return err
	}

	c.connsMut.RLock()
	defer c.connsMut.RUnlock()

	for _, conn := range c.clientConns {
		conn <- convertedMsg
	}

	return nil

}
