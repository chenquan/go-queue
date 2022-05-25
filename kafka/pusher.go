package kafka

import (
	"context"
	"time"

	"github.com/chenquan/go-queue/internal/xtrace"
	"github.com/chenquan/go-queue/queue"
	"go.opentelemetry.io/otel/trace"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	PushOption func(options *chunkOptions)

	Pusher struct {
		tracer   trace.Tracer
		producer *kafka.Writer
		topic    string
		executor *executors.ChunkExecutor
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}

	callOptions struct {
		isSync bool
	}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	tracer := xtrace.Tracer()
	producer := &kafka.Writer{
		Addr:        kafka.TCP(addrs...),
		Topic:       topic,
		Balancer:    &kafka.LeastBytes{},
		Compression: kafka.Snappy,
	}

	pusher := &Pusher{
		tracer:   tracer,
		producer: producer,
		topic:    topic,
	}

	pusher.executor = executors.NewChunkExecutor(
		func(tasks []interface{}) {
			chunk := make([]kafka.Message, len(tasks))
			for i := range tasks {
				chunk[i] = tasks[i].(kafka.Message)
			}
			if err := pusher.producer.WriteMessages(context.Background(), chunk...); err != nil {
				logx.Error(err)
			}
		}, newOptions(opts)...,
	)

	return pusher
}

func (p *Pusher) Close() error {
	return p.producer.Close()
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Push(ctx context.Context, k, v []byte, opts ...queue.CallOptions) (
	interface{}, error) {
	msg := kafka.Message{
		Key:   k,
		Value: v,
	}

	c := new(callOptions)
	for _, opt := range opts {
		opt(c)
	}

	if p.executor == nil {
		c.isSync = true
	}

	if c.isSync {
		return nil, p.producer.WriteMessages(ctx, msg)
	} else {
		return nil, p.executor.Add(msg, len(v))
	}
}

func WithChunkSize(chunkSize int) PushOption {
	return func(options *chunkOptions) {
		options.chunkSize = chunkSize
	}
}

func WithFlushInterval(interval time.Duration) PushOption {
	return func(options *chunkOptions) {
		options.flushInterval = interval
	}
}

func WithSync() queue.CallOptions {
	return func(i interface{}) {
		options, ok := i.(*callOptions)
		if !ok {
			panic(queue.ErrNotSupport)
		}

		options.isSync = true
	}
}

func newOptions(opts []PushOption) []executors.ChunkOption {
	var options chunkOptions
	for _, opt := range opts {
		opt(&options)
	}

	var chunkOpts []executors.ChunkOption
	if options.chunkSize > 0 {
		chunkOpts = append(chunkOpts, executors.WithChunkBytes(options.chunkSize))
	}

	if options.flushInterval > 0 {
		chunkOpts = append(chunkOpts, executors.WithFlushInterval(options.flushInterval))
	}

	return chunkOpts
}
