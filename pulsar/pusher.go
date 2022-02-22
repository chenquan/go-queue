package pulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chenquan/go-queue/internal/xtrace"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	PushOption func(options *chunkOptions)

	Pusher struct {
		tracer   trace.Tracer
		producer pulsar.Producer
		topic    string
		executor *executors.ChunkExecutor
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	tracer := xtrace.Tracer()

	url := strings.Join(addrs, ",")
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://" + url,
		ConnectionTimeout: 5 * time.Second,
		OperationTimeout:  5 * time.Second,
	})
	client.Close()

	if err != nil {
		logx.Errorf("could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	if err != nil {
		logx.Error(err)
	}

	pusher := &Pusher{
		tracer:   tracer,
		producer: producer,
		topic:    topic,
	}

	pusher.executor = executors.NewChunkExecutor(func(tasks []interface{}) {
		for i := range tasks {
			if _, err := pusher.producer.Send(context.Background(), tasks[i].(*pulsar.ProducerMessage)); err != nil {
				logx.Error(err)
			}
		}

	}, newOptions(opts)...)

	return pusher
}

func (p *Pusher) Close() error {
	p.producer.Close()
	return nil
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Push(ctx context.Context, k, v []byte) error {
	msg := &pulsar.ProducerMessage{
		Key:     string(k),
		Payload: v,
	}

	if p.executor != nil {
		return p.executor.Add(msg, len(v))
	} else {
		_, err := p.producer.Send(ctx, msg)
		return err
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
