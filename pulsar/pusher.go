package pulsar

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chenquan/go-queue/internal/xtrace"
	"github.com/chenquan/go-queue/queue"
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
		client   pulsar.Client
		topic    string
		executor *executors.ChunkExecutor
	}

	Message struct {
		// Payload for the message
		Payload []byte

		// Value and payload is mutually exclusive, `Value interface{}` for schema message.
		Value interface{}

		// Key sets the key of the message for routing policy
		Key string

		// OrderingKey sets the ordering key of the message
		OrderingKey string

		// Properties attach application defined properties on the message
		Properties map[string]string

		// EventTime set the event time for a given message
		// By default, messages don't have an event time associated, while the publish
		// time will be be always present.
		// Set the event time to a non-zero timestamp to explicitly declare the time
		// that the event "happened", as opposed to when the message is being published.
		EventTime time.Time

		// ReplicationClusters override the replication clusters for this message.
		ReplicationClusters []string

		// DisableReplication disables the replication for this message
		DisableReplication bool

		// SequenceID sets the sequence id to assign to the current message
		SequenceID *int64

		// DeliverAfter requests to deliver the message only after the specified relative delay.
		// Note: messages are only delivered with delay when a consumer is consuming
		//     through a `SubscriptionType=Shared` subscription. With other subscription
		//     types, the messages will still be delivered immediately.
		DeliverAfter time.Duration

		// DeliverAt delivers the message only at or after the specified absolute timestamp.
		// Note: messages are only delivered with delay when a consumer is consuming
		//     through a `SubscriptionType=Shared` subscription. With other subscription
		//     types, the messages will still be delivered immediately.
		DeliverAt time.Time
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	tracer := xtrace.Tracer()

	url := fmt.Sprintf("pulsar://%s", strings.Join(addrs, ","))
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               url,
		ConnectionTimeout: 5 * time.Second,
		OperationTimeout:  5 * time.Second,
	})

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
		client:   client,
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
	p.client.Close()
	return nil
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Push(ctx context.Context, k, v []byte, opts ...queue.CallOptions) (interface{}, error) {
	op := new(Message)

	for _, opt := range opts {
		opt(op)
	}

	msg := &pulsar.ProducerMessage{
		Payload:             v,
		Value:               op.Value,
		Key:                 string(k),
		OrderingKey:         op.OrderingKey,
		Properties:          op.Properties,
		EventTime:           op.EventTime,
		ReplicationClusters: op.ReplicationClusters,
		DisableReplication:  op.DisableReplication,
		SequenceID:          op.SequenceID,
		DeliverAfter:        op.DeliverAfter,
		DeliverAt:           op.DeliverAt,
	}

	if p.executor != nil {
		return nil, p.executor.Add(msg, len(v))
	} else {
		id, err := p.producer.Send(ctx, msg)
		return id, err
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

func WithMessage(message Message) queue.CallOptions {
	return func(i interface{}) {
		m, ok := i.(*Message)
		if !ok {
			panic(queue.ErrNotSupport)
		}
		*m = message

	}
}

func WithDeliverAt(deliverAt time.Time) queue.CallOptions {
	return func(i interface{}) {
		m, ok := i.(*Message)
		if !ok {
			panic(queue.ErrNotSupport)
		}
		m.DeliverAt = deliverAt

	}
}

func WithDeliverAfter(deliverAfter time.Duration) queue.CallOptions {
	return func(i interface{}) {
		m, ok := i.(*Message)
		if !ok {
			panic(queue.ErrNotSupport)
		}
		m.DeliverAfter = deliverAfter

	}
}

func WithOrderingKey(OrderingKey string) queue.CallOptions {
	return func(i interface{}) {

		m, ok := i.(*Message)
		if !ok {
			panic(queue.ErrNotSupport)
		}
		m.OrderingKey = OrderingKey
	}
}
