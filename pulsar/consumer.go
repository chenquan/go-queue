package pulsar

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chenquan/go-queue/queue"
	"go.opentelemetry.io/otel/trace"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zeromicro/go-zero/core/timex"
)

const (
	defaultCommitInterval = time.Second
	defaultMaxWait        = time.Second
	defaultQueueCapacity  = 1000
)

type (
	queueOptions struct {
		commitInterval time.Duration
		queueCapacity  int
		maxWait        time.Duration
		metrics        *stat.Metrics
	}

	QueueOption func(*queueOptions)

	pulsarQueue struct {
		c                Conf
		consumer         pulsar.Consumer
		handler          queue.Consumer
		channel          chan pulsar.ConsumerMessage
		consumerRoutines *threading.RoutineGroup
		metrics          *stat.Metrics
		tracer           trace.Tracer
	}

	Queues struct {
		queues []*pulsarQueue
		group  *service.ServiceGroup
		client pulsar.Client
	}
)

func MustNewQueue(c Conf, handler queue.Consumer, opts ...QueueOption) *Queues {
	q, err := NewQueue(c, handler, opts...)
	if err != nil {
		log.Fatal(err)
	}

	return q
}

func NewQueue(c Conf, handler queue.Consumer, opts ...QueueOption) (*Queues, error) {

	var options queueOptions
	for _, opt := range opts {
		opt(&options)
	}

	ensureQueueOptions(&options)

	if c.Conns < 1 {
		c.Conns = 1
	}

	// create a client
	url := fmt.Sprintf("pulsar://%s", strings.Join(c.Brokers, ","))
	client, err := pulsar.NewClient(
		pulsar.ClientOptions{
			URL:               url,
			ConnectionTimeout: 5 * time.Second,
			OperationTimeout:  5 * time.Second,
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	q := &Queues{
		group:  service.NewServiceGroup(),
		client: client,
	}
	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newPulsarQueue(c, client, handler, options))
	}

	return q, nil
}

func newPulsarQueue(
	c Conf, client pulsar.Client, handler queue.Consumer, options queueOptions) *pulsarQueue {
	// use client create more consumers, one consumer has one channel message
	channel := make(chan pulsar.ConsumerMessage, options.queueCapacity)
	consumer, err := client.Subscribe(
		pulsar.ConsumerOptions{
			Topic:            c.Topic,
			Type:             pulsar.Shared,
			SubscriptionName: c.SubscriptionName,
			MessageChannel:   channel,
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	return &pulsarQueue{
		c:                c,
		consumer:         consumer,
		handler:          handler,
		channel:          channel,
		consumerRoutines: threading.NewRoutineGroup(),
		metrics:          options.metrics,
	}
}

func (q *pulsarQueue) Start() {
	q.startConsumers()
	q.consumerRoutines.Wait()
}

func (q *pulsarQueue) Stop() {
	_ = q.consumer.Unsubscribe()
	close(q.channel)
	q.consumer.Close()
}

func (q *pulsarQueue) consumeOne(ctx context.Context, key, val []byte) error {
	startTime := timex.Now()
	err := q.handler.Consume(ctx, key, val)
	q.metrics.Add(
		stat.Task{
			Duration: timex.Since(startTime),
		},
	)
	return err
}

func (q *pulsarQueue) startConsumers() {

	for i := 0; i < q.c.Processors; i++ {
		q.consumerRoutines.Run(
			func() {
				for msg := range q.channel {
					q.consume(msg)
				}
			},
		)
	}
}

func (q *pulsarQueue) consume(m pulsar.ConsumerMessage) {
	ctx, span := q.tracer.Start(context.Background(), "consumer")
	defer span.End()
	if err := q.consumeOne(ctx, []byte(m.Key()), m.Payload()); err != nil {
		logx.WithContext(ctx).Errorf("Error on consuming: %s, error: %v", string(m.Payload()), err)
	}
	q.consumer.Ack(m)
}

func (q Queues) Start() {
	for _, each := range q.queues {
		q.group.Add(each)
	}
	q.group.Start()
}

func (q Queues) Stop() {
	q.group.Stop()
	q.client.Close()
	_ = logx.Close()

}

func WithCommitInterval(interval time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.commitInterval = interval
	}
}

func WithQueueCapacity(queueCapacity int) QueueOption {
	return func(options *queueOptions) {
		options.queueCapacity = queueCapacity
	}
}

func WithMaxWait(wait time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.maxWait = wait
	}
}

func WithMetrics(metrics *stat.Metrics) QueueOption {
	return func(options *queueOptions) {
		options.metrics = metrics
	}
}

func ensureQueueOptions(options *queueOptions) {
	if options.commitInterval == 0 {
		options.commitInterval = defaultCommitInterval
	}
	if options.queueCapacity == 0 {
		options.queueCapacity = defaultQueueCapacity
	}
	if options.maxWait == 0 {
		options.maxWait = defaultMaxWait
	}
	if options.metrics == nil {
		options.metrics = stat.NewMetrics("pulsar-consumer")
	}
}
