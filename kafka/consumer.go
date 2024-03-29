package kafka

import (
	"context"
	"io"
	"log"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/chenquan/go-queue/internal/xtrace"
	"github.com/chenquan/go-queue/queue"
	"github.com/chenquan/orderhash"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/segmentio/kafka-go"
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

	kafkaQueue struct {
		c                Conf
		consumer         *kafka.Reader
		handler          queue.Consumer
		channels         []chan kafka.Message
		producerRoutines *threading.RoutineGroup
		consumerRoutines *threading.RoutineGroup
		metrics          *stat.Metrics
		tracer           trace.Tracer
		hashFunc         func(b []byte) uint64
	}

	Queues struct {
		queues []*kafkaQueue
		group  *service.ServiceGroup
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
	q := &Queues{
		group: service.NewServiceGroup(),
	}
	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newKafkaQueue(c, handler, options))
	}

	return q, nil
}

func newKafkaQueue(c Conf, handler queue.Consumer, options queueOptions) *kafkaQueue {
	var offset int64
	if c.Offset == firstOffset {
		offset = kafka.FirstOffset
	} else {
		offset = kafka.LastOffset
	}
	readerConfig := kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        c.Group,
		Topic:          c.Topic,
		StartOffset:    offset,
		MinBytes:       c.MinBytes, // 10KB
		MaxBytes:       c.MaxBytes, // 10MB
		MaxWait:        options.maxWait,
		CommitInterval: options.commitInterval,
		QueueCapacity:  options.queueCapacity,
	}
	if len(c.Username) > 0 && len(c.Password) > 0 {
		readerConfig.Dialer = &kafka.Dialer{
			SASLMechanism: plain.Mechanism{
				Username: c.Username,
				Password: c.Password,
			},
		}
	}
	consumer := kafka.NewReader(
		readerConfig,
	)

	channels := make([]chan kafka.Message, c.Processors)
	for i := 0; i < c.Processors; i++ {
		channels[i] = make(chan kafka.Message, 8)
	}

	return &kafkaQueue{
		c:        c,
		consumer: consumer,
		handler: queue.ConsumeHandle(func(ctx context.Context, key, value []byte) error {
			defer func() {
				if e := recover(); e != nil {
					logx.WithContext(ctx).Errorf("panic: %v", debug.Stack())
				}
			}()

			return handler.Consume(ctx, key, value)
		}),
		channels:         channels,
		producerRoutines: threading.NewRoutineGroup(),
		consumerRoutines: threading.NewRoutineGroup(),
		metrics:          options.metrics,
		tracer:           xtrace.Tracer(),
		hashFunc:         orderhash.Hash64(xxhash.Sum64),
	}
}

func (q *kafkaQueue) Start() {
	q.startConsumers()
	q.startProducers()

	q.producerRoutines.Wait()
	for _, channel := range q.channels {
		close(channel)
	}
	q.consumerRoutines.Wait()
}

func (q *kafkaQueue) Stop() {
	_ = q.consumer.Close()
}

func (q *kafkaQueue) consumeOne(ctx context.Context, key, val []byte) error {
	startTime := timex.Now()
	err := q.handler.Consume(ctx, key, val)
	if err != nil {
		q.metrics.AddDrop()
		return err
	}

	q.metrics.Add(
		stat.Task{
			Duration: timex.Since(startTime),
		},
	)

	return nil
}

func (q *kafkaQueue) startConsumers() {
	for _, channel := range q.channels {
		channel := channel
		q.consumerRoutines.Run(
			func() {
				for msg := range channel {
					q.consume(msg)
				}
			},
		)
	}
}

func (q *kafkaQueue) consume(m kafka.Message) {
	propagator := otel.GetTextMapPropagator()
	ctxRemote := propagator.Extract(context.Background(), &Headers{headers: &m.Headers})
	link := trace.LinkFromContext(ctxRemote)

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(m.Topic),
		semconv.MessagingOperationReceive,
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(m.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(m.Partition)),
		semconv.MessagingKafkaConsumerGroupKey.String(q.c.Group),
	}

	ctx, span := q.tracer.Start(context.Background(),
		"kafka-consumer",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
		trace.WithLinks(link),
		trace.WithTimestamp(time.Now()),
	)

	defer span.End()

	if len(m.Headers) != 0 {
		ctx = NewHeadersContext(ctx, m.Headers...)
	}
	spanContextRemote := link.SpanContext
	if spanContextRemote.IsValid() {
		if spanContextRemote.HasTraceID() {
			ctx = logx.WithFields(ctx, logx.Field("link", spanContextRemote.TraceID().String()))
		}
	}
	logger := logx.WithContext(ctx)

	if err := q.consumeOne(ctx, m.Key, m.Value); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.Errorf("error on consuming: %s, error: %v", string(m.Value), err)
		return
	}

	err := q.consumer.CommitMessages(ctx, m)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.Error(err)
		return
	}

	span.SetStatus(codes.Ok, "")
}

func (q *kafkaQueue) startProducers() {
	for i := 0; i < q.c.Consumers; i++ {
		q.producerRoutines.Run(
			func() {
				for {
					msg, err := q.consumer.FetchMessage(context.Background())
					// io.EOF means consumer closed
					// io.ErrClosedPipe means committing messages on the consumer,
					// kafka will refire the messages on uncommitted messages, ignore
					if err == io.EOF || err == io.ErrClosedPipe {
						return
					}

					if err != nil {
						logx.Errorf("Error on reading message, %q", err.Error())
						continue
					}

					hashValue := q.hashFunc(msg.Key)
					index := hashValue % uint64(len(q.channels))

					q.channels[index] <- msg
				}
			},
		)
	}
}

func (q Queues) Start() {
	for _, each := range q.queues {
		q.group.Add(each)
	}
	q.group.Start()
}

func (q Queues) Stop() {
	q.group.Stop()
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
		options.metrics = stat.NewMetrics("kafka-consumer")
	}
}
