package kafka

import (
	"context"
	"time"

	"github.com/chenquan/go-queue/internal/xtrace"
	"github.com/chenquan/go-queue/queue"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/segmentio/kafka-go"
)

type (
	PushOption func(options *pushOptions)

	Pusher struct {
		tracer   trace.Tracer
		producer *kafka.Writer
		metrics  *stat.Metrics
		executor *executors.ChunkExecutor
		topic    string
		stopOnce func()
	}

	pushOptions struct {

		// An optional function called when the writer succeeds or fails the
		// delivery of messages to a kafka partition.
		completion               func(messages []kafka.Message, err error)
		balancer                 Balancer
		disableAutoTopicCreation bool
		// Limit on how many messages will be buffered before being sent to a
		// partition.
		//
		// The default is to use a target batch size of 100 messages.
		batchSize int

		// Limit the maximum size of a request in bytes before being sent to
		// a partition.
		//
		// The default is to use a kafka default value of 1048576.
		batchBytes int64

		requiredAcks RequiredAcks

		// async
		chunkSize     int
		flushInterval time.Duration

		// auth
		username string
		password string
	}

	callOptions struct {
		sync bool
	}

	message struct {
		msg *kafka.Message
		ctx context.Context
	}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	tracer := xtrace.Tracer()
	var options pushOptions
	for _, opt := range opts {
		opt(&options)
	}

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(addrs...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		Compression:            kafka.Snappy,
		Completion:             options.completion,
		AllowAutoTopicCreation: !options.disableAutoTopicCreation,
		RequiredAcks:           options.requiredAcks,
	}
	if len(options.username) > 0 && len(options.password) > 0 {
		producer.Transport.(*kafka.Transport).SASL = plain.Mechanism{
			Username: options.username,
			Password: options.password,
		}
	}

	if options.balancer != nil {
		producer.Balancer = options.balancer
	}

	pusher := &Pusher{
		tracer:   tracer,
		producer: producer,
		topic:    topic,
		metrics:  stat.NewMetrics("kafka-pusher"),
	}

	var chunkOpts []executors.ChunkOption
	if options.chunkSize > 0 {
		chunkOpts = append(chunkOpts, executors.WithChunkBytes(options.chunkSize))
	}
	if options.flushInterval > 0 {
		chunkOpts = append(chunkOpts, executors.WithFlushInterval(options.flushInterval))
	}
	pusher.executor = executors.NewChunkExecutor(pusher.doTask, chunkOpts...)

	pusher.stopOnce = syncx.Once(pusher.doStop)

	return pusher
}

func (p *Pusher) Close() error {
	p.stopOnce()
	return nil
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Start() {
}

func (p *Pusher) doStop() {
	p.executor.Flush()
	p.executor.Wait()
	err := p.producer.Close()
	if err != nil {
		logx.Error(err)
	}
}

func (p *Pusher) Stop() {
	p.stopOnce()
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

	headers, b := HeadersFromContext(ctx)
	if b {
		msg.Headers = headers
	}

	if c.sync {
		startTime := timex.Now()

		attrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String("kafka"),
			semconv.MessagingDestinationKindTopic,
			semconv.MessagingDestinationKey.String(p.topic),
		}
		ctx, span := p.tracer.Start(ctx,
			"kafka-pusher",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attrs...),
		)
		spanContext := span.SpanContext()
		if spanContext.IsValid() {
			propagator := otel.GetTextMapPropagator()
			propagator.Inject(ctx, &Headers{headers: &msg.Headers})
		}
		defer span.End()

		err := p.producer.WriteMessages(ctx, msg)
		if err != nil {
			p.metrics.AddDrop()

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		p.metrics.Add(stat.Task{
			Duration: timex.Since(startTime),
		})
		span.SetStatus(codes.Ok, "")

	} else {
		_ = p.executor.Add(message{msg: &msg, ctx: ctx}, len(v))
	}

	return nil, nil
}

func (p *Pusher) doTask(tasks []interface{}) {
	startTime := timex.Now()

	propagator := otel.GetTextMapPropagator()
	messages := make([]kafka.Message, 0, len(tasks))
	links := make([]trace.Link, 0, len(tasks))
	for i := range tasks {
		msg := tasks[i].(message)
		ctx := msg.ctx

		links = append(links, trace.LinkFromContext(ctx))

		span := trace.SpanFromContext(ctx)
		spanContext := span.SpanContext()
		if spanContext.IsValid() {
			propagator.Inject(ctx, &Headers{headers: &msg.msg.Headers})
		}
		messages = append(messages, *msg.msg)
	}

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(p.topic),
	}
	ctx, span := p.tracer.Start(context.Background(),
		"kafka-async-batch-pusher",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithLinks(links...),
		trace.WithAttributes(attrs...),
	)
	defer span.End()

	logger := logx.WithContext(ctx)
	if err := p.producer.WriteMessages(ctx, messages...); err != nil {
		logger.Errorf("send failed, total size:%d, err:%s", len(messages), err.Error())
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		for range messages {
			p.metrics.AddDrop()
		}

		return
	}

	span.SetStatus(codes.Ok, "")
	logger.Infof("send successful,total size:%d", len(messages))

	duration := timex.Since(startTime)
	for range messages {
		p.metrics.Add(stat.Task{
			Duration: duration,
		})
	}
}

func WithCompletion(completion func(messages []kafka.Message, err error)) PushOption {
	return func(options *pushOptions) {
		options.completion = completion
	}
}

func WithBalancer(balancer Balancer) PushOption {
	return func(options *pushOptions) {
		options.balancer = balancer
	}
}

func WithDisableAutoTopicCreation() PushOption {
	return func(options *pushOptions) {
		options.disableAutoTopicCreation = true
	}
}

func WithBatchSize(batchSize int) PushOption {
	return func(options *pushOptions) {
		options.batchSize = batchSize
	}
}

func WithBatchBytes(batchBytes int64) PushOption {
	return func(options *pushOptions) {
		options.batchBytes = batchBytes
	}
}

func WithRequiredAcks(requiredAcks RequiredAcks) PushOption {
	return func(options *pushOptions) {
		options.requiredAcks = requiredAcks
	}
}

func WithSyncCall() queue.CallOptions {
	return func(i interface{}) {
		options, ok := i.(*callOptions)
		if !ok {
			panic(queue.ErrNotSupport)
		}
		options.sync = true
	}
}

func WithChunkSize(chunkSize int) PushOption {
	return func(options *pushOptions) {
		options.chunkSize = chunkSize
	}
}

func WithFlushInterval(flushInterval time.Duration) PushOption {
	return func(options *pushOptions) {
		options.flushInterval = flushInterval
	}
}

func WithAuth(username, password string) PushOption {
	return func(options *pushOptions) {
		options.username = username
		options.password = password
	}

}
