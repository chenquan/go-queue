package beanstalkd

import (
	"context"
	"github.com/chenquan/go-queue/queue"
	"github.com/zeromicro/go-zero/core/hash"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stat"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	expiration = 3600 // seconds
	guardValue = "1"
	tolerance  = time.Minute * 30
)

var maxCheckBytes = getMaxTimeLen()

type (
	ConsumeHandle func(ctx context.Context, body []byte)

	ConsumerOption func(*queueOptions)

	ConsumerCluster struct {
		nodes  []*consumerNode
		red    *redis.Redis
		group  *service.ServiceGroup
		handle queue.Consumer
		metric *stat.Metrics
	}
	queueOptions struct {
		metrics *stat.Metrics
	}
)

func NewConsumer(c Conf, handle queue.Consumer, opts ...ConsumerOption) *ConsumerCluster {
	var nodes []*consumerNode
	for _, endpoint := range c.Endpoints {
		nodes = append(nodes, newConsumerNode(endpoint, c.Tube))
	}

	op := new(queueOptions)
	for _, opt := range opts {
		opt(op)
	}
	if op.metrics == nil {
		op.metrics = stat.NewMetrics("beanstalkd-consumer")
	}

	return &ConsumerCluster{
		group:  service.NewServiceGroup(),
		nodes:  nodes,
		red:    c.Redis.NewRedis(),
		handle: handle,
		metric: op.metrics,
	}
}

func (c ConsumerCluster) Start() {
	guardedConsume := func(ctx context.Context, body []byte) {
		key := hash.Md5Hex(body)

		body, ok := c.unwrap(ctx, body)
		if !ok {
			logx.WithContext(ctx).Errorf("discarded: %q", string(body))
			return
		}

		startTime := time.Now()
		defer c.metric.Add(stat.Task{
			Duration: time.Since(startTime),
		})

		ok, err := c.red.SetnxEx(key, guardValue, expiration)
		if err != nil {
			logx.WithContext(ctx).Error(err)
		} else if ok {
			_ = c.handle.Consume(ctx, nil, body)
		}
	}

	for _, node := range c.nodes {
		c.group.Add(consumeService{
			c:       node,
			consume: guardedConsume,
		})
	}
	c.group.Start()
}

func (c ConsumerCluster) Stop() {
	c.group.Stop()
}

func (c *ConsumerCluster) unwrap(ctx context.Context, body []byte) ([]byte, bool) {
	var pos = -1
	for i := 0; i < maxCheckBytes && i < len(body); i++ {
		if body[i] == timeSep {
			pos = i
			break
		}
	}
	if pos < 0 {
		return nil, false
	}

	val, err := strconv.ParseInt(string(body[:pos]), 10, 64)
	if err != nil {
		logx.WithContext(ctx).Error(err)
		return nil, false
	}

	t := time.Unix(0, val)
	if t.Add(tolerance).Before(time.Now()) {
		return nil, false
	}

	return body[pos+1:], true
}

func getMaxTimeLen() int {
	return len(strconv.FormatInt(time.Now().UnixNano(), 10)) + 2
}

type innerConsumeHandler struct {
	handle ConsumeHandle
}

func (ch innerConsumeHandler) Consume(ctx context.Context, _, v []byte) error {
	ch.handle(ctx, v)
	return nil
}

func WithHandle(handle ConsumeHandle) queue.Consumer {
	return innerConsumeHandler{
		handle: handle,
	}
}

func WithMetrics(metrics *stat.Metrics) ConsumerOption {
	return func(options *queueOptions) {
		options.metrics = metrics
	}
}
