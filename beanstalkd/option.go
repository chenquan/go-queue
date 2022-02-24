package beanstalkd

import (
	"github.com/chenquan/go-queue/queue"
	"github.com/zeromicro/go-zero/core/stat"
	"time"
)

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

func WithAt(at time.Time) queue.CallOptions {
	return func(i interface{}) {
		options, ok := i.(*callOptions)
		if !ok {
			panic(queue.ErrNotSupport)
		}
		options.at = at

	}
}

func WithDuration(duration time.Duration) queue.CallOptions {
	return func(i interface{}) {
		options, ok := i.(*callOptions)
		if !ok {
			panic(queue.ErrNotSupport)
		}
		options.at = time.Now().Add(duration)
	}
}
