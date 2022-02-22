package beanstalkd

import (
	"context"
	"github.com/chenquan/go-queue/internal/xtrace"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
)

type (
	consumerNode struct {
		conn *connection
		tube string
		on   *syncx.AtomicBool
	}

	consumeService struct {
		c       *consumerNode
		consume ConsumeHandle
	}
)

func newConsumerNode(endpoint, tube string) *consumerNode {
	return &consumerNode{
		conn: newConnection(endpoint, tube),
		tube: tube,
		on:   syncx.ForAtomicBool(true),
	}
}

func (c *consumerNode) dispose() {
	c.on.Set(false)
}

func (c *consumerNode) consumeEvents(consume ConsumeHandle) {
	tracer := xtrace.Tracer()

	f := func() (exit bool) {
		ctx, span := tracer.Start(context.Background(), "consume-"+c.tube)
		defer span.End()
		conn, err := c.conn.get()
		logger := logx.WithContext(ctx)
		if err != nil {
			logger.Error(err)
			time.Sleep(time.Second)
			return
		}
		// because getting conn takes at most one second, reserve tasks at most 5 seconds,
		// if don't check on/off here, the conn might not be closed due to
		// graceful shutdon waits at most 5.5 seconds.
		if !c.on.True() {
			return true
		}

		conn.Tube.Name = c.tube
		conn.TubeSet.Name[c.tube] = true
		id, body, err := conn.Reserve(reserveTimeout)
		if err == nil {
			_ = conn.Delete(id)
			consume(ctx, body)
			return
		}

		// the error can only be beanstalk.NameError or beanstalk.ConnError
		switch cerr := err.(type) {
		case beanstalk.ConnError:
			switch cerr.Err {
			case beanstalk.ErrTimeout:
				// timeout error on timeout, just continue the loop
			case beanstalk.ErrBadChar, beanstalk.ErrBadFormat, beanstalk.ErrBuried, beanstalk.ErrDeadline,
				beanstalk.ErrDraining, beanstalk.ErrEmpty, beanstalk.ErrInternal, beanstalk.ErrJobTooBig,
				beanstalk.ErrNoCRLF, beanstalk.ErrNotFound, beanstalk.ErrNotIgnored, beanstalk.ErrTooLong:
				// won't reset
				logger.Error(err)
			default:
				// beanstalk.ErrOOM, beanstalk.ErrUnknown and other errors
				logger.Error(err)
				c.conn.reset()
				time.Sleep(time.Second)
			}
		default:
			logger.Error(err)
		}
		return
	}
	for c.on.True() {
		f()
	}

	if err := c.conn.Close(); err != nil {
		logx.Error(err)
	}
}

func (cs consumeService) Start() {
	cs.c.consumeEvents(cs.consume)
}

func (cs consumeService) Stop() {
	cs.c.dispose()
}
