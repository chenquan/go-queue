package dq

import (
	"context"
	"errors"
	"fmt"
	"github.com/chenquan/go-queue"
	"github.com/chenquan/go-queue/internal/xtrace"
	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel/trace"
	"strconv"
	"strings"
	"time"

	"github.com/beanstalkd/go-beanstalk"
)

var ErrTimeBeforeNow = errors.New("can't schedule task to past time")

type producerNode struct {
	tracer   trace.Tracer
	endpoint string
	tube     string
	conn     *connection
}

func (p *producerNode) Name() string {
	return p.tube
}

func NewProducerNode(endpoint, tube string) queue.DelayPusher {
	return &producerNode{
		tracer:   xtrace.Tracer(),
		endpoint: endpoint,
		tube:     tube,
		conn:     newConnection(endpoint, tube),
	}
}

func (p *producerNode) At(ctx context.Context, body []byte, at time.Time) (string, error) {
	now := time.Now()
	if at.Before(now) {
		return "", ErrTimeBeforeNow
	}

	duration := at.Sub(now)
	return p.Delay(ctx, body, duration)
}

func (p *producerNode) Close() error {
	return p.conn.Close()
}

func (p *producerNode) Delay(ctx context.Context, body []byte, delay time.Duration) (string, error) {
	ctx, span := p.tracer.Start(ctx, "delay")
	defer span.End()

	conn, err := p.conn.get()
	defer func() {
		if err != nil {
			logx.WithContext(ctx).Error(err)
		}
	}()

	if err != nil {
		return "", err
	}

	id, err := conn.Put(body, PriNormal, delay, defaultTimeToRun)
	if err == nil {
		return fmt.Sprintf("%s/%s/%d", p.endpoint, p.tube, id), nil
	}

	// the error can only be beanstalk.NameError or beanstalk.ConnError
	// just return when the error is beanstalk.NameError, don't reset
	switch cerr := err.(type) {
	case beanstalk.ConnError:
		switch cerr.Err {
		case beanstalk.ErrBadChar, beanstalk.ErrBadFormat, beanstalk.ErrBuried, beanstalk.ErrDeadline,
			beanstalk.ErrDraining, beanstalk.ErrEmpty, beanstalk.ErrInternal, beanstalk.ErrJobTooBig,
			beanstalk.ErrNoCRLF, beanstalk.ErrNotFound, beanstalk.ErrNotIgnored, beanstalk.ErrTooLong:
			// won't reset
		default:
			// beanstalk.ErrOOM, beanstalk.ErrTimeout, beanstalk.ErrUnknown and other errors
			p.conn.reset()
		}
	}

	return "", err
}

func (p *producerNode) Revoke(_ context.Context, jointId string) error {
	ids := strings.Split(jointId, idSep)
	for _, id := range ids {
		fields := strings.Split(id, "/")
		if len(fields) < 3 {
			continue
		}
		if fields[0] != p.endpoint || fields[1] != p.tube {
			continue
		}

		conn, err := p.conn.get()
		if err != nil {
			return err
		}

		n, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return err
		}

		return conn.Delete(n)
	}

	// if not in this beanstalk, ignore
	return nil
}
