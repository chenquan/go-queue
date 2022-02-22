package beanstalkd

import (
	"context"
	"io"
	"time"
)

const (
	PriHigh   = 1
	PriNormal = 2
	PriLow    = 3

	defaultTimeToRun = time.Second * 5
	reserveTimeout   = time.Second * 5

	idSep   = ","
	timeSep = '/'
)

type DelayPusher interface {
	io.Closer
	At(ctx context.Context, body []byte, at time.Time) (string, error)
	Delay(ctx context.Context, body []byte, delay time.Duration) (string, error)
	Revoke(ctx context.Context, ids string) error
}
