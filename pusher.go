package queue

import (
	"context"
	"io"
	"time"
)

type DelayPusher interface {
	io.Closer
	At(ctx context.Context, body []byte, at time.Time) (string, error)
	Delay(ctx context.Context, body []byte, delay time.Duration) (string, error)
	Revoke(ctx context.Context, ids string) error
}

type Pusher interface {
	io.Closer
	Push(ctx context.Context, body []byte) error
	Name() string
}
