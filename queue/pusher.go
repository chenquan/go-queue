package queue

import (
	"context"
	"errors"
	"io"
)

var ErrNotSupport = errors.New("not support")

type CallOptions func(interface{})

type Pusher interface {
	io.Closer
	Push(ctx context.Context, key, body []byte, opts ...CallOptions) (interface{}, error)
	Name() string
}
