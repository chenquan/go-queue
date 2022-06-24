package queue

import (
	"context"
)

type Consumer interface {
	Consume(ctx context.Context, key, value []byte) error
}

type ConsumeHandle func(ctx context.Context, key, value []byte) error

func (ch ConsumeHandle) Consume(ctx context.Context, k, v []byte) error {
	return ch(ctx, k, v)
}
