package queue

import (
	"context"
)

type Consumer interface {
	Consume(ctx context.Context, key, value []byte) error
}

type ConsumeHandle func(ctx context.Context, key, value []byte) error

type innerConsumeHandler struct {
	handle ConsumeHandle
}

func (ch innerConsumeHandler) Consume(ctx context.Context, k, v []byte) error {
	return ch.handle(ctx, k, v)
}

func WithHandle(handle ConsumeHandle) Consumer {
	return innerConsumeHandler{
		handle: handle,
	}
}
