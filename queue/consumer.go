package queue

import "context"

type Consumer interface {
	Consume(ctx context.Context, key, value []byte) error
}
