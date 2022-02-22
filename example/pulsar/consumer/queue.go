package main

import (
	"context"
	"fmt"
	"github.com/chenquan/go-queue/pulsar"
	"github.com/chenquan/go-queue/queue"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c pulsar.Conf
	conf.MustLoad("config.yaml", &c)

	q := pulsar.MustNewQueue(c, queue.WithHandle(func(ctx context.Context, k, v []byte) error {
		logx.WithContext(ctx).Info(fmt.Sprintf("=> %s\n", v))
		return nil
	}))
	defer q.Stop()
	q.Start()
}
