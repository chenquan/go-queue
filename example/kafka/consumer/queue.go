package main

import (
	"context"
	"fmt"
	"github.com/chenquan/go-queue/kafka"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c kafka.Conf
	conf.MustLoad("config.yaml", &c)

	q := kafka.MustNewQueue(c, kafka.WithHandle(func(ctx context.Context, k, v []byte) error {
		logx.WithContext(ctx).Info(fmt.Sprintf("=> %s\n", v))
		return nil
	}))
	defer q.Stop()
	q.Start()
}
