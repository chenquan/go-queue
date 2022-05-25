package main

import (
	"context"
	"fmt"

	"github.com/chenquan/go-queue/kafka"
	"github.com/chenquan/go-queue/queue"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"

	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c struct {
		kafka.Conf
		service.ServiceConf
	}

	conf.MustLoad("config.yaml", &c)
	c.MustSetUp()

	q := kafka.MustNewQueue(
		c.Conf, queue.WithHandle(
			func(ctx context.Context, k, v []byte) error {
				logx.WithContext(ctx).Info(fmt.Sprintf("=> %s\n", v))
				return nil
			},
		),
	)
	defer q.Stop()
	q.Start()
}
