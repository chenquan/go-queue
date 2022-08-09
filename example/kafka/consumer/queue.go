package main

import (
	"context"

	"github.com/chenquan/go-queue/kafka"
	"github.com/chenquan/go-queue/queue"
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
		c.Conf, queue.ConsumeHandle(
			func(ctx context.Context, k, v []byte) error {
				//headers, b := kafka.HeadersFromContext(ctx)
				//fmt.Println("header", headers, b)
				//logx.WithContext(ctx).Info(fmt.Sprintf("=> %s\n", v))
				return nil
			},
		),
	)
	defer q.Stop()
	q.Start()
}
