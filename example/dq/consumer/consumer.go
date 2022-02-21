package main

import (
	"context"
	"github.com/chenquan/go-queue/dq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

func main() {
	var c service.ServiceConf
	conf.MustLoad("config.yaml", &c)

	consumer := dq.NewConsumer(dq.Conf{
		ServiceConf: c,
		Beanstalks: []dq.Beanstalk{
			{
				Endpoint: "localhost:11300",
				Tube:     "tube",
			},
			{
				Endpoint: "localhost:11300",
				Tube:     "tube",
			},
		},
		Redis: redis.RedisConf{
			Host: "localhost:6379",
			Type: redis.NodeType,
		},
	}, dq.WithHandle(func(ctx context.Context, body []byte) {
		logx.WithContext(ctx).Info(string(body))

	}))
	defer consumer.Stop()
	consumer.Start()

}
