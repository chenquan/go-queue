package main

import (
	"context"
	"fmt"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/chenquan/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c kq.Conf
	conf.MustLoad("config.yaml", &c)

	q := kq.MustNewQueue(c, kq.WithHandle(func(ctx context.Context, k, v []byte) error {
		logx.WithContext(ctx).Info(fmt.Sprintf("=> %s\n", v))
		return nil
	}))
	defer q.Stop()
	q.Start()
}
