package beanstalkd

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type (
	Conf struct {
		Beanstalkd
		Redis redis.RedisConf
	}

	Beanstalkd struct {
		Endpoints []string
		Tube      string
	}
)
