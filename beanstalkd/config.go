package beanstalkd

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type (
	Beanstalk struct {
		Endpoint string
		Tube     string
	}

	Conf struct {
		Beanstalks []Beanstalk
		Redis      redis.RedisConf
	}
)
