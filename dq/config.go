package dq

import (
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type (
	Beanstalk struct {
		Endpoint string
		Tube     string
	}

	Conf struct {
		service.ServiceConf
		Beanstalks []Beanstalk
		Redis      redis.RedisConf
	}
)
