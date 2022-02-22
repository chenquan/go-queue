package pulsar

import "github.com/zeromicro/go-zero/core/service"

type Conf struct {
	service.ServiceConf
	Brokers          []string
	Topic            string
	SubscriptionName string
	Conns            int `json:",default=1"`
	Processors       int `json:",default=8"`
}
