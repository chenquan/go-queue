package pulsar

type Conf struct {
	Brokers          []string
	Topic            string
	SubscriptionName string
	Conns            int `json:",default=1"`
	Processors       int `json:",default=8"`
}
