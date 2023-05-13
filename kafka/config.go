package kafka

const (
	firstOffset = "first"
	lastOffset  = "last"
)

type Conf struct {
	Brokers    []string
	Group      string
	Topic      string
	Offset     string `json:",options=first|last,default=last"`
	Conns      int    `json:",default=1"`
	Consumers  int    `json:",default=8"`
	Processors int    `json:",default=8"`
	MinBytes   int    `json:",default=10240"`    // 10K
	MaxBytes   int    `json:",default=10485760"` // 10M
	Username   string `json:",optional"`
	Password   string `json:",optional"`
}
