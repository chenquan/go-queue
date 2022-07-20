package kafka

import "github.com/segmentio/kafka-go"

type RequiredAcks = kafka.RequiredAcks

const (
	RequireNone = kafka.RequireNone
	RequireOne  = kafka.RequireOne
	RequireAll  = kafka.RequireAll
)
