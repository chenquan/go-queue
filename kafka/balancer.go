package kafka

import "github.com/segmentio/kafka-go"

type (
	Balancer        = kafka.Balancer
	BalancerFunc    = kafka.BalancerFunc
	CRC32Balancer   = kafka.CRC32Balancer
	Hash            = kafka.Hash
	LeastBytes      = kafka.LeastBytes
	Murmur2Balancer = kafka.Murmur2Balancer
	ReferenceHash   = kafka.ReferenceHash
	RoundRobin      = kafka.RoundRobin
)
