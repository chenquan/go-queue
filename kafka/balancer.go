package kafka

import (
	"github.com/cespare/xxhash/v2"
	"github.com/segmentio/kafka-go"
)

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

type XXHashBalancer struct {
	rr RoundRobin
}

func (x *XXHashBalancer) Balance(msg kafka.Message, partitions ...int) (partition int) {
	if len(msg.Key) == 0 {
		return x.rr.Balance(msg, partitions...)
	}

	idx := xxhash.Sum64(msg.Key) % uint64(len(partitions))
	return partitions[idx]
}
