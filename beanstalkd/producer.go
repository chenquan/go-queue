package beanstalkd

import (
	"bytes"
	"context"
	"github.com/chenquan/go-queue/queue"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/errorx"
	"github.com/zeromicro/go-zero/core/fx"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	replicaNodes    = 3
	minWrittenNodes = 2
)

type (
	producerCluster struct {
		nodes []queue.DelayPusher
	}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewProducer(beanstalks []Beanstalk) queue.DelayPusher {
	if len(beanstalks) < minWrittenNodes {
		log.Fatalf("nodes must be equal or greater than %d", minWrittenNodes)
	}

	var nodes []queue.DelayPusher
	producers := make(map[string]lang.PlaceholderType)
	for _, node := range beanstalks {
		if _, ok := producers[node.Endpoint]; ok {
			log.Fatal("all node endpoints must be different")
		}

		producers[node.Endpoint] = lang.Placeholder
		nodes = append(nodes, NewProducerNode(node.Endpoint, node.Tube))
	}

	return &producerCluster{nodes: nodes}
}

func (p *producerCluster) At(ctx context.Context, body []byte, at time.Time) (string, error) {
	wrapped := p.wrap(body, at)
	return p.insert(ctx, func(node queue.DelayPusher) (string, error) {
		return node.At(ctx, wrapped, at)
	})
}

func (p *producerCluster) Close() error {
	var be errorx.BatchError
	for _, node := range p.nodes {
		if err := node.Close(); err != nil {
			be.Add(err)
		}
	}
	return be.Err()
}

func (p *producerCluster) Delay(ctx context.Context, body []byte, delay time.Duration) (string, error) {
	wrapped := p.wrap(body, time.Now().Add(delay))
	return p.insert(ctx, func(node queue.DelayPusher) (string, error) {
		return node.Delay(ctx, wrapped, delay)
	})
}

func (p *producerCluster) Revoke(ctx context.Context, ids string) error {
	var be errorx.BatchError

	fx.From(func(source chan<- interface{}) {
		for _, node := range p.nodes {
			source <- node
		}
	}).Map(func(item interface{}) interface{} {
		node := item.(queue.DelayPusher)
		return node.Revoke(ctx, ids)
	}).ForEach(func(item interface{}) {
		if item != nil {
			be.Add(item.(error))
		}
	})

	return be.Err()
}

func (p *producerCluster) cloneNodes() []queue.DelayPusher {
	return append([]queue.DelayPusher(nil), p.nodes...)
}

func (p *producerCluster) getWriteNodes() []queue.DelayPusher {
	if len(p.nodes) <= replicaNodes {
		return p.nodes
	}

	nodes := p.cloneNodes()
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return nodes[:replicaNodes]
}

func (p *producerCluster) insert(ctx context.Context, fn func(node queue.DelayPusher) (string, error)) (string, error) {
	type idErr struct {
		id  string
		err error
	}
	var ret []idErr
	fx.From(func(source chan<- interface{}) {
		for _, node := range p.getWriteNodes() {
			source <- node
		}
	}).Map(func(item interface{}) interface{} {
		node := item.(queue.DelayPusher)
		id, err := fn(node)
		return idErr{
			id:  id,
			err: err,
		}
	}).ForEach(func(item interface{}) {
		ret = append(ret, item.(idErr))
	})

	var ids []string
	var be errorx.BatchError
	for _, val := range ret {
		if val.err != nil {
			be.Add(val.err)
		} else {
			ids = append(ids, val.id)
		}
	}

	jointId := strings.Join(ids, idSep)
	if len(ids) >= minWrittenNodes {
		return jointId, nil
	}

	if err := p.Revoke(ctx, jointId); err != nil {
		logx.WithContext(ctx).Error(err)
	}

	return "", be.Err()
}

func (p *producerCluster) wrap(body []byte, at time.Time) []byte {
	var builder bytes.Buffer
	builder.WriteString(strconv.FormatInt(at.UnixNano(), 10))
	builder.WriteByte(timeSep)
	builder.Write(body)
	return builder.Bytes()
}
