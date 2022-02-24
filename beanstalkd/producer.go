package beanstalkd

import (
	"bytes"
	"context"
	"github.com/chenquan/go-queue/queue"
	"io"
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
	DelayPusher interface {
		io.Closer
		At(ctx context.Context, body []byte, at time.Time) (string, error)
		Delay(ctx context.Context, body []byte, delay time.Duration) (string, error)
		Revoke(ctx context.Context, ids string) error
	}

	ProducerCluster struct {
		tube  string
		nodes []DelayPusher
	}

	callOptions struct {
		at time.Time
	}
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func NewProducer(beanstalk Beanstalkd) *ProducerCluster {
	if len(beanstalk.Endpoints) < minWrittenNodes {
		log.Fatalf("nodes must be equal or greater than %d", minWrittenNodes)
	}

	var nodes []DelayPusher
	producers := make(map[string]lang.PlaceholderType)
	for _, endpoint := range beanstalk.Endpoints {
		if _, ok := producers[endpoint]; ok {
			log.Fatal("all node endpoints must be different")
		}

		producers[endpoint] = lang.Placeholder
		nodes = append(nodes, NewProducerNode(endpoint, beanstalk.Tube))
	}

	return &ProducerCluster{
		nodes: nodes,
	}
}

func (p *ProducerCluster) Push(ctx context.Context, _, body []byte, opts ...queue.CallOptions) (interface{}, error) {
	if len(opts) == 0 {
		panic("expiration time must be set")
	}

	op := new(callOptions)
	for _, opt := range opts {
		opt(op)
	}

	return p.At(ctx, body, op.at)
}

func (p *ProducerCluster) Name() string {
	return p.tube
}

func (p *ProducerCluster) At(ctx context.Context, body []byte, at time.Time) (string, error) {
	wrapped := p.wrap(body, at)
	return p.insert(ctx, func(node DelayPusher) (string, error) {
		return node.At(ctx, wrapped, at)
	})
}

func (p *ProducerCluster) Close() error {
	var be errorx.BatchError
	for _, node := range p.nodes {
		if err := node.Close(); err != nil {
			be.Add(err)
		}
	}
	return be.Err()
}

func (p *ProducerCluster) Delay(ctx context.Context, body []byte, delay time.Duration) (string, error) {
	wrapped := p.wrap(body, time.Now().Add(delay))
	return p.insert(ctx, func(node DelayPusher) (string, error) {
		return node.Delay(ctx, wrapped, delay)
	})
}

func (p *ProducerCluster) Revoke(ctx context.Context, ids string) error {
	var be errorx.BatchError

	fx.From(func(source chan<- interface{}) {
		for _, node := range p.nodes {
			source <- node
		}
	}).Map(func(item interface{}) interface{} {
		node := item.(DelayPusher)
		return node.Revoke(ctx, ids)
	}).ForEach(func(item interface{}) {
		if item != nil {
			be.Add(item.(error))
		}
	})

	return be.Err()
}

func (p *ProducerCluster) cloneNodes() []DelayPusher {
	return append([]DelayPusher(nil), p.nodes...)
}

func (p *ProducerCluster) getWriteNodes() []DelayPusher {
	if len(p.nodes) <= replicaNodes {
		return p.nodes
	}

	nodes := p.cloneNodes()
	r.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return nodes[:replicaNodes]
}

func (p *ProducerCluster) insert(ctx context.Context, fn func(node DelayPusher) (string, error)) (string, error) {
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
		node := item.(DelayPusher)
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

func (p *ProducerCluster) wrap(body []byte, at time.Time) []byte {
	var builder bytes.Buffer
	builder.WriteString(strconv.FormatInt(at.UnixNano(), 10))
	builder.WriteByte(timeSep)
	builder.Write(body)
	return builder.Bytes()
}
