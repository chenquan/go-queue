package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/chenquan/go-queue/internal/xtrace"
	"github.com/chenquan/go-queue/kafka"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	var c struct {
		service.ServiceConf
	}
	conf.MustLoad("config.yaml", &c)

	c.MustSetUp()

	pusher := kafka.NewPusher(
		[]string{
			"127.0.0.1:9092",
			"127.0.0.1:9092",
			"127.0.0.1:9092",
		}, "kafka",
	)

	ticker := time.NewTicker(time.Millisecond)
	for round := 0; ; round++ {
		<-ticker.C

		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%d,%d", round, count),
		}
		body, err := json.Marshal(m)
		if err != nil {
			log.Fatal(err)
		}
		tracer := xtrace.Tracer()
		ctx, span := tracer.Start(context.Background(), "push-test")

		//fmt.Println(string(body))
		if _, err := pusher.Push(ctx, []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), body); err != nil {
			log.Fatal(err)
		}
		span.End()
		ticker.Reset(time.Second / 20)
	}

}
