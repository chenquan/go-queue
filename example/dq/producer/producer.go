package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/chenquan/go-queue/dq"
)

func main() {
	producer := dq.NewProducer([]dq.Beanstalk{
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
	})
	for i := 1000; i < 1005; i++ {
		_, err := producer.Delay(context.Background(), []byte(strconv.Itoa(i)), time.Second*5)
		if err != nil {
			fmt.Println(err)
		}
	}
}
