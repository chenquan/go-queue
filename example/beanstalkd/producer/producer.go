package main

import (
	"context"
	"fmt"
	"github.com/chenquan/go-queue/beanstalkd"
	"strconv"
	"time"
)

func main() {
	producer := beanstalkd.NewProducer(
		beanstalkd.Beanstalkd{
			Tube: "tube",
			Endpoints: []string{
				"localhost:11300",
				"127.0.0.1:11300",
			},
		},
	)

	for i := 1; i < 1005; i++ {
		//_, err := producer.Delay(context.Background(), []byte(strconv.Itoa(i)), time.Second*5)
		//if err != nil {
		//	fmt.Println(err)
		//}
		_, err := producer.Push(context.Background(), nil, []byte(strconv.Itoa(i)), beanstalkd.WithDuration(time.Second*5))
		if err != nil {
			fmt.Println(err)
		}
	}
}
