package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/matryer/vice/queues/nsq"
)

func StrToFloat(value string) float64 {
	parsed_value, err := strconv.ParseFloat(value, 32)
	if err != nil {
		panic(value + " is not a valid float")
	}
	return parsed_value
}

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	transport := nsq.New()

	values := transport.Send("values2")
	go func() {
		values <- []byte("0.0|10.0")
	}()

	feeds := transport.Receive("feed2")
	go func() {
		for feed := range feeds {
			values <- feed
			println(string(feed))
		}
	}()

	result := 0.0
	totals := transport.Receive("total2")
	go func() {
		for total := range totals {
			result += StrToFloat(string(total))
			fmt.Printf("%.3f\n", result)
		}
	}()

	<-ctx.Done()
	transport.Stop()
	<-transport.Done()
	log.Println("transport stopped")
}
