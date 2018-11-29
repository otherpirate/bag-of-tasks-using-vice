package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/matryer/vice/queues/nsq"
)

const MAX_SIZE = 0.1

func StrToFloat(value string) float64 {
	parsed_value, err := strconv.ParseFloat(value, 32)
	if err != nil {
		panic(value + " is not a valid float")
	}
	return parsed_value
}

func FloatToStr(value float64) string {
	return fmt.Sprintf("%.6f", value)
}

// Service to calculate values
func Calculate(ctx context.Context, values <-chan []byte,
	total_queue chan<- []byte,
	feed_queue chan<- []byte,
	errs <-chan error,
) {
	count := 0
	for {
		select {
		case <-ctx.Done():
			log.Println("finished")
			return
		case err := <-errs:
			log.Println("an error occurred:", err)
		case value := <-values:
			count = count + 1
			fmt.Printf("Count: %d\n", count)
			data := strings.Split(string(value), "|")
			left := StrToFloat(data[0])
			right := StrToFloat(data[1])
			diff := math.Abs(left - right)
			mid := diff / 2.0
			fmt.Printf("%.3f, %.3f, %.3f, %.3f\n", diff, left, mid, right)
			if diff > MAX_SIZE {
				feed_queue <- []byte(FloatToStr(left) + "|" + FloatToStr(left+mid))
				feed_queue <- []byte(FloatToStr(right-mid) + "|" + FloatToStr(right))
				continue
			}
			total_queue <- []byte(FloatToStr(left + right))
		}
	}
}

func main() {
	ctx := context.Background()
	transport := nsq.New()
	defer func() {
		transport.Stop()
		<-transport.Done()
	}()
	values := transport.Receive("values")
	total := transport.Send("total")
	feed := transport.Send("feed")
	Calculate(ctx, values, total, feed, transport.ErrChan())
}
